import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import type { HrContext } from "../types.js";
import multipart from "@fastify/multipart";
import * as config from "../config.js";
import { defaultClient, ErpNextClient, ErpCredentials } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import { publicErpFailure, parseFrappeErrorBody } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import {
  evaluateApproveWorkflow,
  evaluateExpenseClaim,
  evaluateMarkPaid,
  fetchRulesRowForResponse,
  hasBlockingFinding,
  loadCompanyRulesPack,
  mergeClaimPolicyWarnings,
  upsertCompanyRules,
  type PolicyWarningPublic,
} from "../lib/expenseRules.js";
import { logHrPolicyDenial } from "../lib/approvalPolicyLog.js";
import { attachCentyTwoStageExpenseRow, isFirstApproverFlagSet } from "../lib/twoStageCustomFields.js";

const erp = defaultClient();

const BULK_APPROVE_MAX = 40;
const EXPORT_MAX_ROWS = 2000;

type ApproveOnceResult = { ok: true } | { ok: false; status: number; error: string };

function csvEscapeCell(s: string): string {
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

async function getLinkedEmployeeName(ctx: HrContext): Promise<string | null> {
  const mine = await erp.listDocs(ctx.creds, "Employee", {
    filters: [
      ["user_id", "=", ctx.userEmail],
      ["company", "=", ctx.company],
    ],
    fields: ["name"],
    limit_page_length: 1,
  });
  const empName = asRecord(mine.data?.[0])?.name;
  return typeof empName === "string" ? empName : null;
}

async function approveExpenseClaimOnce(
  ctx: HrContext,
  claimName: string,
  pack: Awaited<ReturnType<typeof loadCompanyRulesPack>>
): Promise<ApproveOnceResult> {
  const cur = await erp.getDoc(ctx.creds, "Expense Claim", claimName);
  if (String(cur.company) !== ctx.company) {
    return { ok: false, status: 403, error: "Claim not in your Company" };
  }
  if (!isSubmittedClaim(cur)) {
    return { ok: false, status: 409, error: "Only submitted claims can be approved" };
  }
  if (isTerminalApproval(cur)) {
    return { ok: false, status: 409, error: "Claim decision already finalised" };
  }
  const approver = String(cur.expense_approver ?? "").trim().toLowerCase();
  const me = ctx.userEmail.trim().toLowerCase();
  const canFinanceAct = !!ctx.canSubmitOnBehalf;
  if (approver !== me && !canFinanceAct) {
    return {
      ok: false,
      status: 403,
      error: "Only the assigned approver or finance-privileged user can approve this claim",
    };
  }
  const isAssignee = approver === me;
  const twoStage = config.EXPENSE_TWO_STAGE_APPROVAL;
  const firstField = config.EXPENSE_FIRST_APPROVER_FIELD;

  if (!twoStage) {
    const wfBlock = evaluateApproveWorkflow(cur as Record<string, unknown>, pack, {
      canSubmitOnBehalf: ctx.canSubmitOnBehalf,
    });
    if (wfBlock) {
      if (wfBlock.code === "approve_ceiling") {
        logHrPolicyDenial("expense_approve_amount_ceiling", {
          company: ctx.company,
          user_email: ctx.userEmail,
          app_role: ctx.appRole ?? null,
          expense_claim: claimName,
          claim_total: cur.total_claimed_amount ?? cur.grand_total ?? null,
          ceiling: pack.workflow.approve_ceiling_for_non_finance ?? null,
        });
      }
      return { ok: false, status: 400, error: `Policy: ${wfBlock.message}` };
    }
    await erp.callMethod(ctx.creds, "frappe.client.set_value", {
      doctype: "Expense Claim",
      name: claimName,
      fieldname: "approval_status",
      value: "Approved",
    });
    return { ok: true };
  }

  const firstDone = isFirstApproverFlagSet(cur as Record<string, unknown>, firstField);
  const bypass = config.EXPENSE_HR_BYPASS_FIRST_APPROVER;
  const needFirst = !firstDone && !bypass;

  if (needFirst && canFinanceAct && !isAssignee) {
    return {
      ok: false,
      status: 403,
      error: "First approver must complete their step before finance can finalise this claim.",
    };
  }

  if (needFirst && isAssignee) {
    const wfBlock = evaluateApproveWorkflow(cur as Record<string, unknown>, pack, {
      canSubmitOnBehalf: canFinanceAct,
    });
    if (wfBlock) {
      if (wfBlock.code === "approve_ceiling") {
        logHrPolicyDenial("expense_approve_amount_ceiling", {
          company: ctx.company,
          user_email: ctx.userEmail,
          app_role: ctx.appRole ?? null,
          expense_claim: claimName,
          claim_total: cur.total_claimed_amount ?? cur.grand_total ?? null,
          ceiling: pack.workflow.approve_ceiling_for_non_finance ?? null,
        });
      }
      return { ok: false, status: 400, error: `Policy: ${wfBlock.message}` };
    }
    if (firstDone) {
      return { ok: true };
    }
    await erp.callMethod(ctx.creds, "frappe.client.set_value", {
      doctype: "Expense Claim",
      name: claimName,
      fieldname: firstField,
      value: 1,
    });
    return { ok: true };
  }

  if (needFirst && !isAssignee) {
    return {
      ok: false,
      status: 403,
      error: "Only the assigned expense approver can complete the first approval step.",
    };
  }

  if (!canFinanceAct) {
    if (firstDone) {
      return { ok: true };
    }
    return {
      ok: false,
      status: 403,
      error: "Only finance / HR can finalise this claim after the first approver step.",
    };
  }

  const wfBlock = evaluateApproveWorkflow(cur as Record<string, unknown>, pack, {
    canSubmitOnBehalf: true,
  });
  if (wfBlock) {
    if (wfBlock.code === "approve_ceiling") {
      logHrPolicyDenial("expense_approve_amount_ceiling", {
        company: ctx.company,
        user_email: ctx.userEmail,
        app_role: ctx.appRole ?? null,
        expense_claim: claimName,
        claim_total: cur.total_claimed_amount ?? cur.grand_total ?? null,
        ceiling: pack.workflow.approve_ceiling_for_non_finance ?? null,
      });
    }
    return { ok: false, status: 400, error: `Policy: ${wfBlock.message}` };
  }
  await erp.callMethod(ctx.creds, "frappe.client.set_value", {
    doctype: "Expense Claim",
    name: claimName,
    fieldname: "approval_status",
    value: "Approved",
  });
  return { ok: true };
}

function replyErp(reply: FastifyReply, e: ErpError) {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

/** Max rows scanned for dashboard counts (avoids loading unbounded lists). */
const SUMMARY_SCAN_CAP = 5000;

function parsePageParams(req: FastifyRequest): { page: number; pageSize: number; limitStart: number } {
  const q = (req.query ?? {}) as Record<string, unknown>;
  const page = Math.max(1, parseInt(String(q.page ?? "1"), 10) || 1);
  const raw = parseInt(String(q.page_size ?? "25"), 10) || 25;
  const pageSize = Math.min(100, Math.max(10, raw));
  return { page, pageSize, limitStart: (page - 1) * pageSize };
}

/** ERP filters for claims awaiting this user’s approval (submitted, not terminal). */
function pendingClaimFilters(ctx: { company: string; userEmail: string }): unknown[] {
  return [
    ["company", "=", ctx.company],
    ["expense_approver", "=", ctx.userEmail],
    ["docstatus", "=", 1],
    ["approval_status", "not in", ["Approved", "Rejected"]],
  ];
}

function normalizeStatus(v: unknown): string {
  return String(v ?? "").trim().toLowerCase();
}

function isDraftClaim(doc: Record<string, unknown>): boolean {
  return Number(doc.docstatus) === 0;
}

function isSubmittedClaim(doc: Record<string, unknown>): boolean {
  return Number(doc.docstatus) === 1;
}

function isTerminalApproval(doc: Record<string, unknown>): boolean {
  const st = normalizeStatus(doc.approval_status);
  return st === "approved" || st === "rejected";
}

function isPaidClaim(doc: Record<string, unknown>): boolean {
  const reimb = Number(doc.total_amount_reimbursed ?? 0);
  return Number.isFinite(reimb) && reimb > 0;
}

/**
 * Resolve the canonical ERPNext Department name for a given department label.
 * ERPNext auto-names departments as "Finance - ABBR" (department_name + company abbreviation).
 * Pay Hub may store them as plain "Finance".
 * - Tries exact name match first.
 * - Falls back to searching by `department_name` field.
 * - Creates the department in ERPNext if not found.
 * Returns the canonical ERPNext name (e.g. "Finance - ABC"), or the original value if
 * creation fails (so the caller can decide whether to abort).
 */
async function resolveErpDeptName(
  erpClient: ErpNextClient,
  creds: ErpCredentials,
  company: string,
  rawDeptName: string,
): Promise<string> {
  if (!rawDeptName.trim()) return rawDeptName;
  console.warn(`[dept-ensure] resolving "${rawDeptName}" for company="${company}"`);

  // 1. Exact name match (already canonical, e.g. "Finance - ABC")
  try {
    const existing = (await erpClient.getDoc(creds, "Department", rawDeptName)) as Record<string, unknown>;
    const name = String(existing.name ?? rawDeptName);
    console.warn(`[dept-ensure] exact match: "${name}"`);
    return name;
  } catch {
    console.warn(`[dept-ensure] no exact match — searching by department_name`);
  }

  // 2. Search by department_name field (finds "Finance - ABC" when we have "Finance")
  try {
    const searchResult = await erpClient.listDocs(creds, "Department", {
      filters: [["department_name", "=", rawDeptName]],
      fields: ["name", "department_name"],
      limit_page_length: 10,
    });
    const matches = (searchResult.data ?? []) as { name?: string; department_name?: string }[];
    console.warn(`[dept-ensure] department_name search:`, JSON.stringify(matches).slice(0, 300));
    if (matches.length > 0) {
      const companyMatch = matches.find((m) =>
        String(m.name ?? "").toLowerCase().includes(company.slice(0, 4).toLowerCase()),
      );
      const resolved = String((companyMatch ?? matches[0]).name ?? rawDeptName);
      console.warn(`[dept-ensure] resolved via search: "${resolved}"`);
      return resolved;
    }
  } catch (searchErr) {
    console.warn(`[dept-ensure] department_name search failed:`, String(searchErr).slice(0, 200));
  }

  // 3. Not in ERPNext — create it
  let parentDept = "All Departments";
  try {
    await erpClient.getDoc(creds, "Department", "All Departments");
  } catch {
    try {
      const rootsResult = await erpClient.listDocs(creds, "Department", {
        filters: [["is_group", "=", 1]],
        fields: ["name"],
        limit_page_length: 5,
      });
      const roots = (rootsResult.data ?? []) as { name?: string }[];
      if (roots[0]?.name) parentDept = roots[0].name;
    } catch {
      /* keep default */
    }
  }

  console.warn(`[dept-ensure] creating Department "${rawDeptName}" parent="${parentDept}"`);
  try {
    const created = (await erpClient.createDoc(creds, "Department", {
      department_name: rawDeptName,
      company,
      parent_department: parentDept,
      is_group: 0,
    })) as Record<string, unknown>;
    const createdName = String(created.name ?? rawDeptName);
    console.warn(`[dept-ensure] created: "${createdName}"`);
    return createdName;
  } catch (createErr) {
    console.warn(`[dept-ensure] CREATE FAILED:`, String(createErr).slice(0, 400));
    return rawDeptName; // return original; caller will get ERPNext validation error
  }
}

/**
 * Before submitting a saved claim, ensure its department exists in ERPNext and
 * patch the claim to use the canonical name if they differ.
 */
async function ensureDepartmentExists(
  erpClient: ErpNextClient,
  creds: ErpCredentials,
  company: string,
  claimName: string,
): Promise<void> {
  try {
    const claimDoc = (await erpClient.getDoc(creds, "Expense Claim", claimName)) as Record<string, unknown>;
    const rawDept = String(claimDoc.department ?? "").trim();
    console.warn(`[dept-ensure] claim "${claimName}" department="${rawDept}"`);
    if (!rawDept) return;

    const canonical = await resolveErpDeptName(erpClient, creds, company, rawDept);

    if (canonical !== rawDept) {
      console.warn(`[dept-ensure] patching claim "${claimName}": "${rawDept}" → "${canonical}"`);
      try {
        await erpClient.updateDoc(creds, "Expense Claim", claimName, { department: canonical });
        console.warn(`[dept-ensure] patch OK`);
      } catch (patchErr) {
        console.warn(`[dept-ensure] PATCH FAILED:`, String(patchErr).slice(0, 400));
      }
    }
  } catch (err) {
    console.warn(`[dept-ensure] ERROR:`, String(err).slice(0, 400));
  }
}

/**
 * ERPNext requires each Expense Claim Type to have a default account per company
 * before a claim can be created. This helper auto-sets a default account when missing,
 * using the company's default payable account (falls back to any payable/expense account).
 * Returns true if the account was already set or was successfully added.
 */
async function ensureExpenseClaimTypeAccount(
  erpClient: ErpNextClient,
  creds: ErpCredentials,
  company: string,
  claimTypeName: string,
): Promise<boolean> {
  console.warn(`[claim-type] ensuring account for type="${claimTypeName}" company="${company}"`);
  try {
    // Fetch the Expense Claim Type doc
    const typeDoc = (await erpClient.getDoc(creds, "Expense Claim Type", claimTypeName)) as Record<string, unknown>;
    const accounts = Array.isArray(typeDoc.accounts) ? typeDoc.accounts as Record<string, unknown>[] : [];

    // Check if an account is already configured for this company
    const existing = accounts.find(
      (a) => String(a.company ?? "").toLowerCase() === company.toLowerCase(),
    );
    if (existing?.default_account) {
      console.warn(`[claim-type] account already set: "${existing.default_account}"`);
      return true;
    }

    // Find a suitable default account — prefer company's default payable account
    let defaultAccount: string | null = null;

    // Try company doc for default_payable_account
    try {
      const companyDoc = (await erpClient.getDoc(creds, "Company", company)) as Record<string, unknown>;
      const payable = String(companyDoc.default_payable_account ?? "").trim();
      if (payable) {
        defaultAccount = payable;
        console.warn(`[claim-type] using company default_payable_account: "${defaultAccount}"`);
      }
    } catch (compErr) {
      console.warn(`[claim-type] could not fetch Company doc:`, String(compErr).slice(0, 200));
    }

    // Fallback: search for any Payable or Expense account for this company
    if (!defaultAccount) {
      try {
        const accResult = await erpClient.listDocs(creds, "Account", {
          filters: [
            ["company", "=", company],
            ["account_type", "in", ["Payable", "Expense Account"]],
            ["is_group", "=", 0],
          ],
          fields: ["name", "account_type"],
          limit_page_length: 5,
        });
        const accs = (accResult.data ?? []) as { name?: string; account_type?: string }[];
        console.warn(`[claim-type] fallback account search:`, JSON.stringify(accs).slice(0, 300));
        // Prefer Payable type
        const payableAcc = accs.find((a) => a.account_type === "Payable");
        const chosen = payableAcc ?? accs[0];
        if (chosen?.name) defaultAccount = chosen.name;
      } catch (accErr) {
        console.warn(`[claim-type] account search failed:`, String(accErr).slice(0, 200));
      }
    }

    if (!defaultAccount) {
      console.warn(`[claim-type] no suitable account found — cannot auto-configure`);
      return false;
    }

    // Patch the Expense Claim Type to add this company's account
    const updatedAccounts = [
      ...accounts,
      { company, default_account: defaultAccount },
    ];
    console.warn(`[claim-type] patching type "${claimTypeName}" with account "${defaultAccount}"`);
    await erpClient.updateDoc(creds, "Expense Claim Type", claimTypeName, {
      accounts: updatedAccounts,
    });
    console.warn(`[claim-type] patch OK`);
    return true;
  } catch (err) {
    console.warn(`[claim-type] ERROR:`, String(err).slice(0, 400));
    return false;
  }
}

function syntheticClaimFromCreateBody(body: Record<string, unknown>): Record<string, unknown> {
  const expenses = Array.isArray(body.expenses) ? body.expenses : [];
  let total = 0;
  for (const x of expenses) {
    if (x && typeof x === "object") total += Number((x as Record<string, unknown>).amount ?? 0);
  }
  return { expenses, total_claimed_amount: total };
}

async function mapRowsWithPolicy(rows: unknown[], companyKey: string): Promise<unknown[]> {
  const pack = await loadCompanyRulesPack(companyKey);
  return rows.map((r) => {
    const rec = asRecord(r);
    if (!rec) return r;
    return attachCentyTwoStageExpenseRow(mergeClaimPolicyWarnings(rec, pack) as Record<string, unknown>);
  });
}

export const expenseRoutes: FastifyPluginAsync = async (app) => {
  await app.register(multipart, { limits: { fileSize: 15 * 1024 * 1024 } });

  /** HR expense claim line types (Expense Claim Type master). */
  app.get("/v1/meta/expense-claim-types", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const rows = await erp.getList(ctx.creds, "Expense Claim Type", {
        fields: ["name"],
        limit_page_length: 100,
      });
      const names = (rows as { name?: string }[])
        .map((r) => String(r.name ?? "").trim())
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b));
      return { data: names.map((name) => ({ name })) };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Cost Center names for this Company (typing aid; ERP remains source of truth). */
  app.get("/v1/meta/cost-centers", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const raw = (req.query ?? {}) as Record<string, unknown>;
    const q = String(raw.q ?? "").trim().slice(0, 120);
    const limit = Math.min(80, Math.max(10, parseInt(String(raw.limit ?? "40"), 10) || 40));
    const esc = q.replace(/\\/g, "\\\\").replace(/%/g, "\\%");
    const like = esc ? `%${esc}%` : "";
    try {
      let filters: unknown[] = [["company", "=", ctx.company]];
      if (like) filters = ["and", filters, ["name", "like", like]];
      const res = await erp.listDocs(ctx.creds, "Cost Center", {
        filters,
        fields: ["name"],
        order_by: "name asc",
        limit_page_length: limit,
      });
      const names = (res.data ?? [])
        .map((r) => String(asRecord(r)?.name ?? "").trim())
        .filter(Boolean);
      return { data: names.map((name) => ({ name })) };
    } catch (e) {
      if (e instanceof ErpError) {
        console.warn("[hr] meta/cost-centers:", e.status, e.body);
        return { data: [] as { name: string }[] };
      }
      throw e;
    }
  });

  /** Project names for this Company (typing aid). */
  app.get("/v1/meta/projects", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const raw = (req.query ?? {}) as Record<string, unknown>;
    const q = String(raw.q ?? "").trim().slice(0, 120);
    const limit = Math.min(80, Math.max(10, parseInt(String(raw.limit ?? "40"), 10) || 40));
    const esc = q.replace(/\\/g, "\\\\").replace(/%/g, "\\%");
    const like = esc ? `%${esc}%` : "";
    try {
      let filters: unknown[] = [["company", "=", ctx.company]];
      if (like) filters = ["and", filters, ["name", "like", like]];
      const res = await erp.listDocs(ctx.creds, "Project", {
        filters,
        fields: ["name"],
        order_by: "name asc",
        limit_page_length: limit,
      });
      const names = (res.data ?? [])
        .map((r) => String(asRecord(r)?.name ?? "").trim())
        .filter(Boolean);
      return { data: names.map((name) => ({ name })) };
    } catch (e) {
      if (e instanceof ErpError) {
        console.warn("[hr] meta/projects:", e.status, e.body);
        return { data: [] as { name: string }[] };
      }
      throw e;
    }
  });

  app.get("/v1/expenses", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    try {
      const filters: unknown[] = [["company", "=", ctx.company]];
      const qEmp = String((req.query as Record<string, unknown>)?.employee ?? "").trim();
      if (ctx.canSubmitOnBehalf) {
        // Finance/HR admin: can see all company claims; optionally filter by a specific employee
        if (qEmp) {
          const other = (await erp.getDoc(ctx.creds, "Employee", qEmp)) as Record<string, unknown>;
          if (String(other.company) !== ctx.company) {
            return reply.status(403).send({ error: "Employee not in your Company" });
          }
          filters.push(["employee", "=", qEmp]);
        }
      } else {
        // Regular employee: must have a linked employee record; can only see their own claims
        const mine = await erp.listDocs(ctx.creds, "Employee", {
          filters: [
            ["user_id", "=", ctx.userEmail],
            ["company", "=", ctx.company],
          ],
          fields: ["name"],
          limit_page_length: 1,
        });
        const empName = asRecord(mine.data?.[0])?.name;
        if (!empName || typeof empName !== "string") {
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        }
        filters.push(["employee", "=", empName]);
        if (qEmp && qEmp !== empName) {
          return reply.status(403).send({ error: "Cannot filter by another employee" });
        }
      }

      const { page, pageSize, limitStart } = parsePageParams(req);
      const take = pageSize + 1;
      const expFields = [
        "name",
        "employee",
        "employee_name",
        "company",
        "posting_date",
        "approval_status",
        "expense_approver",
        "docstatus",
        "grand_total",
        "total_claimed_amount",
        "total_sanctioned_amount",
        "total_amount_reimbursed",
      ];
      if (config.EXPENSE_TWO_STAGE_APPROVAL) {
        expFields.push(config.EXPENSE_FIRST_APPROVER_FIELD);
      }
      const res = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters,
        fields: expFields,
        order_by: "modified desc",
        limit_start: limitStart,
        limit_page_length: take,
      });
      const raw = res.data ?? [];
      const hasMore = raw.length > pageSize;
      const slice = hasMore ? raw.slice(0, pageSize) : raw;
      const data = await mapRowsWithPolicy(slice, ctx.company);
      return {
        data,
        meta: { page, page_size: pageSize, has_more: hasMore },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Aggregate counts for dashboard cards (bounded scan — not tied to list pagination). */
  app.get("/v1/expenses/summary", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    try {
      const filters: unknown[] = [["company", "=", ctx.company]];
      if (!ctx.canSubmitOnBehalf) {
        // Regular employee: restrict to their own claims
        const mine = await erp.listDocs(ctx.creds, "Employee", {
          filters: [
            ["user_id", "=", ctx.userEmail],
            ["company", "=", ctx.company],
          ],
          fields: ["name"],
          limit_page_length: 1,
        });
        const empName = asRecord(mine.data?.[0])?.name;
        if (!empName || typeof empName !== "string") {
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        }
        filters.push(["employee", "=", empName]);
      }

      const mineRes = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters,
        fields: ["docstatus", "approval_status"],
        order_by: "modified desc",
        limit_page_length: SUMMARY_SCAN_CAP,
      });
      const mineRows = mineRes.data ?? [];
      let drafts = 0;
      let approved = 0;
      let inReview = 0;
      for (const r of mineRows) {
        const rec = asRecord(r);
        if (!rec) continue;
        if (Number(rec.docstatus) === 0) drafts++;
        else if (String(rec.approval_status ?? "").toLowerCase() === "approved") approved++;
        else inReview++;
      }

      const pendRes = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters: pendingClaimFilters(ctx),
        fields: ["name"],
        order_by: "modified desc",
        limit_page_length: SUMMARY_SCAN_CAP,
      });
      const queue = (pendRes.data ?? []).length;

      return {
        data: {
          drafts,
          in_review: inReview,
          approved,
          queue,
          scan_capped: mineRows.length >= SUMMARY_SCAN_CAP || queue >= SUMMARY_SCAN_CAP,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Policy / workflow / feature flags (Supabase). Empty defaults when not configured. */
  app.get("/v1/expenses/rules", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const pack = await loadCompanyRulesPack(ctx.company);
      const row = await fetchRulesRowForResponse(ctx.company);
      return {
        data: {
          policy: pack.policy,
          workflow: pack.workflow,
          feature_flags: pack.feature_flags,
          updated_at: row?.updated_at ?? null,
          supabase_configured: Boolean(config.SUPABASE_URL?.trim() && config.SUPABASE_SERVICE_ROLE_KEY?.trim()),
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR/finance only: upsert merged JSON for policy, workflow, feature_flags. */
  app.put("/v1/expenses/rules", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR/finance users may update expense rules" });
    }
    const body = (req.body ?? {}) as Record<string, unknown>;
    try {
      const pack = await upsertCompanyRules(ctx.company, {
        policy: asRecord(body.policy) ?? undefined,
        workflow: asRecord(body.workflow) ?? undefined,
        feature_flags: asRecord(body.feature_flags) ?? undefined,
      });
      const row = await fetchRulesRowForResponse(ctx.company);
      req.log.info({ company: ctx.company }, "expense rules updated");
      return {
        data: {
          policy: pack.policy,
          workflow: pack.workflow,
          feature_flags: pack.feature_flags,
          updated_at: row?.updated_at ?? null,
        },
      };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      req.log.error({ err: msg }, "expense rules upsert failed");
      return reply.status(500).send({ error: msg || "Could not save rules" });
    }
  });

  app.post("/v1/expenses", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const postingDate = (body.posting_date as string) || new Date().toISOString().slice(0, 10);

    try {
      const pack = await loadCompanyRulesPack(ctx.company);
      const syn = syntheticClaimFromCreateBody(body);
      const createFindings = evaluateExpenseClaim(syn, pack);
      const blockCreate = hasBlockingFinding(createFindings);
      if (blockCreate) {
        req.log.warn({ company: ctx.company, code: blockCreate.code }, "expense policy blocked create");
        return reply.status(400).send({ error: `Policy: ${blockCreate.message}` });
      }

      let employee: string;
      let employeeDept: string | undefined;

      if (body.employee != null && body.employee !== "") {
        // Submitting on behalf of a specific employee (finance/HR only)
        if (!ctx.canSubmitOnBehalf) {
          return reply.status(403).send({ error: "Only HR may set employee (submit on behalf)" });
        }
        employee = String(body.employee);
        const other = (await erp.getDoc(ctx.creds, "Employee", employee)) as Record<string, unknown>;
        if (String(other.company) !== ctx.company) {
          return reply.status(403).send({ error: "Employee is not in your Company" });
        }
        employeeDept = String(other.department ?? "").trim() || undefined;
      } else {
        // Creating own claim — look up employee by user_id first, then personal_email as fallback.
        // The ensure endpoint may create an employee with only personal_email when the ERP User
        // isn't provisioned yet, so we must check both fields.
        console.log(`[expense-create] self-claim lookup: email=${ctx.userEmail} company=${ctx.company}`);
        async function findSelfEmployee(): Promise<Record<string, unknown> | null> {
          for (const field of ["user_id", "personal_email"] as const) {
            try {
              const rows = await erp.listDocs(ctx!.creds, "Employee", {
                filters: [[field, "=", ctx!.userEmail], ["company", "=", ctx!.company]],
                fields: ["name", "company", "department", "expense_approver"],
                limit_page_length: 1,
              });
              const row = asRecord(rows.data?.[0]);
              if (row?.name) {
                console.log(`[expense-create] found employee via ${field}: ${row.name}`);
                return row;
              }
              console.log(`[expense-create] no match via ${field}`);
            } catch (e) {
              console.warn(`[expense-create] error searching by ${field}:`, e instanceof Error ? e.message : e);
            }
          }
          return null;
        }
        const selfEmp = await findSelfEmployee();
        if (!selfEmp?.name) {
          console.error(`[expense-create] BLOCKED — no employee found for email=${ctx.userEmail} company=${ctx.company}`);
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        }
        employee = String(selfEmp.name);
        employeeDept = String(selfEmp.department ?? "").trim() || undefined;
        console.log(`[expense-create] using employee=${employee} dept=${employeeDept ?? "none"}`);
      }

      // Frappe will copy employee.department onto the claim via fetch_from on every save.
      // Ensure the department exists in ERPNext (with canonical name) before creating the claim,
      // otherwise ERPNext 417s with "Could not find Department: <name>".
      let resolvedDept: string | undefined;
      if (employeeDept) {
        resolvedDept = await resolveErpDeptName(erp, ctx.creds, ctx.company, employeeDept);
        console.warn(`[dept-ensure] create: employee dept "${employeeDept}" → resolved "${resolvedDept}"`);
      }

      const doc: Record<string, unknown> = {
        doctype: "Expense Claim",
        company: ctx.company,
        employee,
        posting_date: postingDate,
        expenses: body.expenses ?? [],
        ...(resolvedDept ? { department: resolvedDept } : {}),
      };

      let created: Record<string, unknown>;
      try {
        created = await erp.createDoc(ctx.creds, "Expense Claim", doc);
      } catch (e) {
        // ERPNext 417: "Set the default account for the Expense Claim Type <Name>"
        // The real message is in e.body (not e.message which is always "Upstream HTTP 417").
        // Auto-configure the missing account and retry once.
        if (e instanceof ErpError) {
          const erpMsg = parseFrappeErrorBody(e.body) ?? "";
          // Strip HTML tags and frappe exception prefix so the regex can match cleanly
          const plainMsg = erpMsg
            .replace(/<[^>]*>/g, "")                       // remove all HTML tags
            .replace(/^frappe\.[^:]+:\s*/i, "")            // remove "frappe.exceptions.Xxx: "
            .trim();
          console.warn(`[claim-type] createDoc failed (plain): "${plainMsg}"`);
          const missingTypeMatch = plainMsg.match(/Set the default account for the Expense Claim Type\s+(.+)/i);
          if (missingTypeMatch) {
            const claimTypeName = missingTypeMatch[1].trim();
            console.warn(`[claim-type] auto-fix triggered for type="${claimTypeName}"`);
            const fixed = await ensureExpenseClaimTypeAccount(erp, ctx.creds, ctx.company, claimTypeName);
            if (fixed) {
              console.warn(`[claim-type] retrying createDoc after account fix`);
              created = await erp.createDoc(ctx.creds, "Expense Claim", doc);
            } else {
              return replyErp(reply, e);
            }
          } else {
            return replyErp(reply, e);
          }
        } else {
          throw e;
        }
      }
      return { data: created };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/expenses/:id/submit", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    try {
      const cur = await erp.getDoc(ctx.creds, "Expense Claim", name);
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Claim not in your Company" });
      }
      if (!isDraftClaim(cur)) {
        return reply.status(409).send({ error: "Only draft claims can be submitted" });
      }
      if (!ctx.canSubmitOnBehalf) {
        const mine = await erp.listDocs(ctx.creds, "Employee", {
          filters: [
            ["user_id", "=", ctx.userEmail],
            ["company", "=", ctx.company],
          ],
          fields: ["name"],
          limit_page_length: 1,
        });
        const empName = asRecord(mine.data?.[0])?.name;
        if (!empName || String(cur.employee) !== String(empName)) {
          return reply.status(403).send({ error: "You may only submit your own claims" });
        }
      }

      const pack = await loadCompanyRulesPack(ctx.company);
      const submitFindings = evaluateExpenseClaim(cur as Record<string, unknown>, pack);
      const blockSubmit = hasBlockingFinding(submitFindings);
      if (blockSubmit) {
        req.log.warn({ company: ctx.company, claim: name, code: blockSubmit.code }, "expense policy blocked submit");
        return reply.status(400).send({ error: `Policy: ${blockSubmit.message}` });
      }

      // Frappe unconditionally copies employee.department onto the claim via fetch_from
      // on every save/submit. If that department name doesn't exist in the ERPNext
      // Department doctype, submission fails with a LinkValidationError.
      // Company departments are defined in Pay Hub settings but may not yet exist in ERPNext.
      // We auto-create any missing Department record before submitting.
      await ensureDepartmentExists(erp, ctx.creds, ctx.company, name);

      const result = await erp.submitDoc(ctx.creds, "Expense Claim", name);
      return { data: result };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/expenses/:id/attachments", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;

    try {
      const cur = await erp.getDoc(ctx.creds, "Expense Claim", name);
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Claim not in your Company" });
      }
      if (!isDraftClaim(cur)) {
        return reply.status(409).send({ error: "Receipts can only be added while claim is in draft" });
      }
      if (!ctx.canSubmitOnBehalf) {
        const mine = await erp.listDocs(ctx.creds, "Employee", {
          filters: [
            ["user_id", "=", ctx.userEmail],
            ["company", "=", ctx.company],
          ],
          fields: ["name"],
          limit_page_length: 1,
        });
        const empName = asRecord(mine.data?.[0])?.name;
        if (!empName || String(cur.employee) !== String(empName)) {
          return reply.status(403).send({ error: "You may only attach to your own claims" });
        }
      }

      const mp = await req.file();
      if (!mp) return reply.status(400).send({ error: "Expected multipart file field" });

      const buf = await mp.toBuffer();
      const result = await erp.uploadFile(ctx.creds, {
        buffer: buf,
        filename: mp.filename || "receipt",
        contentType: mp.mimetype || "application/octet-stream",
        isPrivate: false,
        doctype: "Expense Claim",
        docname: name,
      });
      return { data: result };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/expenses/pending-approval", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    try {
      const { page, pageSize, limitStart } = parsePageParams(req);
      const take = pageSize + 1;
      const pendFields = [
        "name",
        "employee",
        "employee_name",
        "company",
        "posting_date",
        "approval_status",
        "expense_approver",
        "docstatus",
        "grand_total",
        "total_claimed_amount",
        "total_sanctioned_amount",
        "total_amount_reimbursed",
      ];
      if (config.EXPENSE_TWO_STAGE_APPROVAL) {
        pendFields.push(config.EXPENSE_FIRST_APPROVER_FIELD);
      }
      const res = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters: pendingClaimFilters(ctx),
        fields: pendFields,
        order_by: "modified desc",
        limit_start: limitStart,
        limit_page_length: take,
      });
      const raw = res.data ?? [];
      const hasMore = raw.length > pageSize;
      const slice = hasMore ? raw.slice(0, pageSize) : raw;
      const data = await mapRowsWithPolicy(slice, ctx.company);
      return {
        data,
        meta: { page, page_size: pageSize, has_more: hasMore },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * CSV export (UTF-8 BOM for Excel). scope=`mine` | `all` (finance) | `pending` (approver queue or finance-wide pending).
   */
  app.get("/v1/expenses/export", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const scope = String(q.scope ?? "mine").toLowerCase();
    const limitRaw = parseInt(String(q.limit ?? "500"), 10) || 500;
    const limit = Math.min(EXPORT_MAX_ROWS, Math.max(1, limitRaw));

    try {
      let filters: unknown[];

      if (scope === "all") {
        if (!ctx.canSubmitOnBehalf) {
          return reply.status(403).send({ error: "Only finance users may export all company claims" });
        }
        filters = [["company", "=", ctx.company]];
      } else if (scope === "pending") {
        if (ctx.canSubmitOnBehalf) {
          filters = [
            ["company", "=", ctx.company],
            ["docstatus", "=", 1],
            ["approval_status", "not in", ["Approved", "Rejected"]],
          ];
        } else {
          filters = pendingClaimFilters(ctx);
        }
      } else {
        const empName = await getLinkedEmployeeName(ctx);
        if (!empName) {
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        }
        filters = [["company", "=", ctx.company], ["employee", "=", empName]];
      }

      const res = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters,
        fields: [
          "name",
          "employee",
          "employee_name",
          "company",
          "posting_date",
          "approval_status",
          "expense_approver",
          "docstatus",
          "grand_total",
          "total_claimed_amount",
          "total_sanctioned_amount",
          "total_amount_reimbursed",
        ],
        order_by: "modified desc",
        limit_page_length: limit,
      });

      const raw = res.data ?? [];
      const header = [
        "name",
        "employee",
        "employee_name",
        "company",
        "posting_date",
        "approval_status",
        "expense_approver",
        "docstatus",
        "grand_total",
        "total_claimed_amount",
        "total_sanctioned_amount",
        "total_amount_reimbursed",
      ];
      const lines = [header.join(",")];
      for (const row of raw) {
        const rec = asRecord(row);
        if (!rec) continue;
        const cells = header.map((key) =>
          csvEscapeCell(String(rec[key] ?? ""))
        );
        lines.push(cells.join(","));
      }

      const csv = "\uFEFF" + lines.join("\n");
      void reply.header("Content-Type", "text/csv; charset=utf-8");
      void reply.header("Content-Disposition", 'attachment; filename="expense-claims.csv"');
      return reply.send(csv);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Approve many claims (sequential ERP calls). Same guards as single approve. */
  app.post("/v1/expenses/bulk-approve", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const rawNames = Array.isArray(body.names) ? body.names : [];
    const names = Array.from(
      new Set(
        rawNames
          .map((x) => String(x ?? "").trim())
          .filter(Boolean)
      )
    ).slice(0, BULK_APPROVE_MAX);

    if (names.length === 0) {
      return reply.status(400).send({ error: "Provide names: string[] (claim IDs)" });
    }

    try {
      const pack = await loadCompanyRulesPack(ctx.company);
      const results: Array<{ name: string; ok: boolean; error?: string }> = [];
      for (const name of names) {
        try {
          const r = await approveExpenseClaimOnce(ctx, name, pack);
          if (r.ok) {
            results.push({ name, ok: true });
          } else {
            results.push({ name, ok: false, error: r.error });
            req.log.warn({ company: ctx.company, claim: name, bulk: true }, "bulk approve skip");
          }
        } catch (err) {
          const msg = err instanceof ErpError ? String(err.message) : String(err);
          results.push({ name, ok: false, error: msg });
          req.log.warn({ company: ctx.company, claim: name, err: msg }, "bulk approve erp error");
        }
      }
      const succeeded = results.filter((x) => x.ok).length;
      return {
        data: {
          results,
          succeeded,
          failed: results.length - succeeded,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Employee Advance rows (HRMS). Empty if doctype unavailable on site. */
  app.get("/v1/expenses/advances", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    try {
      const filters: unknown[] = [["company", "=", ctx.company]];
      const qEmp = String((req.query as Record<string, unknown>)?.employee ?? "").trim();
      if (ctx.canSubmitOnBehalf) {
        if (qEmp) {
          const other = (await erp.getDoc(ctx.creds, "Employee", qEmp)) as Record<string, unknown>;
          if (String(other.company) !== ctx.company) {
            return reply.status(403).send({ error: "Employee not in your Company" });
          }
          filters.push(["employee", "=", qEmp]);
        }
      } else {
        const empName = await getLinkedEmployeeName(ctx);
        if (!empName) {
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        }
        filters.push(["employee", "=", empName]);
        if (qEmp && qEmp !== empName) {
          return reply.status(403).send({ error: "Cannot filter advances for another employee" });
        }
      }

      const res = await erp.listDocs(ctx.creds, "Employee Advance", {
        filters,
        fields: [
          "name",
          "employee",
          "employee_name",
          "posting_date",
          "advance_amount",
          "paid_amount",
          "claimed_amount",
          "status",
          "docstatus",
        ],
        order_by: "modified desc",
        limit_page_length: 100,
      });
      return { data: res.data ?? [] };
    } catch (e) {
      if (e instanceof ErpError && (e.status === 404 || e.status === 400)) {
        return { data: [], meta: { employee_advance_unavailable: true } };
      }
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Full Expense Claim doc (child `expenses` lines, remark, etc.). Must be registered after `/pending-approval` so that path is not captured as `:id`. */
  app.get("/v1/expenses/:id", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    try {
      const cur = (await erp.getDoc(ctx.creds, "Expense Claim", name)) as Record<string, unknown>;
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Claim not in your Company" });
      }

      let allowed = false;
      if (ctx.canSubmitOnBehalf) {
        allowed = true;
      } else {
        const mine = await erp.listDocs(ctx.creds, "Employee", {
          filters: [
            ["user_id", "=", ctx.userEmail],
            ["company", "=", ctx.company],
          ],
          fields: ["name"],
          limit_page_length: 1,
        });
        const empName = asRecord(mine.data?.[0])?.name;
        if (empName && String(cur.employee) === String(empName)) {
          allowed = true;
        } else {
          const approver = String(cur.expense_approver ?? "").trim().toLowerCase();
          const me = ctx.userEmail.trim().toLowerCase();
          if (approver === me) allowed = true;
        }
      }

      if (!allowed) {
        return reply.status(403).send({ error: "You do not have access to this expense claim" });
      }

      const pack = await loadCompanyRulesPack(ctx.company);
      const merged = mergeClaimPolicyWarnings(cur, pack) as Record<string, unknown>;
      const wf = evaluateApproveWorkflow(cur, pack, { canSubmitOnBehalf: ctx.canSubmitOnBehalf });
      if (wf) {
        const pw = (merged.policy_warnings as PolicyWarningPublic[] | undefined) ?? [];
        merged.policy_warnings = [...pw, { code: wf.code, message: wf.message, severity: wf.severity }];
      }
      return { data: attachCentyTwoStageExpenseRow(merged) };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Update draft claim fields/lines (audit-safe lifecycle: draft only). */
  app.patch("/v1/expenses/:id", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    const body = (req.body ?? {}) as Record<string, unknown>;
    try {
      const cur = (await erp.getDoc(ctx.creds, "Expense Claim", name)) as Record<string, unknown>;
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Claim not in your Company" });
      }
      if (!isDraftClaim(cur)) {
        return reply.status(409).send({ error: "Only draft claims can be edited" });
      }
      if (isPaidClaim(cur) || isTerminalApproval(cur)) {
        return reply.status(409).send({ error: "Approved/paid claims are locked for audit" });
      }

      if (!ctx.canSubmitOnBehalf) {
        const mine = await erp.listDocs(ctx.creds, "Employee", {
          filters: [
            ["user_id", "=", ctx.userEmail],
            ["company", "=", ctx.company],
          ],
          fields: ["name"],
          limit_page_length: 1,
        });
        const empName = asRecord(mine.data?.[0])?.name;
        if (!empName || String(cur.employee) !== String(empName)) {
          return reply.status(403).send({ error: "You may only edit your own draft claims" });
        }
      }

      const patch: Record<string, unknown> = {};
      if (typeof body.posting_date === "string" && body.posting_date.trim()) {
        patch.posting_date = body.posting_date.trim();
      }
      if (Array.isArray(body.expenses)) {
        patch.expenses = body.expenses;
      }
      if (typeof body.remark === "string") {
        patch.remark = body.remark;
      }
      if (typeof body.employee === "string" && body.employee.trim()) {
        if (!ctx.canSubmitOnBehalf) {
          return reply.status(403).send({ error: "Only HR may change employee on draft claim" });
        }
        const other = await erp.getDoc(ctx.creds, "Employee", body.employee.trim());
        if (String(other.company) !== ctx.company) {
          return reply.status(403).send({ error: "Employee is not in your Company" });
        }
        patch.employee = body.employee.trim();
      }
      if (Object.keys(patch).length === 0) {
        return reply.status(400).send({ error: "No editable fields supplied" });
      }

      const updated = await erp.updateDoc(ctx.creds, "Expense Claim", name, patch);
      return { data: updated };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/expenses/:id/approve", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    try {
      const pack = await loadCompanyRulesPack(ctx.company);
      const result = await approveExpenseClaimOnce(ctx, name, pack);
      if (!result.ok) {
        if (result.status === 400) {
          req.log.warn({ company: ctx.company, claim: name }, "expense workflow blocked approve");
        }
        return reply.status(result.status).send({ error: result.error });
      }
      return { ok: true };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/expenses/:id/reject", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    const body = (req.body ?? {}) as Record<string, unknown>;
    const reason = typeof body.reason === "string" ? body.reason : "";

    try {
      const cur = await erp.getDoc(ctx.creds, "Expense Claim", name);
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Claim not in your Company" });
      }
      if (!isSubmittedClaim(cur)) {
        return reply.status(409).send({ error: "Only submitted claims can be rejected" });
      }
      if (isTerminalApproval(cur)) {
        return reply.status(409).send({ error: "Claim decision already finalised" });
      }
      const approver = String(cur.expense_approver ?? "").trim().toLowerCase();
      const me = ctx.userEmail.trim().toLowerCase();
      const canFinanceAct = !!ctx.canSubmitOnBehalf;
      if (approver !== me && !canFinanceAct) {
        return reply
          .status(403)
          .send({ error: "Only the assigned approver or finance-privileged user can reject this claim" });
      }
      await erp.callMethod(ctx.creds, "frappe.client.set_value", {
        doctype: "Expense Claim",
        name,
        fieldname: "approval_status",
        value: "Rejected",
      });
      if (reason) {
        try {
          await erp.callMethod(ctx.creds, "frappe.client.set_value", {
            doctype: "Expense Claim",
            name,
            fieldname: "remark",
            value: reason,
          });
        } catch {
          /* remark field may be absent on some versions */
        }
      }
      return { ok: true };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Mark claim as paid (finance action).
   * Phase 1 implementation records payout metadata and reimbursed total; immutable once paid.
   */
  app.post("/v1/expenses/:id/mark-paid", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    const body = (req.body ?? {}) as Record<string, unknown>;
    const paidAt = typeof body.paid_at === "string" && body.paid_at.trim()
      ? body.paid_at.trim()
      : new Date().toISOString().slice(0, 10);
    const paymentRef = typeof body.payment_ref === "string" ? body.payment_ref.trim() : "";
    const account = typeof body.payment_account === "string" ? body.payment_account.trim() : "";
    const paymentModeRaw = typeof body.payment_mode === "string" ? body.payment_mode.trim().toLowerCase() : "";
    const paymentMode = paymentModeRaw === "wallet" || paymentModeRaw === "offline" ? paymentModeRaw : "wallet";

    try {
      const cur = (await erp.getDoc(ctx.creds, "Expense Claim", name)) as Record<string, unknown>;
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Claim not in your Company" });
      }
      const approver = String(cur.expense_approver ?? "").trim().toLowerCase();
      const me = ctx.userEmail.trim().toLowerCase();
      const canFinanceAct = !!ctx.canSubmitOnBehalf;
      if (approver !== me && !canFinanceAct) {
        return reply
          .status(403)
          .send({ error: "Only the assigned approver or finance-privileged user can mark this claim as paid" });
      }
      if (!isSubmittedClaim(cur) || normalizeStatus(cur.approval_status) !== "approved") {
        return reply.status(409).send({ error: "Only approved submitted claims can be marked paid" });
      }
      if (isPaidClaim(cur)) {
        return reply.status(409).send({ error: "Claim already marked paid" });
      }

      const pack = await loadCompanyRulesPack(ctx.company);
      const paidBlock = evaluateMarkPaid(paymentMode as "wallet" | "offline", pack);
      if (paidBlock) {
        req.log.warn({ company: ctx.company, claim: name, code: paidBlock.code }, "expense policy blocked mark-paid");
        return reply.status(400).send({ error: `Policy: ${paidBlock.message}` });
      }

      const claimed = Number(cur.total_claimed_amount ?? cur.grand_total ?? 0);
      const reimbursed = Number.isFinite(claimed) && claimed > 0 ? claimed : 0;
      await erp.callMethod(ctx.creds, "frappe.client.set_value", {
        doctype: "Expense Claim",
        name,
        fieldname: "total_amount_reimbursed",
        value: reimbursed,
      });

      const remarkBits = [
        String(cur.remark ?? "").trim(),
        `Paid on ${paidAt} [${paymentMode}]${account ? ` via ${account}` : ""}${paymentRef ? ` (ref: ${paymentRef})` : ""}`.trim(),
      ].filter(Boolean);
      if (remarkBits.length > 0) {
        await erp.callMethod(ctx.creds, "frappe.client.set_value", {
          doctype: "Expense Claim",
          name,
          fieldname: "remark",
          value: remarkBits.join("\n"),
        });
      }

      return {
        ok: true,
        data: {
          name,
          paid_at: paidAt,
          payment_mode: paymentMode,
          payment_ref: paymentRef || null,
          payment_account: account || null,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Recall: cancel a submitted claim still awaiting approval (not paid, not finally approved/rejected).
   * Uses Frappe cancel — your site’s Expense Claim rules must allow cancellation in this state.
   */
  app.post("/v1/expenses/:id/recall", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    try {
      const cur = (await erp.getDoc(ctx.creds, "Expense Claim", name)) as Record<string, unknown>;
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Claim not in your Company" });
      }
      if (!isSubmittedClaim(cur)) {
        return reply.status(409).send({ error: "Only submitted claims can be recalled" });
      }
      if (isTerminalApproval(cur)) {
        return reply.status(409).send({ error: "Approved or rejected claims cannot be recalled" });
      }
      if (isPaidClaim(cur)) {
        return reply.status(409).send({ error: "Paid claims cannot be recalled" });
      }

      if (!ctx.canSubmitOnBehalf) {
        const empName = await getLinkedEmployeeName(ctx);
        if (!empName || String(cur.employee) !== String(empName)) {
          return reply.status(403).send({ error: "You may only recall your own claims" });
        }
      }

      await erp.cancelDoc(ctx.creds, "Expense Claim", name);
      req.log.info({ company: ctx.company, claim: name }, "expense claim recalled (cancelled)");
      return { ok: true };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
