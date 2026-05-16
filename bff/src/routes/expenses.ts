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
  isDbConfigured,
  loadCompanyRulesPack,
  mergeClaimPolicyWarnings,
  upsertCompanyRules,
  type PolicyWarningPublic,
} from "../lib/expenseRules.js";
import { logHrPolicyDenial } from "../lib/approvalPolicyLog.js";
import { attachCentyTwoStageExpenseRow, isFirstApproverFlagSet } from "../lib/twoStageCustomFields.js";

const erp = defaultClient();

/**
 * Resolve the canonical ERPNext Company docname from ctx.company.
 * ctx.company may be a display name that differs from the ERP docname.
 */
async function resolveCompanyDocName(creds: ErpCredentials, company: string): Promise<string> {
  const raw = String(company ?? "").trim();
  if (!raw) return raw;
  try {
    await erp.getDoc(creds, "Company", raw);
    return raw;
  } catch (e) {
    if (!(e instanceof ErpError)) throw e;
  }
  try {
    const rows = await erp.getList(creds, "Company", {
      filters: [["company_name", "=", raw]],
      fields: ["name"],
      limit_page_length: 1,
    });
    const found = (rows?.[0] as any)?.name;
    return typeof found === "string" && found.trim() ? found.trim() : raw;
  } catch {
    return raw;
  }
}

async function getCompanyDefaultCurrency(creds: ErpCredentials, companyDoc: string): Promise<string> {
  try {
    const c = (await erp.getDoc(creds, "Company", companyDoc)) as Record<string, unknown>;
    return String(c.default_currency ?? "").trim();
  } catch {
    return "";
  }
}

async function resolveEmployeeAdvanceExchangeRate(
  creds: ErpCredentials,
  employeeCurrency: string,
  companyCurrency: string,
  postingDate: string
): Promise<number> {
  const ec = employeeCurrency.trim();
  const cc = companyCurrency.trim();
  if (!ec || !cc) return 1;
  if (ec === cc) return 1;
  try {
    const raw = await erp.callMethod(creds, "erpnext.setup.utils.get_exchange_rate", {
      from_currency: ec,
      to_currency: cc,
      transaction_date: postingDate,
    });
    const msg = (raw as { message?: unknown })?.message;
    const n = typeof msg === "number" ? msg : Number(msg);
    if (Number.isFinite(n) && n > 0) return n;
  } catch {
    /* non-fatal; caller treats 0 as failure */
  }
  return 0;
}

/**
 * HRMS v15 `Employee Advance` has a required `exchange_rate`; drafts sometimes keep 0 (same KES / copy-paste in ERP).
 * Submit then fails. We always PATCH the draft, verify, then `frappe.client.set_value` if needed.
 */
async function ensureEmployeeAdvanceExchangeOnDraft(
  creds: ErpCredentials,
  companyDoc: string,
  name: string,
  employee: string,
  postingDate: string
): Promise<{
  companyCurrency: string;
  salaryCurrency: string;
  exchangeRate: number;
}> {
  const companyCurrency = await getCompanyDefaultCurrency(creds, companyDoc);
  if (!companyCurrency) {
    throw new HttpError("Set company default currency in ERPNext.", 400);
  }
  const empDoc = (await erp.getDoc(creds, "Employee", employee)) as Record<string, unknown>;
  const salaryCurrency = String(empDoc.salary_currency ?? "").trim() || companyCurrency;
  const rate = await resolveEmployeeAdvanceExchangeRate(
    creds,
    salaryCurrency,
    companyCurrency,
    postingDate
  );
  if (rate <= 0) {
    throw new HttpError("Add a currency exchange rate in ERP for this date.", 400);
  }

  const applyPatch = async () => {
    const fresh = (await erp.getDoc(creds, "Employee Advance", name)) as Record<string, unknown>;
    await erp.updateDoc(creds, "Employee Advance", name, {
      doctype: "Employee Advance",
      name,
      modified: fresh.modified,
      currency: salaryCurrency,
      exchange_rate: rate,
    });
  };

  await applyPatch();
  let after = (await erp.getDoc(creds, "Employee Advance", name)) as Record<string, unknown>;
  let n = Number(after.exchange_rate ?? 0);
  if (Number.isFinite(n) && n > 0) {
    return { companyCurrency, salaryCurrency, exchangeRate: rate };
  }

  try {
    await erp.callMethod(creds, "frappe.client.set_value", {
      doctype: "Employee Advance",
      name,
      fieldname: "exchange_rate",
      value: rate,
    });
  } catch (e) {
    console.warn("[BFF] frappe.client.set_value exchange_rate:", e);
  }
  after = (await erp.getDoc(creds, "Employee Advance", name)) as Record<string, unknown>;
  n = Number(after.exchange_rate ?? 0);
  if (!Number.isFinite(n) || n <= 0) {
    throw new HttpError(
      "Set exchange rate on this advance in ERP, save, then approve again from Pay Hub.",
      400
    );
  }
  return { companyCurrency, salaryCurrency, exchangeRate: rate };
}

type AccountHeadMeta = { account_type: string; account_currency: string };

async function getAccountHeadMeta(
  creds: ErpCredentials,
  name: string
): Promise<AccountHeadMeta | null> {
  try {
    const a = (await erp.getDoc(creds, "Account", name)) as Record<string, unknown>;
    return {
      account_type: String(a.account_type ?? ""),
      account_currency: String(a.account_currency ?? "").trim(),
    };
  } catch {
    return null;
  }
}

async function resolveReceivableEmployeeAdvanceAccount(
  creds: ErpCredentials,
  companyDoc: string,
  employee: string,
  salaryCurrency: string
): Promise<string | null> {
  const sc = salaryCurrency.trim();
  if (!sc) return null;

  const comp = (await erp.getDoc(creds, "Company", companyDoc)) as Record<string, unknown>;
  const fromCompany = String(comp.default_employee_advance_account ?? "").trim();
  const emp = (await erp.getDoc(creds, "Employee", employee)) as Record<string, unknown>;
  const fromEmployee = String(emp.employee_advance_account ?? "").trim();

  const tryNames: string[] = [];
  if (fromCompany) tryNames.push(fromCompany);
  if (fromEmployee && fromEmployee !== fromCompany) tryNames.push(fromEmployee);

  for (const accName of tryNames) {
    const meta = await getAccountHeadMeta(creds, accName);
    if (!meta) continue;
    if (meta.account_type !== "Receivable") continue;
    if (meta.account_currency !== sc) continue;
    return accName;
  }

  const rows = await erp.getList(creds, "Account", {
    filters: [
      ["company", "=", companyDoc],
      ["account_type", "=", "Receivable"],
      ["is_group", "=", 0],
      ["disabled", "=", 0],
      ["account_currency", "=", sc],
    ],
    fields: ["name", "account_name"],
    order_by: "name asc",
    limit_page_length: 80,
  });

  for (const raw of rows) {
    const r = asRecord(raw);
    if (!r) continue;
    const n = `${String(r.name ?? "")} ${String(r.account_name ?? "")}`.toLowerCase();
    if (n.includes("advance") || n.includes("employee")) {
      return String(r.name ?? "").trim() || null;
    }
  }
  if (rows.length === 1) {
    const r = asRecord(rows[0]);
    if (r?.name) return String(r.name);
  }
  return null;
}

async function alignCompanyDefaultEmployeeAdvanceAccount(
  creds: ErpCredentials,
  companyDoc: string,
  salaryCurrency: string,
  resolvedAdvance: string
): Promise<void> {
  const comp = (await erp.getDoc(creds, "Company", companyDoc)) as Record<string, unknown>;
  const cur = String(comp.default_employee_advance_account ?? "").trim();
  if (cur === resolvedAdvance) return;
  if (!cur) return;
  const m = await getAccountHeadMeta(creds, cur);
  const curOk = Boolean(
    m && m.account_type === "Receivable" && m.account_currency === salaryCurrency
  );
  if (curOk) return;
  await erp.callMethod(creds, "frappe.client.set_value", {
    doctype: "Company",
    name: companyDoc,
    fieldname: "default_employee_advance_account",
    value: resolvedAdvance,
  });
}

async function alignEmployeeMasterAdvanceAccount(
  creds: ErpCredentials,
  employee: string,
  salaryCurrency: string,
  resolvedAdvance: string,
  existingEmp?: Record<string, unknown>
): Promise<void> {
  const emp = existingEmp ?? ((await erp.getDoc(creds, "Employee", employee)) as Record<string, unknown>);
  const cur = String(emp.employee_advance_account ?? "").trim();
  if (cur === resolvedAdvance) return;
  if (cur) {
    const m = await getAccountHeadMeta(creds, cur);
    const curOk = Boolean(
      m && m.account_type === "Receivable" && m.account_currency === salaryCurrency
    );
    if (curOk) return;
  }
  await erp.callMethod(creds, "frappe.client.set_value", {
    doctype: "Employee",
    name: employee,
    fieldname: "employee_advance_account",
    value: resolvedAdvance,
  });
}

/** Max advance as a share of latest Salary Structure Assignment `base` (monthly, company currency). */
const SALARY_ADVANCE_MAX_SALARY_FRACTION = 0.5;

/** Latest SSA `base` in company / payroll terms (same as GET /v1/payroll/team). */
async function getLatestSalaryBase(
  creds: ErpCredentials,
  companyDoc: string,
  employee: string
): Promise<number | null> {
  const rows = await erp.getList(creds, "Salary Structure Assignment", {
    filters: [
      ["company", "=", companyDoc],
      ["employee", "=", employee],
    ],
    fields: ["base", "from_date"],
    order_by: "from_date desc",
    limit_page_length: 1,
  });
  const r = asRecord(rows[0]);
  if (!r) return null;
  const base = Number(r.base ?? 0);
  if (!Number.isFinite(base) || base <= 0) return null;
  return base;
}

/** Inclusive [start, end] for `postingDate`'s calendar month (YYYY-MM-DD). */
function postingMonthRange(postingDate: string): { start: string; end: string } {
  const m = String(postingDate ?? "").match(/^(\d{4})-(\d{2})/);
  const y = m ? parseInt(m[1], 10) : new Date().getFullYear();
  const mo = m ? parseInt(m[2], 10) : new Date().getMonth() + 1;
  const start = `${y}-${String(mo).padStart(2, "0")}-01`;
  const lastDay = new Date(y, mo, 0).getDate();
  const end = `${y}-${String(mo).padStart(2, "0")}-${String(lastDay).padStart(2, "0")}`;
  return { start, end };
}

function isPastPostingDate(postingDate: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(postingDate ?? "").trim())) return false;
  const now = new Date();
  const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
  return postingDate < today;
}

/** Month-to-date non-cancelled advance total in company currency for the posting month. */
async function sumEmployeeAdvancesInSameMonthCompanyCurrency(
  creds: ErpCredentials,
  companyDoc: string,
  companyCurrency: string,
  employee: string,
  postingDate: string,
  options?: { excludeName?: string | null }
): Promise<number> {
  const { start, end } = postingMonthRange(postingDate);
  const rows = await erp.getList(creds, "Employee Advance", {
    filters: [
      ["company", "=", companyDoc],
      ["employee", "=", employee],
      ["posting_date", ">=", start],
      ["posting_date", "<=", end],
      ["docstatus", "!=", 2],
    ],
    fields: ["name", "advance_amount", "currency", "exchange_rate"],
    limit_page_length: 200,
  });
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const excludeName = String(options?.excludeName ?? "").trim();
  let total = 0;
  for (const raw of rows) {
    const row = asRecord(raw);
    const name = String(row?.name ?? "").trim();
    if (excludeName && name === excludeName) continue;
    const amount = Number(row?.advance_amount ?? 0);
    if (!Number.isFinite(amount) || amount <= 0) continue;
    const currency = String(row?.currency ?? "").trim();
    const exchangeRate = Number(row?.exchange_rate ?? 0);
    const inCompanyCurrency =
      !currency || currency === companyCurrency
        ? amount
        : amount * (Number.isFinite(exchangeRate) && exchangeRate > 0 ? exchangeRate : 1);
    total += inCompanyCurrency;
  }
  return Math.round(total * 100) / 100;
}
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

/** Pending list: assigned approver queue, or company-wide submitted queue for finance / HR (`canSubmitOnBehalf`). */
function pendingApprovalListFilters(ctx: HrContext): unknown[] {
  if (ctx.canSubmitOnBehalf) {
    return [
      ["company", "=", ctx.company],
      ["docstatus", "=", 1],
      ["approval_status", "not in", ["Approved", "Rejected"]],
    ];
  }
  return pendingClaimFilters(ctx);
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

/** Sum `amount` on Expense Claim Detail rows per parent claim (receipt / line totals from ERP). */
async function sumExpenseLineAmountsByParent(
  creds: ErpCredentials,
  parentNames: string[],
): Promise<Record<string, number>> {
  const unique = Array.from(new Set(parentNames.map((n) => String(n).trim()).filter(Boolean)));
  const sums: Record<string, number> = {};
  if (unique.length === 0) return sums;

  const chunkSize = 45;
  for (let i = 0; i < unique.length; i += chunkSize) {
    const chunk = unique.slice(i, i + chunkSize);
    const res = await erp.listDocs(creds, "Expense Claim Detail", {
      filters: [["parent", "in", chunk]],
      fields: ["parent", "amount"],
      limit_page_length: Math.min(5000, chunk.length * 100),
    });
    for (const row of res.data ?? []) {
      const rec = asRecord(row);
      if (!rec) continue;
      const p = String(rec.parent ?? "").trim();
      if (!p) continue;
      const amt = Number(rec.amount ?? 0);
      sums[p] = (sums[p] ?? 0) + (Number.isFinite(amt) ? amt : 0);
    }
  }
  return sums;
}

/** Adds `centy_receipt_lines_total` for list/export rows (sum of claim line amounts). */
async function enrichExpenseClaimRowsWithReceiptLineTotals(
  creds: ErpCredentials,
  rows: unknown[],
): Promise<unknown[]> {
  const names = rows.map((r) => String(asRecord(r)?.name ?? "").trim()).filter(Boolean);
  let sums: Record<string, number> = {};
  try {
    sums = await sumExpenseLineAmountsByParent(creds, names);
  } catch (e) {
    console.warn("[expenses] receipt line totals skipped:", e instanceof Error ? e.message : String(e));
  }
  return rows.map((r) => {
    const rec = asRecord(r);
    if (!rec) return r;
    const name = String(rec.name ?? "").trim();
    const total = name ? sums[name] : undefined;
    return {
      ...rec,
      centy_receipt_lines_total:
        total !== undefined && Number.isFinite(total) ? total : null,
    };
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
      const enriched = await enrichExpenseClaimRowsWithReceiptLineTotals(ctx.creds, slice);
      const data = await mapRowsWithPolicy(enriched, ctx.company);
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
      const resolvedCompany = await resolveCompanyDocName(ctx.creds, ctx.company);
      const filters: unknown[] = [["company", "=", resolvedCompany]];
      if (!ctx.canSubmitOnBehalf) {
        // Regular employee: restrict to their own claims (try user_id then personal_email)
        let empName: string | undefined;
        for (const field of ["user_id", "personal_email"] as const) {
          const rows = await erp.listDocs(ctx.creds, "Employee", {
            filters: [
              [field, "=", ctx.userEmail],
              ["company", "=", resolvedCompany],
            ],
            fields: ["name"],
            limit_page_length: 1,
          });
          const row = asRecord(rows.data?.[0]);
          if (row?.name && typeof row.name === "string") { empName = row.name; break; }
        }
        if (!empName) {
          // Return empty summary rather than 403 so the card shows 0s gracefully
          return { data: { drafts: 0, in_review: 0, approved: 0, queue: 0, total_in_review: 0, total_approved: 0, total_drafts: 0, scan_capped: false } };
        }
        filters.push(["employee", "=", empName]);
      }

      const mineRes = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters,
        fields: ["docstatus", "approval_status", "total_claimed_amount", "grand_total"],
        order_by: "modified desc",
        limit_page_length: SUMMARY_SCAN_CAP,
      });
      const mineRows = mineRes.data ?? [];
      let drafts = 0, approved = 0, inReview = 0;
      let totalDrafts = 0, totalApproved = 0, totalInReview = 0;
      for (const r of mineRows) {
        const rec = asRecord(r);
        if (!rec) continue;
        const amt = Number(rec.total_claimed_amount ?? rec.grand_total ?? 0);
        if (Number(rec.docstatus) === 0) { drafts++; totalDrafts += amt; }
        else if (String(rec.approval_status ?? "").toLowerCase() === "approved") { approved++; totalApproved += amt; }
        else { inReview++; totalInReview += amt; }
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
          total_in_review: totalInReview,
          total_approved: totalApproved,
          total_drafts: totalDrafts,
          scan_capped: mineRows.length >= SUMMARY_SCAN_CAP || queue >= SUMMARY_SCAN_CAP,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Policy / workflow / feature flags. Empty defaults when DB not configured. */
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
          supabase_configured: isDbConfigured(),
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
        // Creating own claim — resolve company docname first (ctx.company may be a display name
        // that doesn't match the ERP docname), then look up by user_id / personal_email fallback.
        const resolvedCompany = await resolveCompanyDocName(ctx.creds, ctx.company);
        let selfEmp: Record<string, unknown> | null = null;
        for (const field of ["user_id", "personal_email"] as const) {
          try {
            const rows = await erp.listDocs(ctx.creds, "Employee", {
              filters: [[field, "=", ctx.userEmail], ["company", "=", resolvedCompany]],
              fields: ["name", "company", "department", "expense_approver"],
              limit_page_length: 1,
            });
            const row = asRecord(rows.data?.[0]);
            if (row?.name) { selfEmp = row; break; }
          } catch { /* try next field */ }
        }
        if (!selfEmp?.name) {
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        }
        employee = String(selfEmp.name);
        employeeDept = String(selfEmp.department ?? "").trim() || undefined;
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
        filters: pendingApprovalListFilters(ctx),
        fields: pendFields,
        order_by: "modified desc",
        limit_start: limitStart,
        limit_page_length: take,
      });
      const raw = res.data ?? [];
      const hasMore = raw.length > pageSize;
      const slice = hasMore ? raw.slice(0, pageSize) : raw;
      const enriched = await enrichExpenseClaimRowsWithReceiptLineTotals(ctx.creds, slice);
      const data = await mapRowsWithPolicy(enriched, ctx.company);
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
   * Approved, submitted claims with no reimbursement recorded yet (finance payout queue).
   * Only users with `canSubmitOnBehalf` may list this.
   */
  app.get("/v1/expenses/ready-to-pay", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only finance users may view the ready-to-pay queue" });
    }

    try {
      const { page, pageSize, limitStart } = parsePageParams(req);
      const take = pageSize + 1;
      const payFields = [
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
        payFields.push(config.EXPENSE_FIRST_APPROVER_FIELD);
      }
      const res = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters: [
          ["company", "=", ctx.company],
          ["docstatus", "=", 1],
          ["approval_status", "=", "Approved"],
          ["total_amount_reimbursed", "=", 0],
        ],
        fields: payFields,
        order_by: "modified desc",
        limit_start: limitStart,
        limit_page_length: take,
      });
      const raw = res.data ?? [];
      const unpaid = raw.filter((row) => {
        const rec = asRecord(row);
        if (!rec) return false;
        return !isPaidClaim(rec);
      });
      const hasMore = unpaid.length > pageSize;
      const slice = hasMore ? unpaid.slice(0, pageSize) : unpaid;
      const enriched = await enrichExpenseClaimRowsWithReceiptLineTotals(ctx.creds, slice);
      const data = await mapRowsWithPolicy(enriched, ctx.company);
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
      const enriched = await enrichExpenseClaimRowsWithReceiptLineTotals(ctx.creds, raw);
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
        "centy_receipt_lines_total",
        "total_sanctioned_amount",
        "total_amount_reimbursed",
      ];
      const lines = [header.join(",")];
      for (const row of enriched) {
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

  /**
   * Salary-advance eligibility for the employee portal / HR issue flow.
   * Uses the same latest Salary Structure Assignment base rule as Employee Advance creation.
   */
  app.get("/v1/expenses/advances/eligibility", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    try {
      const query = (req.query as Record<string, unknown>) ?? {};
      const qEmp = String(query.employee ?? "").trim();
      const eligibilityPostingDate = /^\d{4}-\d{2}-\d{2}$/.test(String(query.posting_date ?? "").trim())
        ? String(query.posting_date).trim()
        : new Date().toISOString().slice(0, 10);
      const companyDoc = await resolveCompanyDocName(ctx.creds, ctx.company);
      const companyCurrency = await getCompanyDefaultCurrency(ctx.creds, companyDoc);
      if (!companyCurrency) {
        return reply.status(400).send({ error: "Set company default currency in ERPNext." });
      }

      let employee = "";
      if (ctx.canSubmitOnBehalf) {
        employee = qEmp;
        if (!employee) {
          const mine = await getLinkedEmployeeName(ctx);
          if (mine) employee = mine;
        }
        if (!employee) {
          return {
            eligible: false,
            maxAmount: null,
            baseSalary: null,
            reason: "no_employee_linked",
          };
        }
      } else {
        const mine = await getLinkedEmployeeName(ctx);
        if (!mine) {
          return {
            eligible: false,
            maxAmount: null,
            baseSalary: null,
            reason: "no_employee_linked",
          };
        }
        if (qEmp && qEmp !== mine) {
          return reply.status(403).send({ error: "You can only check advance eligibility for your own profile." });
        }
        employee = mine;
      }

      const empDoc = (await erp.getDoc(ctx.creds, "Employee", employee)) as Record<string, unknown>;
      if (String(empDoc.company ?? "").trim() !== companyDoc) {
        return reply.status(403).send({ error: "Employee not in your Company" });
      }

      const monthlySalaryBase = await getLatestSalaryBase(ctx.creds, companyDoc, employee);
      if (monthlySalaryBase == null) {
        return {
          eligible: false,
          maxAmount: null,
          baseSalary: null,
          reason: "no_salary_structure",
        };
      }

      const maxAmount = Math.round(monthlySalaryBase * SALARY_ADVANCE_MAX_SALARY_FRACTION * 100) / 100;
      const alreadyRequestedThisMonth = await sumEmployeeAdvancesInSameMonthCompanyCurrency(
        ctx.creds,
        companyDoc,
        companyCurrency,
        employee,
        eligibilityPostingDate,
      );
      const remainingAmount = Math.max(0, Math.round((maxAmount - alreadyRequestedThisMonth) * 100) / 100);
      return {
        eligible: remainingAmount > 0,
        maxAmount: remainingAmount,
        baseSalary: monthlySalaryBase,
        usedAmount: alreadyRequestedThisMonth,
        monthlyCap: maxAmount,
        reason: remainingAmount > 0 ? "ok" : "monthly_limit_reached",
      };
    } catch (e) {
      if (e instanceof ErpError && e.status === 404) {
        return {
          eligible: false,
          maxAmount: null,
          baseSalary: null,
          reason: "no_salary_structure",
        };
      }
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * POST /v1/expenses/advances
   * Creates **Employee Advance** in ERPNext HR.
   * HR may pass `employee`; employees without submit-on-behalf may only create for their linked Employee row.
   * Blocked if no SSA base or if the month-to-date total would exceed half the monthly salary.
   */
  app.post("/v1/expenses/advances", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();

    try {
      let empName: string;
      if (ctx.canSubmitOnBehalf) {
        if (!qEmp) return reply.status(400).send({ error: "employee query param required for HR users." });
        empName = qEmp;
      } else {
        const mine = await getLinkedEmployeeName(ctx);
        if (!mine) {
          return reply.send({ eligible: false, maxAmount: null, reason: "no_employee_linked" });
        }
        if (qEmp && qEmp !== mine) {
          return reply.status(403).send({ error: "You can only check advance eligibility for your own profile." });
        }
        empName = mine;
      }

      const companyDoc = await resolveCompanyDocName(ctx.creds, ctx.company);
      const companyCurrency = await getCompanyDefaultCurrency(ctx.creds, companyDoc);
      if (!companyCurrency) {
        return reply.status(400).send({ error: "Set company default currency in ERPNext." });
      }

      const empDoc = (await erp.getDoc(ctx.creds, "Employee", empName)) as Record<string, unknown>;
      if (String(empDoc.company ?? "").trim() !== companyDoc) {
        return reply.status(403).send({ error: "Employee not in your Company" });
      }

      const monthlySalaryBase = await getLatestSalaryBase(ctx.creds, companyDoc, empName);
      if (monthlySalaryBase == null) {
        return {
          eligible: false,
          maxAmount: null,
          baseSalary: null,
          reason: "no_salary_structure",
        };
      }

      const maxAmount = Math.round(monthlySalaryBase * SALARY_ADVANCE_MAX_SALARY_FRACTION * 100) / 100;
      const alreadyRequestedThisMonth = await sumEmployeeAdvancesInSameMonthCompanyCurrency(
        ctx.creds,
        companyDoc,
        companyCurrency,
        empName,
        eligibilityPostingDate,
      );
      const remainingAmount = Math.max(0, Math.round((maxAmount - alreadyRequestedThisMonth) * 100) / 100);
      return {
        eligible: remainingAmount > 0,
        maxAmount: remainingAmount,
        baseSalary: monthlySalaryBase,
        usedAmount: alreadyRequestedThisMonth,
        monthlyCap: maxAmount,
        reason: remainingAmount > 0 ? "ok" : "monthly_limit_reached",
      };
    } catch (e) {
      if (e instanceof ErpError && e.status === 404) {
        return {
          eligible: false,
          maxAmount: null,
          baseSalary: null,
          reason: "no_salary_structure",
        };
      }
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Create an Employee Advance (ERP draft). HR admins may supply `employee`; regular employees are self-only. */
  app.post("/v1/expenses/advances", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const body = (req.body ?? {}) as Record<string, unknown>;

    try {
      const companyDoc = await resolveCompanyDocName(ctx.creds, ctx.company);
      const companyCurrency = await getCompanyDefaultCurrency(ctx.creds, companyDoc);
      if (!companyCurrency) {
        return reply.status(400).send({ error: "Set company default currency in ERPNext." });
      }

      let empName = "";
      if (ctx.canSubmitOnBehalf) {
        empName = String(body.employee ?? "").trim();
        if (!empName) return reply.status(400).send({ error: "employee is required for HR users." });
      } else {
        const mine = await getLinkedEmployeeName(ctx);
        if (!mine) return reply.status(403).send({ error: "No Employee linked to your account." });
        const requested = String(body.employee ?? "").trim();
        if (requested && requested !== mine) {
          return reply.status(403).send({ error: "You can only request an advance for your own profile." });
        }
        empName = mine;
      }

      const advanceAmount = Number(body.advance_amount ?? body.amount ?? 0);
      if (!Number.isFinite(advanceAmount) || advanceAmount <= 0) {
        return reply.status(400).send({ error: "advance_amount must be a positive number" });
      }

      const postingDate = /^\d{4}-\d{2}-\d{2}$/.test(String(body.posting_date ?? "").trim())
        ? String(body.posting_date).trim()
        : new Date().toISOString().slice(0, 10);
      if (isPastPostingDate(postingDate)) {
        return reply.status(400).send({ error: "Salary advance date cannot be in the past." });
      }
      const purpose = String(body.purpose ?? body.notes ?? "Salary advance").trim() || "Salary advance";

      const empDoc = (await erp.getDoc(ctx.creds, "Employee", empName)) as Record<string, unknown>;
      const empCompany = String(empDoc.company ?? "").trim();
      if (empCompany !== companyDoc) {
        return reply.status(403).send({ error: "That employee is not in your company." });
      }

      const monthlySalaryBase = await getLatestSalaryBase(ctx.creds, companyDoc, empName);
      if (monthlySalaryBase == null) {
        return reply.status(400).send({ error: "Assign a salary in payroll before salary advance." });
      }

      const syncedExchange = await ensureEmployeeAdvanceExchangeOnDraft(
        ctx.creds,
        companyDoc,
        "",
        empName,
        postingDate,
      ).catch(() => null);

      const salaryCurrency = String(syncedExchange?.salaryCurrency ?? companyCurrency).trim() || companyCurrency;
      const exchangeRate = Number(syncedExchange?.exchangeRate ?? 1) || 1;

      const maxInCompany = monthlySalaryBase * SALARY_ADVANCE_MAX_SALARY_FRACTION;
      const advanceInCompany =
        salaryCurrency === companyCurrency ? advanceAmount : advanceAmount * exchangeRate;
      const alreadyRequestedThisMonth = await sumEmployeeAdvancesInSameMonthCompanyCurrency(
        ctx.creds,
        companyDoc,
        companyCurrency,
        empName,
        postingDate,
      );
      const round2 = (n: number) => Math.round(n * 100) / 100;
      const remainingInCompany = Math.max(0, round2(maxInCompany - alreadyRequestedThisMonth));
      if (round2(advanceInCompany) > round2(remainingInCompany)) {
        return reply.status(400).send({
          error: remainingInCompany > 0
            ? `Advance cannot exceed the remaining monthly salary advance balance of ${remainingInCompany.toFixed(2)}.`
            : "This employee has already used the full salary advance limit for this month.",
        });
      }

      const created = await erp.createDoc(ctx.creds, "Employee Advance", {
        employee: empName,
        employee_name: String(empDoc.employee_name ?? empDoc.name ?? empName),
        company: companyDoc,
        posting_date: postingDate,
        advance_amount: advanceAmount,
        purpose,
      }) as Record<string, unknown>;
      return reply.status(201).send({ data: created });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Get single Employee Advance document. Employees may only access their own. */
  app.get("/v1/expenses/advances/:name", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { name: string }).name;
    try {
      const doc = (await erp.getDoc(ctx.creds, "Employee Advance", name)) as Record<string, unknown>;
      if (String(doc.company) !== ctx.company) {
        return reply.status(403).send({ error: "Advance not in your Company." });
      }
      if (!ctx.canSubmitOnBehalf) {
        const empName = await getLinkedEmployeeName(ctx);
        if (!empName || String(doc.employee) !== empName) {
          return reply.status(403).send({ error: "You do not have access to this advance." });
        }
      }
      return { data: doc };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Submit draft Employee Advance (docstatus 0 → submitted). */
  app.post("/v1/expenses/advances/:name/submit", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR access is required to submit this advance." });
    }
    const name = String((req.params as { name: string }).name ?? "").trim();
    if (!name) return reply.status(400).send({ error: "Advance name is required" });
    try {
      const companyDoc = await resolveCompanyDocName(ctx.creds, ctx.company);
      const cur = (await erp.getDoc(ctx.creds, "Employee Advance", name)) as Record<string, unknown>;
      if (String(cur.company) !== companyDoc) {
        return reply.status(403).send({ error: "Advance not in your company." });
      }
      const ds = Number(cur.docstatus ?? 0);
      if (ds !== 0) {
        return reply.status(409).send({ error: "This advance is already submitted in payroll." });
      }

      // Same business rules as POST /v1/expenses/advances (month-to-date total within half-salary cap).
      const employee = String(cur.employee ?? "").trim();
      const postingDate = /^\d{4}-\d{2}-\d{2}$/.test(String(cur.posting_date ?? "").trim())
        ? String(cur.posting_date).trim()
        : new Date().toISOString().slice(0, 10);
      if (isPastPostingDate(postingDate)) {
        return reply.status(400).send({ error: "Salary advance date cannot be in the past." });
      }

      let companyCurrency = "";
      let exchangeRate = 0;
      let salaryCurrency = "";
      if (employee) {
        const synced = await ensureEmployeeAdvanceExchangeOnDraft(
          ctx.creds,
          companyDoc,
          name,
          employee,
          postingDate
        );
        companyCurrency = synced.companyCurrency;
        salaryCurrency = synced.salaryCurrency;
        exchangeRate = synced.exchangeRate;
      }

      const advanceAmount = Number(cur.advance_amount ?? 0);
      if (employee && Number.isFinite(advanceAmount) && advanceAmount > 0) {
        const monthlySalaryBase = await getLatestSalaryBase(ctx.creds, companyDoc, employee);
        if (monthlySalaryBase == null) {
          return reply
            .status(400)
            .send({ error: "Assign a salary in payroll before salary advance." });
        }
        if (exchangeRate <= 0) {
          return reply.status(400).send({ error: "Add a currency exchange rate in ERP for this date." });
        }
        const maxInCompany = monthlySalaryBase * SALARY_ADVANCE_MAX_SALARY_FRACTION;
        const advanceInCompany =
          salaryCurrency === companyCurrency ? advanceAmount : advanceAmount * exchangeRate;
        const round2 = (n: number) => Math.round(n * 100) / 100;
        const existingMonthTotal = await sumEmployeeAdvancesInSameMonthCompanyCurrency(
          ctx.creds,
          companyDoc,
          companyCurrency,
          employee,
          postingDate,
          { excludeName: name },
        );
        const remainingInCompany = Math.max(0, round2(maxInCompany - existingMonthTotal));
        if (round2(advanceInCompany) > round2(remainingInCompany)) {
          return reply.status(400).send({
            error: remainingInCompany > 0
              ? `Advance cannot exceed the remaining monthly salary advance balance of ${remainingInCompany.toFixed(2)}.`
              : "This employee has already used the full salary advance limit for this month.",
          });
        }
      }
      /* Full doc: `frappe.client.submit` uses `get_doc(sparse dict)` which does not reload from DB;
       * omitted fields (e.g. exchange_rate) would stay at defaults and fail HR validate. */
      await erp.submitDoc(ctx.creds, "Employee Advance", name, { mode: "full" });
      const after = (await erp.getDoc(ctx.creds, "Employee Advance", name)) as Record<string, unknown>;
      return { data: after };
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      if (e instanceof ErpError && e.status === 404) {
        return reply.status(404).send({ error: "Advance not found in payroll." });
      }
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Delete draft Employee Advance only (docstatus 0). HR: any draft in company; others: own draft only. */
  app.delete("/v1/expenses/advances/:name", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { name: string }).name;
    try {
      const doc = (await erp.getDoc(ctx.creds, "Employee Advance", name)) as Record<string, unknown>;
      if (String(doc.company) !== ctx.company) {
        return reply.status(403).send({ error: "Advance not in your Company." });
      }
      if (!ctx.canSubmitOnBehalf) {
        const empName = await getLinkedEmployeeName(ctx);
        if (!empName || String(doc.employee) !== empName) {
          return reply.status(403).send({ error: "You do not have access to this advance." });
        }
      }
      if (Number(doc.docstatus ?? 0) !== 0) {
        return reply.status(409).send({ error: "Only draft advances can be deleted." });
      }
      await erp.deleteDoc(ctx.creds, "Employee Advance", name);
      return reply.status(200).send({ ok: true });
    } catch (e) {
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
