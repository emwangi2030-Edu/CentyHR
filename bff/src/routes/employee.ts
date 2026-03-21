import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import { defaultClient } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import { insertEmployeeInvite, invitesAvailable } from "../lib/employeeInvites.js";
import type { HrContext } from "../types.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError) {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

const SUMMARY_SCAN_CAP = 5000;
/** Max expense claim rows scanned for per-employee aggregates (snapshot + heatmap). */
const EMPLOYEE_INSIGHTS_CLAIM_CAP = 2000;
/** Max rows for connection counts (honest cap — UI may show "500+"). */
const CONNECTION_COUNT_CAP = 500;

type EmployeeReadResult =
  | { ok: true; doc: Record<string, unknown> }
  | { ok: false; status: number; error: string };

/**
 * Load an Employee doc if the caller may read it: same company, and non-HR users only their own row.
 */
async function loadEmployeeReadableByCaller(ctx: HrContext, employeeId: string): Promise<EmployeeReadResult> {
  try {
    const cur = (await erp.getDoc(ctx.creds, "Employee", employeeId)) as Record<string, unknown>;
    if (String(cur.company) !== ctx.company) {
      return { ok: false, status: 403, error: "Employee not in your Company" };
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
      const myName = asRecord(mine.data?.[0])?.name;
      if (String(myName) !== employeeId) {
        return { ok: false, status: 403, error: "You can only access your own employee record" };
      }
    }
    return { ok: true, doc: cur };
  } catch (e) {
    if (e instanceof ErpError) {
      return {
        ok: false,
        status: e.status >= 500 ? 502 : e.status,
        error: "Employee not found",
      };
    }
    throw e;
  }
}

async function ensureEmployeeInsightAccess(
  ctx: HrContext,
  employeeId: string
): Promise<{ ok: true } | { ok: false; status: number; error: string }> {
  const r = await loadEmployeeReadableByCaller(ctx, employeeId);
  if (!r.ok) return r;
  return { ok: true };
}

function parsePageParams(req: FastifyRequest): { page: number; pageSize: number; limitStart: number } {
  const q = (req.query ?? {}) as Record<string, unknown>;
  const page = Math.max(1, parseInt(String(q.page ?? "1"), 10) || 1);
  const raw = parseInt(String(q.page_size ?? "25"), 10) || 25;
  const pageSize = Math.min(100, Math.max(10, raw));
  return { page, pageSize, limitStart: (page - 1) * pageSize };
}

function parseSearchQuery(req: FastifyRequest): string {
  const q = (req.query ?? {}) as Record<string, unknown>;
  return String(q.q ?? "").trim().slice(0, 120);
}

/** Fields allowed when HR creates an Employee from Pay Hub (minimal onboarding). */
const EMPLOYEE_CREATE_FIELDS = new Set([
  "first_name",
  "last_name",
  "gender",
  "date_of_joining",
  "department",
  "designation",
  "branch",
  "cell_number",
  "prefered_email",
  "company_email",
  "personal_email",
  "reports_to",
]);

const EMPLOYEE_LIST_FIELDS = [
  "name",
  "employee_name",
  "department",
  "designation",
  "branch",
  "status",
  "user_id",
  "date_of_joining",
  "cell_number",
  "company_email",
  "prefered_email",
];

function normalizeStatus(s: unknown): string {
  return String(s ?? "").trim().toLowerCase();
}

/** Fields Pay Hub may PATCH on Employee (ERPNext); avoids arbitrary writes. */
const EMPLOYEE_PATCH_WHITELIST = new Set([
  "cell_number",
  "prefered_email",
  "personal_email",
  "company_email",
  "expense_approver",
]);

const EMPLOYEE_FIELDS = [
  "name",
  "employee_name",
  "first_name",
  "last_name",
  "company",
  "department",
  "designation",
  "branch",
  "reports_to",
  "gender",
  "date_of_birth",
  "date_of_joining",
  "cell_number",
  "prefered_email",
  "company_email",
  "personal_email",
  "user_id",
  "expense_approver",
  "status",
];

export const employeeRoutes: FastifyPluginAsync = async (app) => {
  /** Bio / master data for the logged-in user's Employee row (same Company). */
  app.get("/v1/me/employee", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    try {
      let rows: unknown[];
      try {
        rows = await erp.getList(ctx.creds, "Employee", {
          filters: [
            ["user_id", "=", ctx.userEmail],
            ["company", "=", ctx.company],
          ],
          fields: EMPLOYEE_FIELDS,
          limit_page_length: 1,
        });
      } catch (first) {
        if (first instanceof ErpError && first.status >= 500) {
          const res = await erp.listDocs(ctx.creds, "Employee", {
            filters: [
              ["user_id", "=", ctx.userEmail],
              ["company", "=", ctx.company],
            ],
            fields: EMPLOYEE_FIELDS,
            limit_page_length: 1,
          });
          rows = res.data ?? [];
        } else {
          throw first;
        }
      }

      const row = rows[0];
      if (!row || typeof row !== "object") {
        return reply.status(404).send({
          error: "No employee record for your account in this company.",
          code: "HR_NO_EMPLOYEE",
          company: ctx.company,
        });
      }
      const rec = row as Record<string, unknown>;
      const { company: _c, ...data } = rec;
      return { data };
    } catch (e) {
      if (e instanceof ErpError) {
        const { status, payload } = (() => {
          const p = publicErpFailure(e);
          return { status: e.status >= 500 ? 502 : e.status, payload: p };
        })();
        return reply.status(status).send(payload);
      }
      throw e;
    }
  });

  /** Self-service: update whitelisted contact fields on the caller's Employee row. */
  app.patch("/v1/me/employee", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const rawBody = (req.body ?? {}) as Record<string, unknown>;
    const patch: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(rawBody)) {
      if (!EMPLOYEE_PATCH_WHITELIST.has(k)) continue;
      if (v === null || v === undefined) continue;
      patch[k] = typeof v === "string" ? v.trim() : v;
    }
    if (Object.keys(patch).length === 0) {
      return reply.status(400).send({ error: "No allowed fields to update" });
    }

    try {
      let rows: unknown[];
      try {
        rows = await erp.getList(ctx.creds, "Employee", {
          filters: [
            ["user_id", "=", ctx.userEmail],
            ["company", "=", ctx.company],
          ],
          fields: ["name"],
          limit_page_length: 1,
        });
      } catch (first) {
        if (first instanceof ErpError && first.status >= 500) {
          const res = await erp.listDocs(ctx.creds, "Employee", {
            filters: [
              ["user_id", "=", ctx.userEmail],
              ["company", "=", ctx.company],
            ],
            fields: ["name"],
            limit_page_length: 1,
          });
          rows = res.data ?? [];
        } else {
          throw first;
        }
      }

      const row = rows[0];
      if (!row || typeof row !== "object") {
        return reply.status(404).send({
          error: "No employee record for your account in this company.",
          code: "HR_NO_EMPLOYEE",
        });
      }
      const name = String((row as Record<string, unknown>).name ?? "");
      if (!name) {
        return reply.status(404).send({ error: "Employee record has no id" });
      }

      const updated = await erp.updateDoc(ctx.creds, "Employee", name, patch);
      const { company: _drop, ...data } = updated as Record<string, unknown>;
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Roster counts for dashboard cards (bounded scan). */
  app.get("/v1/employees/summary", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    try {
      const res = await erp.listDocs(ctx.creds, "Employee", {
        filters: [["company", "=", ctx.company]],
        fields: ["name", "status"],
        order_by: "employee_name asc",
        limit_page_length: SUMMARY_SCAN_CAP,
      });
      const rows = res.data ?? [];
      let total = 0;
      let active = 0;
      let inactive = 0;
      let left = 0;
      for (const r of rows) {
        const rec = asRecord(r);
        if (!rec) continue;
        total++;
        const st = normalizeStatus(rec.status);
        if (st === "active") active++;
        else if (st === "left") left++;
        else inactive++;
      }
      return {
        data: {
          total,
          active,
          inactive,
          left,
          scan_capped: rows.length >= SUMMARY_SCAN_CAP,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: create Employee in the resolved company (minimal fields). */
  app.post("/v1/employees", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to create employee records" });
    }

    const rawBody = (req.body ?? {}) as Record<string, unknown>;
    const doc: Record<string, unknown> = {
      company: ctx.company,
      status: "Active",
    };
    for (const [k, v] of Object.entries(rawBody)) {
      if (!EMPLOYEE_CREATE_FIELDS.has(k)) continue;
      if (v === null || v === undefined) continue;
      doc[k] = typeof v === "string" ? v.trim() : v;
    }

    const first = String(doc.first_name ?? "").trim();
    const last = String(doc.last_name ?? "").trim();
    if (!first || !last) {
      return reply.status(400).send({ error: "first_name and last_name are required" });
    }
    doc.first_name = first;
    doc.last_name = last;
    doc.employee_name = `${first} ${last}`.trim();

    const doj = String(doc.date_of_joining ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(doj)) {
      doc.date_of_joining = new Date().toISOString().slice(0, 10);
    }

    try {
      const created = await erp.createDoc(ctx.creds, "Employee", doc);
      const { company: _drop, ...data } = created as Record<string, unknown>;
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR: issue a self-onboarding link (Supabase). */
  app.post("/v1/employee-invites", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to send onboarding invites" });
    }
    if (!invitesAvailable()) {
      return reply.status(503).send({ error: "Employee invites are not configured (Supabase) on the HR service" });
    }

    const body = (req.body ?? {}) as { email?: string; expires_in_days?: number };
    const email = String(body.email ?? "").trim().toLowerCase();
    if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      return reply.status(400).send({ error: "Valid email is required" });
    }
    const expires_in_days =
      typeof body.expires_in_days === "number" && Number.isFinite(body.expires_in_days)
        ? body.expires_in_days
        : 14;

    const out = await insertEmployeeInvite({
      email,
      company_key: ctx.company,
      invited_by_email: ctx.userEmail,
      expires_in_days,
    });
    if (!out) {
      return reply.status(500).send({ error: "Could not create invite" });
    }

    return {
      data: {
        token: out.token,
        expires_at: out.expires_at,
        onboard_path: `/employee-onboard?token=${encodeURIComponent(out.token)}`,
      },
    };
  });

  /** Paginated company roster (read-only); optional `q` searches name, id, department, designation. */
  app.get("/v1/employees", async (req, reply) => {
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
      const q = parseSearchQuery(req);
      const esc = q.replace(/\\/g, "\\\\").replace(/%/g, "\\%");
      const like = esc ? `%${esc}%` : "";

      let filters: unknown[] = [["company", "=", ctx.company]];
      if (like) {
        filters = [
          ["company", "=", ctx.company],
          [
            "or",
            [
              ["employee_name", "like", like],
              ["name", "like", like],
              ["department", "like", like],
              ["designation", "like", like],
              ["user_id", "like", like],
            ],
          ],
        ];
      }

      let res;
      try {
        res = await erp.listDocs(ctx.creds, "Employee", {
          filters,
          fields: EMPLOYEE_LIST_FIELDS,
          order_by: "employee_name asc",
          limit_start: limitStart,
          limit_page_length: take,
        });
      } catch (first) {
        if (first instanceof ErpError && like) {
          res = await erp.listDocs(ctx.creds, "Employee", {
            filters: [
              ["company", "=", ctx.company],
              ["employee_name", "like", like],
            ],
            fields: EMPLOYEE_LIST_FIELDS,
            order_by: "employee_name asc",
            limit_start: limitStart,
            limit_page_length: take,
          });
        } else {
          throw first;
        }
      }
      const raw = res.data ?? [];
      const hasMore = raw.length > pageSize;
      const data = hasMore ? raw.slice(0, pageSize) : raw;
      return {
        data,
        meta: { page, page_size: pageSize, has_more: hasMore, q: q || undefined },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Expense claim aggregates for one employee (ERPNext), lightweight counts + sums. */
  app.get("/v1/employees/:id/expense-snapshot", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    const gate = await ensureEmployeeInsightAccess(ctx, name);
    if (!gate.ok) return reply.status(gate.status).send({ error: gate.error });

    try {
      const res = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters: [
          ["company", "=", ctx.company],
          ["employee", "=", name],
        ],
        fields: [
          "docstatus",
          "approval_status",
          "grand_total",
          "total_claimed_amount",
          "total_amount_reimbursed",
        ],
        order_by: "modified desc",
        limit_page_length: EMPLOYEE_INSIGHTS_CLAIM_CAP,
      });
      const rows = res.data ?? [];
      let total = 0;
      let pending = 0;
      let approved = 0;
      let total_claimed = 0;
      let paid_out = 0;
      for (const r of rows) {
        const rec = asRecord(r);
        if (!rec) continue;
        total++;
        const docstatus = Number(rec.docstatus);
        const appr = String(rec.approval_status ?? "").trim().toLowerCase();
        if (docstatus === 0) pending++;
        else if (docstatus === 1 && appr !== "approved" && appr !== "rejected") pending++;
        if (appr === "approved") approved++;
        const gt =
          Number(rec.grand_total ?? rec.total_claimed_amount ?? 0) ||
          Number(rec.total_claimed_amount ?? 0) ||
          0;
        total_claimed += gt;
        paid_out += Number(rec.total_amount_reimbursed ?? 0) || 0;
      }
      return {
        data: {
          total,
          pending,
          approved,
          total_claimed,
          paid_out,
          scan_capped: rows.length >= EMPLOYEE_INSIGHTS_CLAIM_CAP,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Daily claim counts for activity heatmap (posting_date preferred, else modified). */
  app.get("/v1/employees/:id/claim-activity", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    const gate = await ensureEmployeeInsightAccess(ctx, name);
    if (!gate.ok) return reply.status(gate.status).send({ error: gate.error });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const months = Math.min(24, Math.max(1, parseInt(String(q.months ?? "6"), 10) || 6));
    const since = new Date();
    since.setMonth(since.getMonth() - months);
    const cutoff = since.toISOString().slice(0, 10);

    try {
      const res = await erp.listDocs(ctx.creds, "Expense Claim", {
        filters: [
          ["company", "=", ctx.company],
          ["employee", "=", name],
        ],
        fields: ["posting_date", "modified"],
        order_by: "modified desc",
        limit_page_length: EMPLOYEE_INSIGHTS_CLAIM_CAP,
      });
      const cells: Record<string, number> = {};
      for (const r of res.data ?? []) {
        const rec = asRecord(r);
        if (!rec) continue;
        const raw = String(rec.posting_date ?? rec.modified ?? "").trim();
        const day = raw.slice(0, 10);
        if (!/^\d{4}-\d{2}-\d{2}$/.test(day) || day < cutoff) continue;
        cells[day] = (cells[day] ?? 0) + 1;
      }
      return {
        data: {
          cells,
          months,
          scan_capped: (res.data ?? []).length >= EMPLOYEE_INSIGHTS_CLAIM_CAP,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Related record counts (ERPNext); optional doctypes return -1 when unavailable. */
  app.get("/v1/employees/:id/connections", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    const gate = await ensureEmployeeInsightAccess(ctx, name);
    if (!gate.ok) return reply.status(gate.status).send({ error: gate.error });

    async function cappedCount(doctype: string, filters: unknown[]): Promise<{ n: number; capped: boolean }> {
      try {
        const res = await erp.listDocs(ctx.creds, doctype, {
          filters,
          fields: ["name"],
          limit_page_length: CONNECTION_COUNT_CAP + 1,
        });
        const len = (res.data ?? []).length;
        return { n: Math.min(len, CONNECTION_COUNT_CAP), capped: len > CONNECTION_COUNT_CAP };
      } catch {
        return { n: -1, capped: false };
      }
    }

    try {
      const empDoc = (await erp.getDoc(ctx.creds, "Employee", name)) as Record<string, unknown>;
      const claims = await cappedCount("Expense Claim", [
        ["company", "=", ctx.company],
        ["employee", "=", name],
      ]);
      const advances = await cappedCount("Employee Advance", [
        ["company", "=", ctx.company],
        ["employee", "=", name],
      ]);
      const leaveApps = await cappedCount("Leave Application", [
        ["company", "=", ctx.company],
        ["employee", "=", name],
      ]);
      const leaveAllocs = await cappedCount("Leave Allocation", [
        ["company", "=", ctx.company],
        ["employee", "=", name],
      ]);
      const left = normalizeStatus(empDoc.status) === "left" ? 1 : 0;

      return {
        data: {
          expense_claims: claims.n,
          expense_claims_capped: claims.capped,
          employee_advances: advances.n,
          employee_advances_capped: advances.capped,
          leave_applications: leaveApps.n,
          leave_applications_capped: leaveApps.capped,
          leave_allocations: leaveAllocs.n,
          leave_allocations_capped: leaveAllocs.capped,
          /** 1 if employee status is Left in ERP (no separate “exit request” row in this integration). */
          separation_indicator: left,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Single employee (detail) — same company; non-HR may only read their own Employee id. */
  app.get("/v1/employees/:id", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const name = (req.params as { id: string }).id;
    const access = await loadEmployeeReadableByCaller(ctx, name);
    if (!access.ok) {
      return reply.status(access.status).send({ error: access.error });
    }
    const { company: _drop, ...rest } = access.doc;
    return { data: rest };
  });

  /** HR admin: update whitelisted fields on an employee in the same company. */
  app.patch("/v1/employees/:id", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to edit employee records" });
    }

    const name = (req.params as { id: string }).id;
    const rawBody = (req.body ?? {}) as Record<string, unknown>;
    const patch: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(rawBody)) {
      if (!EMPLOYEE_PATCH_WHITELIST.has(k)) continue;
      if (v === null || v === undefined) continue;
      patch[k] = typeof v === "string" ? v.trim() : v;
    }
    if (Object.keys(patch).length === 0) {
      return reply.status(400).send({ error: "No allowed fields to update" });
    }

    try {
      const cur = (await erp.getDoc(ctx.creds, "Employee", name)) as Record<string, unknown>;
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Employee not in your Company" });
      }
      const updated = await erp.updateDoc(ctx.creds, "Employee", name, patch);
      const { company: _drop, ...data } = updated as Record<string, unknown>;
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** HR admin: mark employee as exited (Left) with relieving date — standard separation path in ERPNext. */
  app.post("/v1/employees/:id/exit", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to process employee exit" });
    }

    const name = (req.params as { id: string }).id;
    const body = (req.body ?? {}) as { relieving_date?: string; reason_for_leaving?: string };
    const relievingRaw = typeof body.relieving_date === "string" ? body.relieving_date.trim() : "";
    const relieving_date =
      /^\d{4}-\d{2}-\d{2}$/.test(relievingRaw) ? relievingRaw : new Date().toISOString().slice(0, 10);
    const reason =
      typeof body.reason_for_leaving === "string" ? body.reason_for_leaving.trim().slice(0, 2000) : "";

    try {
      const cur = (await erp.getDoc(ctx.creds, "Employee", name)) as Record<string, unknown>;
      if (String(cur.company) !== ctx.company) {
        return reply.status(403).send({ error: "Employee not in your Company" });
      }
      if (normalizeStatus(cur.status) === "left") {
        return reply.status(409).send({ error: "Employee is already marked as Left" });
      }

      const exitPatch: Record<string, unknown> = {
        status: "Left",
        relieving_date,
      };
      if (reason) exitPatch.reason_for_leaving = reason;

      let updated: Record<string, unknown>;
      try {
        updated = (await erp.updateDoc(ctx.creds, "Employee", name, exitPatch)) as Record<string, unknown>;
      } catch (first) {
        if (first instanceof ErpError && reason) {
          updated = (await erp.updateDoc(ctx.creds, "Employee", name, {
            status: "Left",
            relieving_date,
          })) as Record<string, unknown>;
        } else {
          throw first;
        }
      }
      const { company: _drop, ...data } = updated;
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
