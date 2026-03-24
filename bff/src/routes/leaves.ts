/**
 * Leave Application API (ERPNext HR). Approve/reject via `status` after submit;
 * extend for Workflow-only sites if needed.
 */
import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import type { HrContext } from "../types.js";
import { defaultClient } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

type GateOk = { ok: true };
type GateFail = { ok: false; status: number; error: string };
type Gate = GateOk | GateFail;

function replyErp(reply: FastifyReply, e: ErpError): FastifyReply {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

function normalizeStatus(v: unknown): string {
  return String(v ?? "").trim().toLowerCase();
}

function parsePageParams(req: FastifyRequest): { page: number; pageSize: number; limitStart: number } {
  const q = (req.query ?? {}) as Record<string, unknown>;
  const page = Math.max(1, parseInt(String(q.page ?? "1"), 10) || 1);
  const raw = parseInt(String(q.page_size ?? "25"), 10) || 25;
  const pageSize = Math.min(100, Math.max(10, raw));
  return { page, pageSize, limitStart: (page - 1) * pageSize };
}

export function leaveUiStatus(doc: Record<string, unknown>): string {
  const ds = Number(doc.docstatus);
  const st = normalizeStatus(doc.status);
  if (ds === 0) return "draft";
  if (ds === 2) return "cancelled";
  if (st === "approved") return "approved";
  if (st === "rejected") return "rejected";
  if (ds === 1) return "pending";
  return "pending";
}

function pendingFilters(): [string, string, number | string | string[]][] {
  return [
    ["docstatus", "=", 1],
    ["status", "not in", ["Approved", "Rejected", "Cancelled"]],
  ];
}

function buildListFilters(
  ctx: HrContext,
  qStatus: string,
  qEmployee: string
): [string, string, string | number | string[]][] {
  const base: [string, string, string | number | string[]][] = [["company", "=", ctx.company]];
  const st = normalizeStatus(qStatus);
  if (qEmployee) base.push(["employee", "=", qEmployee]);
  if (st === "draft") base.push(["docstatus", "=", 0]);
  else if (st === "pending") {
    for (const f of pendingFilters()) base.push(f);
  } else if (st === "approved") {
    base.push(["docstatus", "=", 1], ["status", "=", "Approved"]);
  } else if (st === "rejected") base.push(["status", "=", "Rejected"]);
  else if (st === "cancelled") base.push(["docstatus", "=", 2]);
  return base;
}

async function resolveSelfEmployee(ctx: HrContext): Promise<string | null> {
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

async function ensureLeaveReadAccess(ctx: HrContext, doc: Record<string, unknown>): Promise<Gate> {
  if (String(doc.company) !== ctx.company) {
    return { ok: false, status: 403, error: "Leave application not in your Company" };
  }
  if (ctx.canSubmitOnBehalf) return { ok: true };
  const me = ctx.userEmail.trim().toLowerCase();
  const approver = String(doc.leave_approver ?? "").trim().toLowerCase();
  if (approver === me) return { ok: true };
  const selfId = await resolveSelfEmployee(ctx);
  if (selfId && String(doc.employee) === String(selfId)) return { ok: true };
  return {
    ok: false,
    status: 403,
    error: "You may only access leave applications you applied for or must approve",
  };
}

function ensureLeaveCompany(ctx: HrContext, doc: Record<string, unknown>): Gate {
  if (String(doc.company) !== ctx.company) {
    return { ok: false, status: 403, error: "Leave application not in your Company" };
  }
  return { ok: true };
}

type ApproveLeaveResult = { ok: true } | GateFail;

async function approveLeaveOnce(ctx: HrContext, name: string): Promise<ApproveLeaveResult> {
  const cur = await erp.getDoc(ctx.creds, "Leave Application", name);
  const g = ensureLeaveCompany(ctx, cur);
  if (!g.ok) return g;
  if (Number(cur.docstatus) !== 1) {
    return { ok: false, status: 409, error: "Only submitted applications can be approved" };
  }
  const st = normalizeStatus(cur.status);
  if (st === "approved" || st === "rejected") {
    return { ok: false, status: 409, error: "Application already finalised" };
  }
  const approver = String(cur.leave_approver ?? "").trim().toLowerCase();
  const me = ctx.userEmail.trim().toLowerCase();
  if (approver !== me && !ctx.canSubmitOnBehalf) {
    return {
      ok: false,
      status: 403,
      error: "Only the assigned leave approver or HR-privileged user can approve",
    };
  }
  await erp.callMethod(ctx.creds, "frappe.client.set_value", {
    doctype: "Leave Application",
    name,
    fieldname: "status",
    value: "Approved",
  });
  return { ok: true };
}

const PATCH_WHITELIST = new Set([
  "leave_type",
  "from_date",
  "to_date",
  "half_day",
  "half_day_date",
  "description",
]);

export const leaveRoutes: FastifyPluginAsync = async (app) => {
  app.get("/v1/meta/leave-types", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const rows = await erp.getList(ctx.creds, "Leave Type", {
        fields: ["name", "max_leaves_allowed", "is_lwp"],
        filters: [],
        limit_page_length: 200,
      });
      const data = rows
        .map((r) => asRecord(r))
        .filter(Boolean)
        .map((r) => ({
          name: String(r!.name ?? ""),
          max_leaves_allowed: r!.max_leaves_allowed ?? null,
          is_lwp: !!r!.is_lwp,
        }))
        .filter((r) => r.name);
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** Leave Allocation rows (balances) — employees see only self; HR must pass `?employee=`. */
  app.get("/v1/leave-balances", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const qEmp = String((req.query as { employee?: string })?.employee ?? "").trim();
    try {
      let employeeId: string;
      if (ctx.canSubmitOnBehalf) {
        if (!qEmp) {
          return reply
            .status(400)
            .send({ error: "employee query parameter is required for HR leave balance lookup" });
        }
        const empDoc = await erp.getDoc(ctx.creds, "Employee", qEmp);
        if (String(empDoc.company) !== ctx.company) {
          return reply.status(403).send({ error: "Employee not in your Company" });
        }
        employeeId = qEmp;
      } else {
        const selfId = await resolveSelfEmployee(ctx);
        if (!selfId) {
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        }
        if (qEmp && qEmp !== selfId) {
          return reply.status(403).send({ error: "You may only view your own leave balances" });
        }
        employeeId = selfId;
      }
      const rows = await erp.getList(ctx.creds, "Leave Allocation", {
        filters: [
          ["company", "=", ctx.company],
          ["employee", "=", employeeId],
          ["docstatus", "!=", 2],
        ],
        fields: [
          "name",
          "leave_type",
          "from_date",
          "to_date",
          "new_leaves_allocated",
          "total_leaves_allocated",
          "carry_forward",
          "docstatus",
        ],
        order_by: "modified desc",
        limit_page_length: 100,
      });
      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/leave-applications/summary", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const CAP = 500;
    try {
      const filters: [string, string, string | number | string[]][] = [
        ["company", "=", ctx.company],
        ...pendingFilters(),
      ];
      let or_filters: [string, string, string][] | undefined;
      if (!ctx.canSubmitOnBehalf) {
        const selfId = await resolveSelfEmployee(ctx);
        if (!selfId)
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        or_filters = [
          ["employee", "=", selfId],
          ["leave_approver", "=", ctx.userEmail],
        ];
      }
      const rows = await erp.getList(ctx.creds, "Leave Application", {
        filters,
        or_filters,
        fields: ["name"],
        limit_page_length: CAP + 1,
      });
      const len = rows.length;
      return {
        data: {
          pending: Math.min(len, CAP),
          pending_capped: len > CAP,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/leave-applications", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const qEmp = String((req.query as { employee?: string })?.employee ?? "").trim();
    const qStatus = String((req.query as { status?: string })?.status ?? "all").trim();
    try {
      let or_filters: [string, string, string][] | undefined;
      let hrEmployeeFilter = "";
      if (!ctx.canSubmitOnBehalf) {
        const selfId = await resolveSelfEmployee(ctx);
        if (!selfId)
          return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        if (qEmp && qEmp !== selfId) {
          return reply.status(403).send({ error: "Cannot filter by another employee" });
        }
        or_filters = [
          ["employee", "=", selfId],
          ["leave_approver", "=", ctx.userEmail],
        ];
      } else if (qEmp) {
        const other = await erp.getDoc(ctx.creds, "Employee", qEmp);
        if (String(other.company) !== ctx.company) {
          return reply.status(403).send({ error: "Employee not in your Company" });
        }
        hrEmployeeFilter = qEmp;
      }
      const filters = buildListFilters(ctx, qStatus === "all" ? "" : qStatus, hrEmployeeFilter);
      const { page, pageSize, limitStart } = parsePageParams(req);
      const take = pageSize + 1;
      const rowObjs = await erp.getList(ctx.creds, "Leave Application", {
        filters,
        or_filters,
        fields: [
          "name",
          "employee",
          "employee_name",
          "department",
          "leave_type",
          "from_date",
          "to_date",
          "half_day",
          "total_leave_days",
          "status",
          "docstatus",
          "leave_approver",
          "posting_date",
          "creation",
          "description",
          "company",
        ],
        order_by: "modified desc",
        limit_start: limitStart,
        limit_page_length: take,
      });
      const rows = rowObjs.map(asRecord).filter(Boolean) as Record<string, unknown>[];
      const slice = rows.slice(0, pageSize);
      const hasMore = rows.length > pageSize;
      const data = slice.map((r) => ({
        ...r,
        ui_status: leaveUiStatus(r),
      }));
      return {
        data,
        meta: { page, page_size: pageSize, has_more: hasMore },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/leave-applications/:id", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const name = (req.params as { id: string }).id;
    try {
      const doc = await erp.getDoc(ctx.creds, "Leave Application", name);
      const gate = await ensureLeaveReadAccess(ctx, doc);
      if (!gate.ok) return reply.status(gate.status).send({ error: gate.error });
      return { data: { ...doc, ui_status: leaveUiStatus(doc) } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/leave-applications", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const body = (req.body ?? {}) as Record<string, unknown>;
    try {
      const selfId = await resolveSelfEmployee(ctx);
      if (!selfId) {
        return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
      }
      let employee = selfId;
      if (body.employee != null && String(body.employee).trim() !== "") {
        if (!ctx.canSubmitOnBehalf) {
          return reply.status(403).send({ error: "Only HR may apply on behalf of another employee" });
        }
        employee = String(body.employee).trim();
        const empDoc = await erp.getDoc(ctx.creds, "Employee", employee);
        if (String(empDoc.company) !== ctx.company) {
          return reply.status(403).send({ error: "Employee is not in your Company" });
        }
      }
      const leaveType = typeof body.leave_type === "string" ? body.leave_type.trim() : "";
      const fromDate = typeof body.from_date === "string" ? body.from_date.trim() : "";
      const toDate = typeof body.to_date === "string" ? body.to_date.trim() : "";
      if (!leaveType || !fromDate || !toDate) {
        return reply.status(400).send({ error: "leave_type, from_date, and to_date are required" });
      }
      const postingDate =
        typeof body.posting_date === "string" && body.posting_date.trim()
          ? body.posting_date.trim()
          : new Date().toISOString().slice(0, 10);
      const halfDay = body.half_day === true || body.half_day === 1 || body.half_day === "1" ? 1 : 0;
      const halfDayDate = typeof body.half_day_date === "string" ? body.half_day_date.trim() : "";
      const description = typeof body.description === "string" ? body.description : "";
      const empFull = await erp.getDoc(ctx.creds, "Employee", employee);
      const leaveApprover = empFull.leave_approver != null ? String(empFull.leave_approver) : "";
      const doc: Record<string, unknown> = {
        doctype: "Leave Application",
        company: ctx.company,
        employee,
        leave_type: leaveType,
        from_date: fromDate,
        to_date: toDate,
        half_day: halfDay,
        ...(halfDay && halfDayDate ? { half_day_date: halfDayDate } : {}),
        description,
        posting_date: postingDate,
        ...(leaveApprover ? { leave_approver: leaveApprover } : {}),
      };
      const created = await erp.createDoc(ctx.creds, "Leave Application", doc);
      return { data: { ...created, ui_status: leaveUiStatus(created) } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.patch("/v1/leave-applications/:id", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const name = (req.params as { id: string }).id;
    const body = (req.body ?? {}) as Record<string, unknown>;
    try {
      const cur = await erp.getDoc(ctx.creds, "Leave Application", name);
      const gate = await ensureLeaveReadAccess(ctx, cur);
      if (!gate.ok) return reply.status(gate.status).send({ error: gate.error });
      if (Number(cur.docstatus) !== 0) {
        return reply.status(409).send({ error: "Only draft applications can be edited" });
      }
      const patch: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(body)) {
        if (!PATCH_WHITELIST.has(k)) continue;
        if (k === "half_day") {
          patch[k] = v === true || v === 1 || v === "1" ? 1 : 0;
        } else if (typeof v === "string" || typeof v === "number" || v === null) {
          patch[k] = v;
        }
      }
      if (Object.keys(patch).length === 0) {
        return reply.status(400).send({ error: "No editable fields supplied" });
      }
      const updated = await erp.updateDoc(ctx.creds, "Leave Application", name, patch);
      return { data: { ...updated, ui_status: leaveUiStatus(updated) } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/leave-applications/:id/submit", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const name = (req.params as { id: string }).id;
    try {
      const cur = await erp.getDoc(ctx.creds, "Leave Application", name);
      const gate = await ensureLeaveReadAccess(ctx, cur);
      if (!gate.ok) return reply.status(gate.status).send({ error: gate.error });
      if (Number(cur.docstatus) !== 0) {
        return reply.status(409).send({ error: "Only draft applications can be submitted" });
      }
      const result = await erp.submitDoc(ctx.creds, "Leave Application", name);
      return { data: result };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/leave-applications/:id/approve", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const name = (req.params as { id: string }).id;
    try {
      const result = await approveLeaveOnce(ctx, name);
      if (!result.ok) return reply.status(result.status).send({ error: result.error });
      return { ok: true };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/leave-applications/:id/reject", async (req, reply) => {
    let ctx: HrContext;
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
      const cur = await erp.getDoc(ctx.creds, "Leave Application", name);
      const g0 = ensureLeaveCompany(ctx, cur);
      if (!g0.ok) return reply.status(g0.status).send({ error: g0.error });
      if (Number(cur.docstatus) !== 1) {
        return reply.status(409).send({ error: "Only submitted applications can be rejected" });
      }
      const st = normalizeStatus(cur.status);
      if (st === "approved" || st === "rejected") {
        return reply.status(409).send({ error: "Application already finalised" });
      }
      const approver = String(cur.leave_approver ?? "").trim().toLowerCase();
      const me = ctx.userEmail.trim().toLowerCase();
      if (approver !== me && !ctx.canSubmitOnBehalf) {
        return reply
          .status(403)
          .send({ error: "Only the assigned leave approver or HR-privileged user can reject" });
      }
      await erp.callMethod(ctx.creds, "frappe.client.set_value", {
        doctype: "Leave Application",
        name,
        fieldname: "status",
        value: "Rejected",
      });
      if (reason) {
        try {
          await erp.callMethod(ctx.creds, "frappe.client.set_value", {
            doctype: "Leave Application",
            name,
            fieldname: "description",
            value: `${String(cur.description ?? "").trim()}\n[Rejected: ${reason}]`.trim(),
          });
        } catch {
          /* ignore */
        }
      }
      return { ok: true };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
