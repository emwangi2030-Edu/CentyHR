/**
 * Time & Attendance (ERPNext HRMS) — read-only routes for Pay Hub.
 *
 * ERPNext remains the source of truth. This BFF only enforces:
 * - tenant scoping (Company)
 * - access rules (self vs HR/admin via bridge `canHr`)
 *
 * Doctypes used (may vary by site/apps; capabilities should hide UI if unavailable):
 * - Shift Type
 * - Shift Assignment
 * - Employee Checkin
 * - Attendance
 */
import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import type { HrContext } from "../types.js";
import { defaultClient } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError): FastifyReply {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
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

function parseDate(v: unknown): string {
  const s = String(v ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

function parseDateTime(v: unknown): string {
  const s = String(v ?? "").trim();
  // Accept ISO date-time; ERP stores e.g. "2026-03-25 09:00:00" or ISO strings.
  if (!s) return "";
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s;
  return "";
}

export const attendanceRoutes: FastifyPluginAsync = async (app) => {
  /**
   * Shift types (read-only metadata).
   * Intended for showing a configuration summary; not all sites expose the same fields.
   */
  app.get("/v1/attendance/shift-types", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const rows = await erp.getList(ctx.creds, "Shift Type", {
        fields: ["name", "start_time", "end_time", "enable_auto_attendance", "working_hours_calculation_based_on"],
        filters: [],
        order_by: "modified desc",
        limit_page_length: 200,
      });
      const data = rows.map((r) => asRecord(r)).filter(Boolean);
      return { data };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Shift assignments.
   * - Employees: self only
   * - HR/admin: may pass `?employee=EMP-ID`
   */
  app.get("/v1/attendance/shift-assignments", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDate(q.from);
    const to = parseDate(q.to);
    try {
      let employeeId: string;
      if (ctx.canSubmitOnBehalf) {
        if (!qEmp) return reply.status(400).send({ error: "employee query parameter is required for team shift view" });
        const empDoc = await erp.getDoc(ctx.creds, "Employee", qEmp);
        if (String(empDoc.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your Company" });
        employeeId = qEmp;
      } else {
        const selfId = await resolveSelfEmployee(ctx);
        if (!selfId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        if (qEmp && qEmp !== selfId) return reply.status(403).send({ error: "You may only view your own shifts" });
        employeeId = selfId;
      }

      const filters: unknown[] = [["company", "=", ctx.company], ["employee", "=", employeeId], ["docstatus", "!=", 2]];
      if (from) filters.push(["start_date", ">=", from]);
      if (to) filters.push(["end_date", "<=", to]);

      const rows = await erp.getList(ctx.creds, "Shift Assignment", {
        fields: ["name", "employee", "employee_name", "shift_type", "start_date", "end_date", "status", "docstatus"],
        filters,
        order_by: "start_date desc",
        limit_page_length: 200,
      });
      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Raw check-ins (IN/OUT logs).
   */
  app.get("/v1/attendance/checkins", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDateTime(q.from_datetime ?? q.from);
    const to = parseDateTime(q.to_datetime ?? q.to);
    try {
      let employeeId: string;
      if (ctx.canSubmitOnBehalf) {
        if (!qEmp) return reply.status(400).send({ error: "employee query parameter is required for team checkins view" });
        const empDoc = await erp.getDoc(ctx.creds, "Employee", qEmp);
        if (String(empDoc.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your Company" });
        employeeId = qEmp;
      } else {
        const selfId = await resolveSelfEmployee(ctx);
        if (!selfId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        if (qEmp && qEmp !== selfId) return reply.status(403).send({ error: "You may only view your own check-ins" });
        employeeId = selfId;
      }

      const filters: unknown[] = [["employee", "=", employeeId]];
      if (from) filters.push(["time", ">=", from]);
      if (to) filters.push(["time", "<=", to]);

      const rows = await erp.getList(ctx.creds, "Employee Checkin", {
        fields: ["name", "employee", "time", "log_type", "device_id", "shift", "skip_auto_attendance"],
        filters,
        order_by: "time desc",
        limit_page_length: 500,
      });
      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Daily attendance rollups.
   */
  app.get("/v1/attendance/daily", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    try {
      let employeeId: string;
      if (ctx.canSubmitOnBehalf) {
        if (!qEmp) return reply.status(400).send({ error: "employee query parameter is required for team attendance view" });
        const empDoc = await erp.getDoc(ctx.creds, "Employee", qEmp);
        if (String(empDoc.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your Company" });
        employeeId = qEmp;
      } else {
        const selfId = await resolveSelfEmployee(ctx);
        if (!selfId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });
        if (qEmp && qEmp !== selfId) return reply.status(403).send({ error: "You may only view your own attendance" });
        employeeId = selfId;
      }

      const filters: unknown[] = [["employee", "=", employeeId]];
      if (from) filters.push(["attendance_date", ">=", from]);
      if (to) filters.push(["attendance_date", "<=", to]);

      const rows = await erp.getList(ctx.creds, "Attendance", {
        fields: [
          "name",
          "employee",
          "employee_name",
          "attendance_date",
          "status",
          "shift",
          "in_time",
          "out_time",
          "working_hours",
          "late_entry",
          "early_exit",
        ],
        filters,
        order_by: "attendance_date desc",
        limit_page_length: 200,
      });
      return { data: rows };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};

