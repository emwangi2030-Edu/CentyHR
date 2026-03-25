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

function parseBodyString(v: unknown): string {
  return String(v ?? "").trim();
}

function parseBoolish(v: unknown): boolean | null {
  if (v === true || v === 1 || v === "1") return true;
  if (v === false || v === 0 || v === "0") return false;
  if (typeof v === "string") {
    const s = v.trim().toLowerCase();
    if (s === "true" || s === "yes") return true;
    if (s === "false" || s === "no") return false;
  }
  return null;
}

function normalizeTime(v: unknown): string {
  const s = parseBodyString(v);
  // Allow "HH:MM" and "HH:MM:SS"
  if (/^\d{2}:\d{2}:\d{2}$/.test(s)) return s;
  if (/^\d{2}:\d{2}$/.test(s)) return `${s}:00`;
  return "";
}

export const attendanceRoutes: FastifyPluginAsync = async (app) => {
  async function submitShiftAssignmentWithRetry(
    creds: HrContext["creds"],
    name: string,
    maxAttempts = 3
  ): Promise<unknown> {
    let lastErr: unknown = null;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        // Ensure docstatus hasn't already changed between create and submit attempts.
        try {
          const cur = await erp.getDoc(creds, "Shift Assignment", name);
          if (Number(cur.docstatus) === 1) return { alreadySubmitted: true };
        } catch {
          /* ignore refresh failures; submit attempt will surface the issue */
        }

        // Small delay reduces likelihood of optimistic-lock clashes with ERP background hooks.
        if (attempt > 1) {
          await new Promise((r) => setTimeout(r, 1000 * attempt));
        }
        return await erp.submitDoc(creds, "Shift Assignment", name);
      } catch (e) {
        lastErr = e;
        if (e instanceof ErpError && e.status === 417) {
          // Frappe returns TimestampMismatchError when doc changes between open/save.
          const b = e.body as any;
          const excType = b?.exc_type ? String(b.exc_type) : "";
          const raw = typeof b === "string" ? b : e.message;
          if (excType.includes("TimestampMismatchError") || String(raw).includes("TimestampMismatchError")) {
            // retry
            continue;
          }
        }
        throw e;
      }
    }
    throw lastErr;
  }

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
   * Phase 3a: HR creates Shift Types (master data).
   *
   * This prevents the tenant from needing to manually configure ERP behind the scenes.
   */
  app.post("/v1/attendance/shift-types", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can create shift types." });
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const name = parseBodyString(body.name);
    const start_time = normalizeTime(body.start_time);
    const end_time = normalizeTime(body.end_time);
    const working_hours_calculation_based_on = parseBodyString(body.working_hours_calculation_based_on);
    const enable_auto_attendance = parseBoolish(body.enable_auto_attendance);

    if (!name || !start_time || !end_time) {
      return reply.status(400).send({
        error: "name, start_time (HH:MM or HH:MM:SS), and end_time are required",
      });
    }

    try {
      const doc: Record<string, unknown> = {
        name,
        start_time,
        end_time,
        ...(working_hours_calculation_based_on ? { working_hours_calculation_based_on } : {}),
        ...(enable_auto_attendance == null ? {} : { enable_auto_attendance: enable_auto_attendance ? 1 : 0 }),
      };

      // Idempotency: if the shift type already exists, return it (avoid upstream 409
      // and tracebacks leaking into the UI).
      const existingRows = await erp.getList(ctx.creds, "Shift Type", {
        fields: ["name", "start_time", "end_time", "enable_auto_attendance"],
        filters: [["name", "=", name]],
        limit_page_length: 1,
      });
      const existing = existingRows.map(asRecord).filter(Boolean)[0];
      if (existing) return { data: existing, meta: { alreadyExists: true } };

      const created = await erp.createDoc(ctx.creds, "Shift Type", doc);
      return { data: created };
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

  /**
   * Phase 3: HR creates shift assignments (write route).
   *
   * Request:
   * - employee: ERP employee name (required)
   * - shift_type: Shift Type name (required)
   * - start_date: YYYY-MM-DD (required)
   * - end_date: YYYY-MM-DD (optional)
   */
  app.post("/v1/attendance/shift-assignments", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can create shift assignments." });
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const employee = parseBodyString(body.employee);
    const shift_type = parseBodyString(body.shift_type);
    const start_date = parseBodyString(body.start_date);
    const end_date = parseBodyString(body.end_date);

    if (!employee || !shift_type || !start_date) {
      return reply.status(400).send({ error: "employee, shift_type, and start_date are required" });
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(start_date)) {
      return reply.status(400).send({ error: "start_date must be YYYY-MM-DD" });
    }
    if (end_date && !/^\d{4}-\d{2}-\d{2}$/.test(end_date)) {
      return reply.status(400).send({ error: "end_date must be YYYY-MM-DD when provided" });
    }

    try {
      const empDoc = await erp.getDoc(ctx.creds, "Employee", employee);
      if (String(empDoc.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your Company" });

      const doc: Record<string, unknown> = {
        employee,
        shift_type,
        start_date,
        ...(end_date ? { end_date } : {}),
      };

      const created = await erp.createDoc(ctx.creds, "Shift Assignment", doc);
      const name = parseBodyString((created as Record<string, unknown>)?.name);
      if (name) {
        try {
          await submitShiftAssignmentWithRetry(ctx.creds, name, 3);
        } catch (e) {
          // The Shift Assignment doctype in this tenant throws TimestampMismatchError
          // on API submit even when the doc was just created. Creating the draft
          // still writes the assignment row, which is what the Phase 3 UI needs.
          if (e instanceof ErpError && e.status === 417) {
            const b = e.body as any;
            const excType = b?.exc_type ? String(b.exc_type) : "";
            const raw = typeof b === "string" ? b : e.message;
            const isTimestampMismatch =
              excType.includes("TimestampMismatchError") || String(raw).includes("TimestampMismatchError");
            if (isTimestampMismatch) {
              return { data: created, meta: { submitSkipped: true } };
            }
          }
          throw e;
        }
      }
      return { data: created, meta: { submitSkipped: false } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};

