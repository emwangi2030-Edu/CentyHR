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
      // Best-effort UX: when ERPNext is temporarily unavailable, keep the UI stable.
      const st = e && typeof (e as any).status === "number" ? (e as any).status : undefined;
      if (st != null && st >= 500) {
        return { data: [] };
      }
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

      let rows: Record<string, unknown>[] = [];
      try {
        rows = (await erp.getList(ctx.creds, "Employee Checkin", {
          fields: ["name", "employee", "time", "log_type", "device_id", "shift", "skip_auto_attendance"],
          filters,
          order_by: "time desc",
          limit_page_length: 500,
        })) as Record<string, unknown>[];
      } catch (e) {
        const st = e && typeof (e as any).status === "number" ? (e as any).status : undefined;
        if (st != null && st >= 500) rows = [];
        else throw e;
      }

      // If ERP has no check-ins yet (e.g. Shift Assignment submit is skipped due to ERP timing issues),
      // synthesize IN/OUT events from Shift Assignments + Shift Type timings.
      if (!rows || rows.length === 0) {
        const fromDay = String(q.from_datetime ?? q.from ?? "").slice(0, 10);
        const toDay = String(q.to_datetime ?? q.to ?? "").slice(0, 10);

        const canBound = /^\d{4}-\d{2}-\d{2}$/.test(fromDay) && /^\d{4}-\d{2}-\d{2}$/.test(toDay);
        const dayStart = canBound ? new Date(fromDay + "T00:00:00.000Z") : null;
        const dayEnd = canBound ? new Date(toDay + "T23:59:59.999Z") : null;

        let shiftAssignments: any[] = [];
        try {
          shiftAssignments = (await erp.getList(ctx.creds, "Shift Assignment", {
            fields: ["name", "shift_type", "start_date", "end_date", "docstatus"],
            filters: [
              ["company", "=", ctx.company],
              ["employee", "=", employeeId],
              ["docstatus", "!=", 2],
            ],
            limit_page_length: 500,
          })) as any[];
        } catch (e) {
          const st = e && typeof (e as any).status === "number" ? (e as any).status : undefined;
          if (st != null && st >= 500) shiftAssignments = [];
          else throw e;
        }

        const shiftTypeNames = Array.from(new Set(shiftAssignments.map((s) => String(s.shift_type ?? "")).filter(Boolean)));
        let shiftTypes: any[] = [];
        if (shiftTypeNames.length) {
          try {
            shiftTypes = (await erp.getList(ctx.creds, "Shift Type", {
              fields: ["name", "start_time", "end_time"],
              filters: [["name", "IN", shiftTypeNames]],
              limit_page_length: 200,
            })) as any[];
          } catch (e) {
            const st = e && typeof (e as any).status === "number" ? (e as any).status : undefined;
            if (st != null && st >= 500) shiftTypes = [];
            else throw e;
          }
        }
        const stByName = new Map<string, { start_time: string; end_time: string }>();
        for (const st of shiftTypes) {
          stByName.set(String(st.name), { start_time: String(st.start_time ?? ""), end_time: String(st.end_time ?? "") });
        }

        function parseTimeHHMMSS(t: string): { hh: number; mm: number; ss: number } | null {
          const s = String(t ?? "").trim();
          const m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
          if (!m) return null;
          return { hh: Number(m[1]), mm: Number(m[2]), ss: Number(m[3] ?? "00") };
        }

        function combine(dateIso: string, time: { hh: number; mm: number; ss: number }): string {
          // Return something human-friendly (UI just displays it).
          const dd = dateIso;
          const hh = String(time.hh).padStart(2, "0");
          const mm = String(time.mm).padStart(2, "0");
          const ss = String(time.ss).padStart(2, "0");
          return `${dd} ${hh}:${mm}:${ss}`;
        }

        function withinRange(dtIso: string): boolean {
          if (!dayStart || !dayEnd) return true;
          // dtIso is "YYYY-MM-DD HH:MM:SS" — treat as UTC.
          const d = new Date(dtIso.replace(" ", "T") + "Z");
          return d >= dayStart && d <= dayEnd;
        }

        const out: Record<string, unknown>[] = [];
        const seen = new Set<string>();

        // Requirement: only synthesize the shift that is "active" for each date.
        // When multiple Shift Assignments overlap, we choose the one with the latest start_date.
        function isActiveOnDate(sa: any, dd: string): boolean {
          const saStart = String(sa.start_date ?? "").slice(0, 10);
          const saEndRaw = sa.end_date == null ? "" : String(sa.end_date).slice(0, 10);
          if (!saStart || !/^\d{4}-\d{2}-\d{2}$/.test(saStart)) return false;
          const saEnd = saEndRaw && /^\d{4}-\d{2}-\d{2}$/.test(saEndRaw) ? saEndRaw : saStart;
          return dd >= saStart && dd <= saEnd;
        }

        function pickBestAssignmentForDate(dd: string): any | null {
          const active = shiftAssignments.filter((sa) => {
            const shiftType = String(sa.shift_type ?? "");
            return shiftType && stByName.has(shiftType) && isActiveOnDate(sa, dd);
          });
          if (active.length === 0) return null;

          // Sort by latest start_date, then latest end_date, then name (deterministic).
          active.sort((a, b) => {
            const aStart = String(a.start_date ?? "").slice(0, 10);
            const bStart = String(b.start_date ?? "").slice(0, 10);
            if (aStart !== bStart) return bStart.localeCompare(aStart);

            const aEndRaw = a.end_date == null ? "" : String(a.end_date).slice(0, 10);
            const bEndRaw = b.end_date == null ? "" : String(b.end_date).slice(0, 10);
            const aEnd = aEndRaw && /^\d{4}-\d{2}-\d{2}$/.test(aEndRaw) ? aEndRaw : aStart;
            const bEnd = bEndRaw && /^\d{4}-\d{2}-\d{2}$/.test(bEndRaw) ? bEndRaw : bStart;
            if (aEnd !== bEnd) return bEnd.localeCompare(aEnd);

            return String(b.name ?? "").localeCompare(String(a.name ?? ""));
          });
          return active[0];
        }

        const dayStrings: string[] = [];
        if (canBound && dayStart && dayEnd) {
          const startDOnly = new Date(fromDay + "T00:00:00.000Z");
          const endDOnly = new Date(toDay + "T00:00:00.000Z");
          for (let d = new Date(startDOnly); d <= endDOnly; d = new Date(d.getTime() + 24 * 3600 * 1000)) {
            dayStrings.push(d.toISOString().slice(0, 10));
          }
        }

        if (dayStrings.length > 0) {
          for (const dd of dayStrings) {
            const best = pickBestAssignmentForDate(dd);
            if (!best) continue;

            const shiftType = String(best.shift_type ?? "");
            const timings = stByName.get(shiftType);
            if (!timings) continue;

            const startT = parseTimeHHMMSS(timings.start_time);
            const endT = parseTimeHHMMSS(timings.end_time);
            if (!startT || !endT) continue;

            const inTime = combine(dd, startT);

            const startSeconds = startT.hh * 3600 + startT.mm * 60 + startT.ss;
            const endSeconds = endT.hh * 3600 + endT.mm * 60 + endT.ss;
            const overnight = endSeconds < startSeconds;
            const outDateIso = overnight ? new Date(new Date(dd + "T00:00:00.000Z").getTime() + 24 * 3600 * 1000).toISOString().slice(0, 10) : dd;
            const outTime = combine(outDateIso, endT);

            if (withinRange(inTime)) {
              const key = `${employeeId}|${inTime}|IN|${shiftType}`;
              if (!seen.has(key)) {
                seen.add(key);
                out.push({
                  name: `scheduled-checkin-${employeeId}-${dd}-${shiftType}-IN`.replace(/[^a-zA-Z0-9_-]/g, "_"),
                  employee: employeeId,
                  time: inTime,
                  log_type: "IN",
                  device_id: null,
                  shift: shiftType,
                });
              }
            }

            if (withinRange(outTime)) {
              const key = `${employeeId}|${outTime}|OUT|${shiftType}`;
              if (!seen.has(key)) {
                seen.add(key);
                out.push({
                  name: `scheduled-checkin-${employeeId}-${outDateIso}-${shiftType}-OUT`.replace(/[^a-zA-Z0-9_-]/g, "_"),
                  employee: employeeId,
                  time: outTime,
                  log_type: "OUT",
                  device_id: null,
                  shift: shiftType,
                });
              }
            }
          }
        } else {
          // If query is not bounded by valid dates, fall back to the previous
          // "generate from all overlapping assignments" behavior.
          for (const sa of shiftAssignments) {
            const saStart = String(sa.start_date ?? "").slice(0, 10);
            const saEndRaw = sa.end_date == null ? "" : String(sa.end_date).slice(0, 10);
            if (!saStart || !/^\d{4}-\d{2}-\d{2}$/.test(saStart)) continue;
            const saEnd = saEndRaw && /^\d{4}-\d{2}-\d{2}$/.test(saEndRaw) ? saEndRaw : saStart;

            const shiftType = String(sa.shift_type ?? "");
            const timings = stByName.get(shiftType);
            if (!timings) continue;

            const startT = parseTimeHHMMSS(timings.start_time);
            const endT = parseTimeHHMMSS(timings.end_time);
            if (!startT || !endT) continue;

            const startSeconds = startT.hh * 3600 + startT.mm * 60 + startT.ss;
            const endSeconds = endT.hh * 3600 + endT.mm * 60 + endT.ss;
            const overnight = endSeconds < startSeconds;

            for (let d = new Date(saStart + "T00:00:00.000Z"); d <= new Date(saEnd + "T00:00:00.000Z"); d = new Date(d.getTime() + 24 * 3600 * 1000)) {
              const dd = d.toISOString().slice(0, 10);
              const inTime = combine(dd, startT);
              const outDateIso = overnight ? new Date(d.getTime() + 24 * 3600 * 1000).toISOString().slice(0, 10) : dd;
              const outTime = combine(outDateIso, endT);

              if (withinRange(inTime)) {
                const key = `${employeeId}|${inTime}|IN|${shiftType}`;
                if (!seen.has(key)) {
                  seen.add(key);
                  out.push({
                    name: `scheduled-checkin-${employeeId}-${dd}-${shiftType}-IN`.replace(/[^a-zA-Z0-9_-]/g, "_"),
                    employee: employeeId,
                    time: inTime,
                    log_type: "IN",
                    device_id: null,
                    shift: shiftType,
                  });
                }
              }

              if (withinRange(outTime)) {
                const key = `${employeeId}|${outTime}|OUT|${shiftType}`;
                if (!seen.has(key)) {
                  seen.add(key);
                  out.push({
                    name: `scheduled-checkin-${employeeId}-${outDateIso}-${shiftType}-OUT`.replace(/[^a-zA-Z0-9_-]/g, "_"),
                    employee: employeeId,
                    time: outTime,
                    log_type: "OUT",
                    device_id: null,
                    shift: shiftType,
                  });
                }
              }
            }
          }
        }

        out.sort((a, b) => String((b as any).time ?? "").localeCompare(String((a as any).time ?? "")));
        return { data: out };
      }

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

      let attendanceRows: Record<string, unknown>[] = [];
      try {
        // 1) Real attendance rows.
        attendanceRows = (await erp.getList(ctx.creds, "Attendance", {
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
        })) as Record<string, unknown>[];
      } catch (e) {
        const st = e && typeof (e as any).status === "number" ? (e as any).status : undefined;
        if (st != null && st >= 500) {
          attendanceRows = [];
        } else {
          throw e;
        }
      }

      // 2) Fallback: if ERP attendance generation is delayed/unavailable,
      // populate the Daily tab from Shift Assignments (including drafts).
      // This keeps the UI responsive even when Shift Assignment submit fails.
      let shiftRows: Record<string, unknown>[] = [];
      try {
        // 2) Fallback: if ERP attendance generation is delayed/unavailable,
        // populate the Daily tab from Shift Assignments (including drafts).
        shiftRows = (await erp.getList(ctx.creds, "Shift Assignment", {
          fields: ["name", "shift_type", "start_date", "end_date", "docstatus"],
          filters: [
            ["company", "=", ctx.company],
            ["employee", "=", employeeId],
            ["docstatus", "!=", 2],
          ],
          // Use server-side bounds loosely; we further bound dates client-side.
          order_by: "start_date asc",
          limit_page_length: 400,
        })) as Record<string, unknown>[];
      } catch (e) {
        const st = e && typeof (e as any).status === "number" ? (e as any).status : undefined;
        if (st != null && st >= 500) {
          shiftRows = [];
        } else {
          throw e;
        }
      }

      const byDate = new Map<string, Record<string, unknown>>();
      for (const r of attendanceRows as Record<string, unknown>[]) {
        const d = String((r as any).attendance_date ?? "").slice(0, 10);
        if (!d) continue;
        byDate.set(d, r);
      }

      function isoToDate(iso: string): Date {
        return new Date(iso + "T00:00:00.000Z");
      }
      function dateToIso(d: Date): string {
        return d.toISOString().slice(0, 10);
      }

      const fromBound = from ?? "";
      const toBound = to ?? fromBound;
      const canBound = fromBound && toBound && /^\d{4}-\d{2}-\d{2}$/.test(fromBound) && /^\d{4}-\d{2}-\d{2}$/.test(toBound);

      if (canBound) {
        const startD = isoToDate(fromBound);
        const endD = isoToDate(toBound);
        const iterStart = startD <= endD ? startD : endD;
        const iterEnd = startD <= endD ? endD : startD;

      for (const sa of shiftRows as any[]) {
          const saStart = String(sa.start_date ?? "").slice(0, 10);
          if (!/^\d{4}-\d{2}-\d{2}$/.test(saStart)) continue;
          const saEndRaw = sa.end_date == null ? "" : String(sa.end_date).slice(0, 10);
          const saEnd = saEndRaw && /^\d{4}-\d{2}-\d{2}$/.test(saEndRaw) ? saEndRaw : toBound;
          if (!/^\d{4}-\d{2}-\d{2}$/.test(saEnd)) continue;

          const shiftType = String(sa.shift_type ?? "");
          if (!shiftType) continue;

          const saStartD = isoToDate(saStart);
          const saEndD = isoToDate(saEnd);

          let d = iterStart > saStartD ? iterStart : saStartD;
          const last = iterEnd < saEndD ? iterEnd : saEndD;

          while (d <= last) {
            const dd = dateToIso(d);
            if (!byDate.has(dd)) {
              byDate.set(dd, {
                name: `scheduled-${employeeId}-${dd}-${shiftType}`.replace(/[^a-zA-Z0-9_-]/g, "_"),
                employee: employeeId,
                attendance_date: dd,
                status: "Scheduled",
                shift: shiftType,
                in_time: null,
                out_time: null,
                working_hours: null,
                late_entry: null,
                early_exit: null,
              });
            }
            d = new Date(d.getTime() + 24 * 3600 * 1000);
          }
        }
      }

      const merged = Array.from(byDate.values()).sort((a, b) => {
        const da = String((a as any).attendance_date ?? "");
        const db = String((b as any).attendance_date ?? "");
        return db.localeCompare(da);
      });

      return { data: merged };
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

