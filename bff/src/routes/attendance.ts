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

function parseFrappeDateTime(s: string): Date | null {
  const str = String(s ?? "").trim();
  if (!str) return null;
  // Frappe typically stores as `YYYY-MM-DD HH:MM:SS` (timezone-naive). Treat as UTC.
  if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(str)) {
    return new Date(str.replace(" ", "T") + "Z");
  }
  // Allow ISO strings too.
  const d = new Date(str);
  return Number.isNaN(d.getTime()) ? null : d;
}

function toFrappeDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function toFrappeDateTime(d: Date): string {
  // ISO `YYYY-MM-DDTHH:MM:SS.sssZ` -> `YYYY-MM-DD HH:MM:SS`
  return d.toISOString().slice(0, 19).replace("T", " ");
}

function parseTimeHHMMSS(t: string): { hh: number; mm: number; ss: number } | null {
  const s = String(t ?? "").trim();
  const m = s.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;
  return { hh: Number(m[1]), mm: Number(m[2]), ss: Number(m[3] ?? "00") };
}

function timeToSeconds(t: { hh: number; mm: number; ss: number }): number {
  return t.hh * 3600 + t.mm * 60 + t.ss;
}

function shiftWindowToMs(params: {
  shift_start_date: string;
  start_time: string;
  end_time: string;
}): { startMs: number; endMs: number } | null {
  const { shift_start_date, start_time, end_time } = params;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(shift_start_date ?? ""))) return null;
  const startT = parseTimeHHMMSS(start_time);
  const endT = parseTimeHHMMSS(end_time);
  if (!startT || !endT) return null;

  const base = new Date(shift_start_date + "T00:00:00.000Z");
  const baseMs = base.getTime();
  const startSeconds = timeToSeconds(startT);
  const endSeconds = timeToSeconds(endT);
  const startMs = baseMs + startSeconds * 1000;

  // Overnight shift: end happens next day.
  const endDayMs = endSeconds < startSeconds ? baseMs + 24 * 3600 * 1000 : baseMs;
  const endMs = endDayMs + endSeconds * 1000;

  return { startMs, endMs };
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

  /**
   * Employee clock in/out (wire Timesheets into Time & Attendance).
   *
   * Rules:
   * - Attendance is created for the shift-start calendar day.
   * - Time inside the scheduled shift window -> Regular/Night activity.
   * - Time outside the scheduled shift window -> Overtime activity.
   * - One Timesheet per employee per day: append time_logs to existing Draft.
   * - is_billable is always 0 for payroll-only time logs.
   */

  async function resolveEmployeeIdForRequest(ctx: HrContext, qEmp: string): Promise<string | null> {
    if (ctx.canSubmitOnBehalf) {
      if (!qEmp) return null;
      const empDoc = await erp.getDoc(ctx.creds, "Employee", qEmp);
      if (String(empDoc.company) !== ctx.company) return null;
      return qEmp;
    }
    return await resolveSelfEmployee(ctx);
  }

  async function resolveActiveShiftForTimestamp(params: {
    ctx: HrContext;
    employeeId: string;
    at: Date;
  }): Promise<{
    shift_assignment_name: string;
    shift_type_name: string;
    shift_start_date: string; // shift-start calendar day (YYYY-MM-DD)
    shift_window: { startMs: number; endMs: number };
  }> {
    const { ctx, employeeId, at } = params;
    const atMs = at.getTime();
    const ddNow = toFrappeDate(at);
    const ddPrev = toFrappeDate(new Date(atMs - 24 * 3600 * 1000));

    const assignments = (await erp.getList(ctx.creds, "Shift Assignment", {
      fields: ["name", "shift_type", "start_date", "end_date", "docstatus"],
      filters: [
        ["company", "=", ctx.company],
        ["employee", "=", employeeId],
        ["docstatus", "!=", 2],
        ["start_date", ">=", ddPrev],
        ["start_date", "<=", ddNow],
      ],
      order_by: "start_date desc",
      limit_page_length: 50,
    })) as any[];

    const shiftTypeNames = Array.from(new Set(assignments.map((s) => String(s.shift_type ?? "")).filter(Boolean)));
    const shiftTypes = shiftTypeNames.length
      ? ((await erp.getList(ctx.creds, "Shift Type", {
          fields: ["name", "start_time", "end_time"],
          filters: [["name", "IN", shiftTypeNames]],
          limit_page_length: 200,
        })) as any[])
      : [];
    const stByName = new Map<string, { start_time: string; end_time: string }>();
    for (const st of shiftTypes) {
      stByName.set(String(st.name), { start_time: String(st.start_time ?? ""), end_time: String(st.end_time ?? "") });
    }

    // 1) Prefer assignments whose shift window covers the timestamp.
    for (const sa of assignments) {
      const shiftStartDate = String(sa.start_date ?? "").slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(shiftStartDate)) continue;
      const shiftTypeName = String(sa.shift_type ?? "");
      const timings = stByName.get(shiftTypeName);
      if (!timings) continue;

      const win = shiftWindowToMs({
        shift_start_date: shiftStartDate,
        start_time: timings.start_time,
        end_time: timings.end_time,
      });
      if (!win) continue;
      if (atMs >= win.startMs && atMs <= win.endMs) {
        return {
          shift_assignment_name: String(sa.name),
          shift_type_name: shiftTypeName,
          shift_start_date: shiftStartDate,
          shift_window: win,
        };
      }
    }

    // 2) Fallback: pick latest assignment active on ddNow.
    for (const sa of assignments) {
      const shiftStartDate = String(sa.start_date ?? "").slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(shiftStartDate)) continue;
      const shiftTypeName = String(sa.shift_type ?? "");
      const timings = stByName.get(shiftTypeName);
      if (!timings) continue;

      const saEndRaw = sa.end_date == null ? "" : String(sa.end_date).slice(0, 10);
      const saEnd = saEndRaw && /^\d{4}-\d{2}-\d{2}$/.test(saEndRaw) ? saEndRaw : shiftStartDate;
      if (ddNow >= shiftStartDate && ddNow <= saEnd) {
        const win = shiftWindowToMs({
          shift_start_date: shiftStartDate,
          start_time: timings.start_time,
          end_time: timings.end_time,
        });
        if (!win) continue;
        return {
          shift_assignment_name: String(sa.name),
          shift_type_name: shiftTypeName,
          shift_start_date: shiftStartDate,
          shift_window: win,
        };
      }
    }

    throw new ErpError("No active shift assignment found for this timestamp", 404, {
      error: "No active shift assignment found",
    });
  }

  async function ensureAttendancePresent(params: {
    ctx: HrContext;
    employeeId: string;
    attendance_date: string; // shift-start calendar day
    shift_type_name: string;
  }): Promise<string> {
    const { ctx, employeeId, attendance_date, shift_type_name } = params;
    const existing = (await erp.getList(ctx.creds, "Attendance", {
      fields: ["name", "docstatus"],
      filters: [
        ["employee", "=", employeeId],
        ["attendance_date", "=", attendance_date],
      ],
      limit_page_length: 1,
    })) as any[];
    if (existing.length) {
      const name = String(existing[0].name);
      await erp.updateDoc(ctx.creds, "Attendance", name, {
        status: "Present",
        shift: shift_type_name,
      });
      return name;
    }

    const created = await erp.createDoc(ctx.creds, "Attendance", {
      employee: employeeId,
      company: ctx.company,
      attendance_date,
      status: "Present",
      shift: shift_type_name,
    });
    return String((created as any)?.name ?? "");
  }

  async function resolveActivityTypeMap(creds: HrContext["creds"]): Promise<{
    regular: string | null;
    night: string | null;
    overtime: string | null;
  }> {
    const required = ["Regular Hours", "Night Shift", "Overtime"];
    const rows = (await erp.getList(creds, "Activity Type", {
      fields: ["name"],
      filters: [["name", "IN", required]],
      limit_page_length: 20,
    })) as any[];
    const set = new Set(rows.map((r) => String(r.name)));
    return {
      regular: set.has("Regular Hours") ? "Regular Hours" : null,
      night: set.has("Night Shift") ? "Night Shift" : null,
      overtime: set.has("Overtime") ? "Overtime" : null,
    };
  }

  async function getOrCreateDraftTimesheet(params: {
    ctx: HrContext;
    employeeId: string;
    shift_start_date: string; // YYYY-MM-DD
    project?: string | null;
    shift_location?: unknown;
    time_logsToAppend: Record<string, unknown>[];
  }): Promise<string> {
    const { ctx, employeeId, shift_start_date, project, shift_location, time_logsToAppend } = params;

    const existing = (await erp.getList(ctx.creds, "Timesheet", {
      fields: ["name"],
      filters: [
        ["employee", "=", employeeId],
        ["company", "=", ctx.company],
        ["start_date", "=", shift_start_date],
        ["status", "=", "Draft"],
        ["docstatus", "!=", 2],
      ],
      order_by: "modified desc",
      limit_page_length: 1,
    })) as any[];

    if (existing.length) {
      const name = String(existing[0].name);
      const doc = await erp.getDoc(ctx.creds, "Timesheet", name);
      const existingRows = Array.isArray((doc as any).time_logs) ? ((doc as any).time_logs as Record<string, unknown>[]) : [];
      const next = [...existingRows, ...time_logsToAppend];

      try {
        await erp.updateDoc(ctx.creds, "Timesheet", name, {
          ...(project ? { project } : {}),
          ...(shift_location !== undefined ? { shift_location } : {}),
          time_logs: next,
        });
      } catch (e) {
        const msg = e instanceof ErpError ? String((e as any).body ?? (e as any).message ?? "") : String(e);
        const mentionsShiftLocation = msg.toLowerCase().includes("shift_location") || msg.toLowerCase().includes("shift location");
        if (mentionsShiftLocation) {
          await erp.updateDoc(ctx.creds, "Timesheet", name, {
            ...(project ? { project } : {}),
            time_logs: next,
          });
        } else {
          throw e;
        }
      }
      return name;
    }

    const baseDoc: Record<string, unknown> = {
      employee: employeeId,
      company: ctx.company,
      start_date: shift_start_date,
      end_date: shift_start_date,
      time_logs: time_logsToAppend,
      ...(project ? { project } : {}),
    };

    try {
      if (shift_location !== undefined) (baseDoc as any).shift_location = shift_location;
      const created = await erp.createDoc(ctx.creds, "Timesheet", baseDoc);
      return String((created as any)?.name ?? "");
    } catch (e) {
      const msg = e instanceof ErpError ? String((e as any).body ?? (e as any).message ?? "") : String(e);
      const mentionsShiftLocation = msg.toLowerCase().includes("shift_location") || msg.toLowerCase().includes("shift location");
      if (mentionsShiftLocation) {
        delete (baseDoc as any).shift_location;
        const created = await erp.createDoc(ctx.creds, "Timesheet", baseDoc);
        return String((created as any)?.name ?? "");
      }
      throw e;
    }
  }

  app.post("/v1/attendance/clock-in", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const employeeId = await resolveEmployeeIdForRequest(ctx, qEmp);
    if (!employeeId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });

    try {
      const now = new Date();
      const active = await resolveActiveShiftForTimestamp({ ctx, employeeId, at: now });

      const attendanceName = await ensureAttendancePresent({
        ctx,
        employeeId,
        attendance_date: active.shift_start_date,
        shift_type_name: active.shift_type_name,
      });

      const shiftAssignmentDoc = await erp.getDoc(ctx.creds, "Shift Assignment", active.shift_assignment_name);

      return {
        data: {
          from_time: toFrappeDateTime(now),
          attendance: attendanceName,
          attendance_date: active.shift_start_date,
          shift_assignment: active.shift_assignment_name,
          shift_type: active.shift_type_name,
          project: (shiftAssignmentDoc as any)?.project ?? null,
          shift_location: (shiftAssignmentDoc as any)?.shift_location ?? null,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * HR-only helper: ensure required Activity Types exist for time log creation.
   * This keeps the clock-in/out flow working even if ERPNext is missing config.
   */
  app.post("/v1/attendance/seed-activity-types", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "Only HR can seed activity types." });

    const activityTypes = [
      { activity_type: "Regular Hours", costing_rate: 0, billing_rate: 0 },
      { activity_type: "Overtime", costing_rate: 0, billing_rate: 0 },
      { activity_type: "Night Shift", costing_rate: 0, billing_rate: 0 },
      { activity_type: "Public Holiday", costing_rate: 0, billing_rate: 0 },
    ];

    try {
      const names = activityTypes.map((a) => a.activity_type);
      const existing = (await erp.getList(ctx.creds, "Activity Type", {
        fields: ["name"],
        filters: [["name", "IN", names]],
        limit_page_length: 50,
      })) as any[];

      const existingNames = new Set(existing.map((r) => String(r.name)));
      const created: string[] = [];

      for (const at of activityTypes) {
        if (existingNames.has(at.activity_type)) continue;
        const createdDoc = await erp.createDoc(ctx.creds, "Activity Type", at);
        const nm = String((createdDoc as any)?.name ?? at.activity_type);
        created.push(nm);
      }

      return { data: { created, alreadyExisted: activityTypes.length - created.length } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/attendance/clock-out", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const employeeId = await resolveEmployeeIdForRequest(ctx, qEmp);
    if (!employeeId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });

    const from_time = String(body.from_time ?? "").trim();
    const to_time = String(body.to_time ?? "").trim();
    const shift_assignment_name = String(body.shift_assignment_name ?? "").trim();
    if (!from_time || !shift_assignment_name) {
      return reply.status(400).send({ error: "from_time and shift_assignment_name are required" });
    }

    try {
      const from = parseFrappeDateTime(from_time);
      const to = to_time ? parseFrappeDateTime(to_time) : new Date();
      if (!from || !to) return reply.status(400).send({ error: "Invalid from_time/to_time format" });
      const fromMs = from.getTime();
      const toMs = to.getTime();
      if (toMs <= fromMs) return reply.status(400).send({ error: "to_time must be after from_time" });

      const shiftAssignmentDoc = await erp.getDoc(ctx.creds, "Shift Assignment", shift_assignment_name);
      const shiftStartDate = String(shiftAssignmentDoc?.start_date ?? "").slice(0, 10);
      const shiftTypeName = String(shiftAssignmentDoc?.shift_type ?? "");
      if (!/^\d{4}-\d{2}-\d{2}$/.test(shiftStartDate) || !shiftTypeName) {
        return reply.status(400).send({ error: "Invalid shift assignment (missing start_date/shift_type)" });
      }

      const shiftTypeDoc = await erp.getDoc(ctx.creds, "Shift Type", shiftTypeName);
      const win = shiftWindowToMs({
        shift_start_date: shiftStartDate,
        start_time: String(shiftTypeDoc?.start_time ?? ""),
        end_time: String(shiftTypeDoc?.end_time ?? ""),
      });
      if (!win) return reply.status(400).send({ error: "Invalid shift type timing" });

      const regularStartMs = Math.max(fromMs, win.startMs);
      const regularEndMs = Math.min(toMs, win.endMs);
      const regularMs = Math.max(0, regularEndMs - regularStartMs);

      const overtimeEarlyMs = fromMs < win.startMs ? Math.max(0, Math.min(toMs, win.startMs) - fromMs) : 0;
      const overtimeLateMs = toMs > win.endMs ? Math.max(0, toMs - Math.max(fromMs, win.endMs)) : 0;

      const activityMap = await resolveActivityTypeMap(ctx.creds);
      if (!activityMap.overtime) return reply.status(400).send({ error: "Activity Type 'Overtime' is not configured in ERPNext." });

      const regularActivity =
        String(shiftTypeName ?? "").toLowerCase().includes("night") && activityMap.night ? activityMap.night : activityMap.regular;
      if (regularMs > 0 && !regularActivity) {
        return reply.status(400).send({ error: "Activity Type 'Regular Hours'/'Night Shift' is not configured in ERPNext." });
      }

      const time_logs: Record<string, unknown>[] = [];
      if (regularMs > 0 && regularActivity) {
        time_logs.push({
          activity_type: regularActivity,
          from_time: toFrappeDateTime(new Date(regularStartMs)),
          to_time: toFrappeDateTime(new Date(regularEndMs)),
          hours: Number((regularMs / 3600000).toFixed(2)),
          is_billable: 0,
        });
      }
      if (overtimeEarlyMs > 0) {
        time_logs.push({
          activity_type: activityMap.overtime,
          from_time: toFrappeDateTime(new Date(fromMs)),
          to_time: toFrappeDateTime(new Date(win.startMs)),
          hours: Number((overtimeEarlyMs / 3600000).toFixed(2)),
          is_billable: 0,
        });
      }
      if (overtimeLateMs > 0) {
        time_logs.push({
          activity_type: activityMap.overtime,
          from_time: toFrappeDateTime(new Date(win.endMs)),
          to_time: toFrappeDateTime(new Date(toMs)),
          hours: Number((overtimeLateMs / 3600000).toFixed(2)),
          is_billable: 0,
        });
      }

      if (time_logs.length === 0) {
        return reply.status(400).send({ error: "No time logs generated (clock times didn't overlap shift window)." });
      }

      const projectName = (shiftAssignmentDoc as any)?.project ?? null;
      const shift_location = (shiftAssignmentDoc as any)?.shift_location ?? null;
      const timesheetName = await getOrCreateDraftTimesheet({
        ctx,
        employeeId,
        shift_start_date: shiftStartDate,
        project: projectName,
        shift_location,
        time_logsToAppend: time_logs,
      });

      return {
        data: {
          timesheet: timesheetName,
          attendance_date: shiftStartDate,
          shift_assignment: shift_assignment_name,
          regular_hours: Number((regularMs / 3600000).toFixed(2)),
          overtime_hours: Number(((overtimeEarlyMs + overtimeLateMs) / 3600000).toFixed(2)),
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Manager time logs (Timesheet drafts + submitted).
   *
   * Notes:
   * - We keep this thin: list Timesheets, then compute total hours from `time_logs`.
   * - UI uses `status` + `docstatus` to decide whether submit is allowed.
   */
  app.get("/v1/attendance/time-logs", async (req, reply) => {
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
    const status = String(q.status ?? "").trim();

    try {
      const employeeId = await resolveEmployeeIdForRequest(ctx, qEmp);
      if (!employeeId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });

      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["employee", "=", employeeId],
        ["docstatus", "!=", 2],
      ];

      // Timesheets are typically per-day, but support overlaps.
      if (from && to) {
        filters.push(["start_date", "<=", to]);
        filters.push(["end_date", ">=", from]);
      } else if (from) {
        filters.push(["end_date", ">=", from]);
      } else if (to) {
        filters.push(["start_date", "<=", to]);
      }

      if (status) filters.push(["status", "=", status]);

      const rows = (await erp.getList(ctx.creds, "Timesheet", {
        fields: ["name", "employee", "employee_name", "start_date", "end_date", "status", "docstatus"],
        filters,
        order_by: "start_date desc",
        limit_page_length: 100,
      })) as any[];

      const out = [];
      for (const r of rows) {
        const doc = await erp.getDoc(ctx.creds, "Timesheet", String(r.name));
        const logs = Array.isArray((doc as any).time_logs) ? ((doc as any).time_logs as any[]) : [];
        const totalHours = logs.reduce((acc, l) => acc + Number(l?.hours ?? 0), 0);
        const activityTypes = Array.from(new Set(logs.map((l) => String(l?.activity_type ?? "")).filter(Boolean)));
        const activity_type = activityTypes.length === 1 ? activityTypes[0] : activityTypes.length > 1 ? "Mixed" : null;

        out.push({
          name: String(r.name),
          employee: r.employee,
          employee_name: r.employee_name,
          start_date: r.start_date,
          end_date: r.end_date,
          status: r.status,
          docstatus: r.docstatus,
          activity_type,
          total_hours: Number(totalHours.toFixed(2)),
        });
      }

      return { data: out };
    } catch (e) {
      const st = e && typeof (e as any).status === "number" ? (e as any).status : undefined;
      if (st != null && st >= 500) return { data: [] };
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Submit a draft timesheet so payroll/Sallary Slip can read it.
   */
  app.post("/v1/attendance/time-logs/:name/submit", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "Only HR can submit timesheets." });

    const params = (req.params ?? {}) as Record<string, unknown>;
    const name = String(params.name ?? "").trim();
    if (!name) return reply.status(400).send({ error: "Timesheet name is required" });

    try {
      const doc = await erp.getDoc(ctx.creds, "Timesheet", name);
      if (Number((doc as any).docstatus ?? 0) === 1) {
        return { data: { name, submitted: false, alreadySubmitted: true } };
      }
      await erp.submitDoc(ctx.creds, "Timesheet", name);
      return { data: { name, submitted: true } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};

