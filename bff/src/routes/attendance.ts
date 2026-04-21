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
import { parseFrappeErrorBody, publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const _dowFilename = fileURLToPath(import.meta.url);
const _dowDirname = dirname(_dowFilename);
const DOW_STORE_PATH = join(_dowDirname, "..", ".shift-assignment-days.json");

const _dowStore: Map<string, string> = (() => {
  try {
    if (existsSync(DOW_STORE_PATH)) {
      const raw = readFileSync(DOW_STORE_PATH, "utf8");
      return new Map(Object.entries(JSON.parse(raw) as Record<string, string>));
    }
  } catch { }
  return new Map();
})();

function dowGet(name: string): string { return _dowStore.get(name) ?? ""; }
function dowSet(name: string, value: string): void {
  if (value) { _dowStore.set(name, value); } else { _dowStore.delete(name); }
  try {
    const obj: Record<string, string> = {};
    _dowStore.forEach((v, k) => { obj[k] = v; });
    writeFileSync(DOW_STORE_PATH, JSON.stringify(obj, null, 2), "utf8");
  } catch { }
}

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError): FastifyReply {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

async function resolveSelfEmployee(ctx: HrContext): Promise<string | null> {
  // Try user_id first, then fall back to personal_email / prefered_email for employees
  // whose user_id was never populated (e.g. created before onboarding set the field).
  for (const field of ["user_id", "personal_email", "prefered_email"] as const) {
    const mine = await erp.listDocs(ctx.creds, "Employee", {
      filters: [[field, "=", ctx.userEmail], ["company", "=", ctx.company]],
      fields: ["name"],
      limit_page_length: 1,
    });
    const empName = asRecord(mine.data?.[0])?.name;
    if (typeof empName === "string" && empName) return empName;
  }
  return null;
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

/** Returns YYYY-MM-DD in the given timezone offset (minutes east of UTC). */
function toLocalDate(d: Date, tzOffsetMinutes: number): string {
  return new Date(d.getTime() + tzOffsetMinutes * 60_000).toISOString().slice(0, 10);
}

/** In-memory cache so we only fetch System Settings once per BFF process lifetime. */
const _tzOffsetCache = new Map<string, number>();

/**
 * Fetch the ERPNext site timezone offset in minutes (east of UTC).
 * Uses `System Settings.time_zone` and resolves via `Intl.DateTimeFormat`.
 * Falls back to 0 (UTC) on any error.
 */
async function getErpTzOffsetMinutes(creds: { apiKey: string; apiSecret: string }): Promise<number> {
  const cacheKey = `${creds.apiKey ?? ""}|${creds.apiSecret ?? ""}`;
  const cached = _tzOffsetCache.get(cacheKey);
  if (cached !== undefined) return cached;
  try {
    const doc = await erp.getDoc(creds, "System Settings", "System Settings");
    const tz = String((doc as any)?.time_zone ?? "").trim();
    if (!tz) {
      _tzOffsetCache.set(cacheKey, 0);
      return 0;
    }
    // Use Intl to resolve offset: format a known UTC epoch and measure local-time offset.
    const testDate = new Date("2024-01-15T12:00:00Z");
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: tz,
      hour: "numeric",
      minute: "numeric",
      hour12: false,
      timeZoneName: "shortOffset",
    }).formatToParts(testDate);
    const tzNamePart = parts.find((p) => p.type === "timeZoneName")?.value ?? "";
    // "GMT+3" or "GMT+5:30" etc.
    const m = tzNamePart.match(/GMT([+-])(\d{1,2})(?::(\d{2}))?/);
    if (!m) {
      _tzOffsetCache.set(cacheKey, 0);
      return 0;
    }
    const sign = m[1] === "+" ? 1 : -1;
    const hours = Number(m[2]);
    const minutes = Number(m[3] ?? "0");
    const offset = sign * (hours * 60 + minutes);
    _tzOffsetCache.set(cacheKey, offset);
    return offset;
  } catch {
    _tzOffsetCache.set(cacheKey, 0);
    return 0;
  }
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
  /** UTC offset of the ERPNext site in minutes (e.g. 180 for EAT/UTC+3). Default 0. */
  tzOffsetMinutes?: number;
}): { startMs: number; endMs: number } | null {
  const { shift_start_date, start_time, end_time, tzOffsetMinutes = 0 } = params;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(shift_start_date ?? ""))) return null;
  const startT = parseTimeHHMMSS(start_time);
  const endT = parseTimeHHMMSS(end_time);
  if (!startT || !endT) return null;

  // Anchor to LOCAL midnight: UTC midnight of the date string minus the tz offset.
  // e.g. for EAT (UTC+3): local midnight = UTC 21:00 previous day.
  const utcMidnightMs = new Date(shift_start_date + "T00:00:00.000Z").getTime();
  const baseMs = utcMidnightMs - tzOffsetMinutes * 60_000;
  const startSeconds = timeToSeconds(startT);
  const endSeconds = timeToSeconds(endT);
  const startMs = baseMs + startSeconds * 1000;

  // Overnight shift: end happens next day.
  const endDayMs = endSeconds < startSeconds ? baseMs + 24 * 3600 * 1000 : baseMs;
  const endMs = endDayMs + endSeconds * 1000;

  return { startMs, endMs };
}

function csvEscapeCell(v: string): string {
  const s = String(v ?? "");
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

/** Lowercased workflow labels treated as “no further action” for the approval queue. Override with TIMESHEET_WORKFLOW_TERMINAL_STATES (comma-separated). */
function timesheetWorkflowTerminalStates(): Set<string> {
  const fromEnv = String(process.env.TIMESHEET_WORKFLOW_TERMINAL_STATES ?? "")
    .split(",")
    .map((x) => x.trim().toLowerCase())
    .filter(Boolean);
  const set = new Set<string>(["approved", "rejected", "cancelled", "complete", "completed"]);
  for (const x of fromEnv) set.add(x);
  return set;
}

/**
 * Parse a conflicting timesheet name out of ERPNext's overlap validation message.
 * e.g. "Row 1: From Time and To Time of TS-2026-00005 is overlapping with TS-2026-00004"
 * → "TS-2026-00004"
 */
/** Extract a readable error string from an ErpError (body is a parsed JSON object, not a string). */
function erpMsg(e: unknown): string {
  if (e instanceof ErpError) return parseFrappeErrorBody(e.body) ?? e.message ?? String(e);
  return String(e);
}

function extractOverlapConflict(msg: string): string | null {
  const m = msg.match(/overlapping with\s+(TS-[\w-]+)/i);
  return m ? m[1].replace(/[^A-Za-z0-9-]/g, "") : null;
}

/**
 * Given a list of time log rows we want to write, remove any whose from_time–to_time
 * interval overlaps with an existing row in the specified timesheet.
 */
async function pruneLogsAgainstTimesheet(
  creds: Parameters<typeof erp.getDoc>[0],
  conflictingTsName: string,
  logsToWrite: Record<string, unknown>[]
): Promise<Record<string, unknown>[]> {
  let conflictDoc: unknown;
  try {
    conflictDoc = await erp.getDoc(creds, "Timesheet", conflictingTsName);
  } catch {
    return logsToWrite; // can't fetch — return unchanged, let ERPNext re-reject
  }
  const conflictLogs: unknown[] = Array.isArray((conflictDoc as any).time_logs)
    ? (conflictDoc as any).time_logs
    : [];
  const existingIntervals = conflictLogs
    .map((l: any) => ({
      from: new Date(String(l.from_time ?? "").replace(" ", "T")).getTime(),
      to: new Date(String(l.to_time ?? "").replace(" ", "T")).getTime(),
    }))
    .filter((i) => !Number.isNaN(i.from) && !Number.isNaN(i.to));

  return logsToWrite.filter((log) => {
    const from = new Date(String(log.from_time ?? "").replace(" ", "T")).getTime();
    const to = new Date(String(log.to_time ?? "").replace(" ", "T")).getTime();
    if (Number.isNaN(from) || Number.isNaN(to)) return true;
    return !existingIntervals.some((ex) => from < ex.to && to > ex.from);
  });
}

function isActivityOvertimeLabel(activityType: string): boolean {
  return String(activityType ?? "")
    .toLowerCase()
    .includes("overtime");
}

function timesheetWorkflowPending(workflowState: unknown, terminal: Set<string>): boolean {
  const s = String(workflowState ?? "").trim();
  if (!s) return false;
  return !terminal.has(s.toLowerCase());
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
   * Delete a Shift Type (master data).
   * HR-only. Blocked if any active (non-cancelled) Shift Assignments reference it.
   */
  app.delete("/v1/attendance/shift-types/:name", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can delete shift types." });
    }

    const { name } = (req.params ?? {}) as Record<string, string>;
    if (!name) return reply.status(400).send({ error: "Shift type name is required." });

    try {
      // Guard: reject if any active shift assignments reference this shift type.
      const activeAssignments = await erp.getList(ctx.creds, "Shift Assignment", {
        fields: ["name"],
        filters: [
          ["shift_type", "=", name],
          ["docstatus", "!=", 2],
        ],
        limit_page_length: 1,
      });
      if ((activeAssignments as unknown[]).length > 0) {
        return reply.status(409).send({
          error: `"${name}" is used by one or more active shift assignments. Unassign or remove those first.`,
          code: "HR_SHIFT_TYPE_IN_USE",
        });
      }

      await erp.deleteDoc(ctx.creds, "Shift Type", name);
      return { data: { deleted: name } };
    } catch (e) {
      if (e instanceof ErpError) {
        // ERPNext LinkValidationError means historical records (Attendance, Timesheets, etc.)
        // still reference this shift type. Surface a clear, actionable message instead of the
        // raw ERPNext traceback.
        const hint = parseFrappeErrorBody(e.body) ?? "";
        const isLinkError =
          e.status === 417 &&
          (hint.toLowerCase().includes("linkvalidation") ||
            hint.toLowerCase().includes("is linked with") ||
            hint.toLowerCase().includes("cannot delete or cancel"));
        if (isLinkError) {
          return reply.status(409).send({
            error: `"${name}" cannot be deleted because it has historical attendance or timesheet records linked to it. Shift types with recorded history are kept permanently to preserve data integrity.`,
            code: "HR_SHIFT_TYPE_HAS_HISTORY",
          });
        }
        return replyErp(reply, e);
      }
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
      // Overlap condition: shift overlaps [from, to] if start_date <= to AND (end_date >= from OR end_date is null/open-ended).
      // Using start_date <= to catches shifts that started before or on the range end.
      // We don't filter end_date here because Frappe can't express OR conditions easily in list filters;
      // instead we only apply start_date <= to and let open-ended shifts (null end_date) through naturally.
      if (to) filters.push(["start_date", "<=", to]);

      const rawRows = await erp.getList(ctx.creds, "Shift Assignment", {
        fields: ["name", "employee", "employee_name", "shift_type", "start_date", "end_date", "status", "docstatus"],
        filters,
        order_by: "start_date desc",
        limit_page_length: 200,
      });
      // Post-filter: exclude shifts whose end_date is set and falls before `from` (shift ended before the range).
      const filtered = from
        ? (rawRows as Record<string, unknown>[]).filter((r) => {
            const endDate = String(r.end_date ?? "").slice(0, 10);
            if (!endDate) return true;
            return endDate >= from;
          })
        : (rawRows as Record<string, unknown>[]);
      const rows = filtered.map((r) => ({ ...r, custom_days_of_week: dowGet(String(r.name ?? "")) }));
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

          // Check days-of-week restriction from local store.
          const saDowStr = dowGet(String(sa.name ?? ""));
          const DOW_NAMES_D = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"] as const;
          const saDow = saDowStr ? saDowStr.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean) : [];

          while (d <= last) {
            const dd = dateToIso(d);
            const dayName = DOW_NAMES_D[new Date(dd + "T00:00:00").getDay()];
            const isScheduledDay = saDow.length === 0 || saDow.includes(dayName);
            if (!byDate.has(dd)) {
              byDate.set(dd, {
                name: `scheduled-${employeeId}-${dd}-${shiftType}`.replace(/[^a-zA-Z0-9_-]/g, "_"),
                employee: employeeId,
                attendance_date: dd,
                status: isScheduledDay ? "Scheduled" : "Unscheduled",
                shift: isScheduledDay ? shiftType : null,
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

      // Augment each row with overtime_hours by querying Timesheet Detail records
      // (activity_type = "Overtime") for this employee in the date range.
      try {
        const tsFilters: unknown[] = [
          ["employee", "=", employeeId],
          ["company", "=", ctx.company],
          ["docstatus", "!=", 2],
        ];
        // Use range overlap so we don't miss timesheets that started before the window
        if (from && to) {
          tsFilters.push(["start_date", "<=", to]);
          tsFilters.push(["end_date", ">=", from]);
        } else if (from) {
          tsFilters.push(["end_date", ">=", from]);
        } else if (to) {
          tsFilters.push(["start_date", "<=", to]);
        }

        const timesheets = (await erp.getList(ctx.creds, "Timesheet", {
          fields: ["name", "start_date"],
          filters: tsFilters,
          limit_page_length: 200,
        })) as any[];

        if (timesheets.length) {
          const tsNames = timesheets.map((t) => String(t.name));
          const tsDateMap = new Map<string, string>();
          for (const ts of timesheets) tsDateMap.set(String(ts.name), String(ts.start_date ?? "").slice(0, 10));

          const otDetails = (await erp.getList(ctx.creds, "Timesheet Detail", {
            fields: ["parent", "hours"],
            filters: [["parent", "IN", tsNames], ["activity_type", "=", "Overtime"]],
            limit_page_length: 5000,
          })) as any[];

          const dateOtMap = new Map<string, number>();
          for (const d of otDetails) {
            const date = tsDateMap.get(String(d.parent ?? "")) ?? "";
            if (date) dateOtMap.set(date, (dateOtMap.get(date) ?? 0) + Number(d.hours ?? 0));
          }

          for (const row of merged) {
            const date = String((row as any).attendance_date ?? "").slice(0, 10);
            const ot = dateOtMap.get(date);
            if (ot != null) (row as any).overtime_hours = Number(ot.toFixed(2));
          }
        }
      } catch {
        // best-effort — don't fail the daily view if overtime query fails
      }

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
    const rawDaysPost = body.days_of_week;
    const daysArrPost = Array.isArray(rawDaysPost) ? rawDaysPost.map(String) : typeof rawDaysPost === "string" ? rawDaysPost.split(",") : [];
    const daysValuePost = daysArrPost.map((d) => d.trim().toLowerCase()).filter((d) => ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"].includes(d)).join(",");

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

      // Reject start_date earlier than the employee's date_of_joining.
      const dateOfJoining = String(empDoc.date_of_joining ?? "").slice(0, 10);
      if (dateOfJoining && start_date < dateOfJoining) {
        return reply.status(400).send({
          error: `Shift assignment cannot start before the employee's joining date (${dateOfJoining}).`,
          code: "HR_BEFORE_JOINING",
          joining_date: dateOfJoining,
        });
      }

      // ERPNext's Shift Assignment controller reads `department` from the Employee
      // record in a before_insert hook. If the employee has a stale display-name
      // (e.g. "Engineering" instead of the doc name "Engineering - NT"), ERPNext's
      // link validation rejects the save.
      //
      // Strategy: resolve the correct Department doc name and pass it *directly*
      // on the Shift Assignment doc so the before_insert hook uses our value.
      // If unresolvable, clear the Employee's department field so the hook copies
      // "" (blank optional Link = valid) instead of the stale invalid name.
      //
      //   1. getDoc("Department", rawDept) — fastest; succeeds if rawDept IS the doc name.
      //   2. getList by department_name — fallback for display-name stored on Employee.
      //   3. Neither found → patch Employee department to "" before creating.
      let resolvedDepartment: string | null = null;
      const rawDept = String((empDoc as any).department ?? "").trim();
      if (rawDept) {
        try {
          // 1. Try direct name lookup (getDoc throws 404 if not found).
          try {
            await erp.getDoc(ctx.creds, "Department", rawDept);
            resolvedDepartment = rawDept; // it IS a valid doc name
          } catch {
            // 404 or permission error — fall through to display-name lookup
          }

          if (resolvedDepartment === null) {
            // 2. Try to resolve via department_name (display name → doc name).
            const byDisplayName = (await erp.getList(ctx.creds, "Department", {
              fields: ["name"],
              filters: [["department_name", "=", rawDept]],
              limit_page_length: 1,
            })) as any[];
            resolvedDepartment = byDisplayName.length ? String((byDisplayName[0] as any).name ?? "").trim() : null;
          }

          console.log(`[shift-assign] employee ${employee}: raw dept="${rawDept}" → resolved="${resolvedDepartment ?? "(unresolvable)"}"`);

          // ERPNext's before_insert/validate hook unconditionally reads department
          // from the Employee record and overwrites whatever we pass in the doc.
          // The ONLY reliable fix is to update Employee.department BEFORE createDoc.
          const patchDept = resolvedDepartment ?? ""; // "" clears stale value if unresolvable
          if (rawDept !== patchDept) {
            console.log(`[shift-assign] patching Employee ${employee} department: "${rawDept}" → "${patchDept || "(cleared)"}"`);
            try {
              await erp.callMethod(ctx.creds, "frappe.client.set_value", {
                doctype: "Employee", name: employee, fieldname: "department", value: patchDept,
              });
            } catch (svErr) {
              console.warn("[shift-assign] set_value failed, trying updateDoc:", svErr instanceof Error ? svErr.message : String(svErr));
              try { await erp.updateDoc(ctx.creds, "Employee", employee, { department: patchDept }); }
              catch (udErr) { console.warn("[shift-assign] updateDoc also failed:", udErr instanceof Error ? udErr.message : String(udErr)); }
            }
          }
        } catch (deptErr) {
          console.warn("[shift-assign] department resolution error:", deptErr instanceof Error ? deptErr.message : String(deptErr));
        }
      }

      const doc: Record<string, unknown> = {
        employee,
        shift_type,
        start_date,
        ...(end_date ? { end_date } : {}),
        // Pass the resolved department directly so ERPNext's before_insert hook
        // uses the correct doc name instead of reading the (stale) Employee field.
        // If resolvedDepartment is null the field is omitted and the hook will
        // copy "" from the now-cleared Employee record.
        ...(resolvedDepartment != null ? { department: resolvedDepartment } : {}),
      };

      let created: unknown;
      try {
        created = await erp.createDoc(ctx.creds, "Shift Assignment", doc);
      } catch (e) {
        // Log the full error body to diagnose department / validation issues.
        if (e instanceof ErpError) {
          console.error("[shift-assign] createDoc failed", {
            status: e.status,
            body: typeof e.body === "string" ? e.body.slice(0, 800) : JSON.stringify(e.body ?? null).slice(0, 800),
            resolvedDepartment,
            rawDept,
          });
        }
        throw e;
      }

      const name = parseBodyString((created as Record<string, unknown>)?.name);
      if (name && daysValuePost) dowSet(name, daysValuePost);
      if (name) {
        try {
          await submitShiftAssignmentWithRetry(ctx.creds, name, 3);
        } catch (e) {
          // Any error during submit is non-fatal — the draft Shift Assignment is still
          // usable by ERPNext. Log for visibility but return success to the caller.
          if (e instanceof ErpError) {
            console.warn("[shift-assign] submit failed (non-fatal), leaving as draft", {
              status: e.status,
              body: typeof e.body === "string" ? e.body.slice(0, 800) : JSON.stringify(e.body ?? null).slice(0, 800),
            });
          } else {
            console.warn("[shift-assign] submit failed (non-fatal), leaving as draft", e instanceof Error ? e.message : String(e));
          }
          return { data: created, meta: { submitSkipped: true } };
        }
      }
      return { data: created, meta: { submitSkipped: false } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * PATCH /v1/attendance/shift-assignments/:name — update start_date, end_date, and/or days_of_week.
   */
  app.patch("/v1/attendance/shift-assignments/:name", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can update shift assignments." });
    }

    const { name } = req.params as { name: string };
    const body = (req.body ?? {}) as Record<string, unknown>;

    const rawDays = body.days_of_week;
    const daysArr = Array.isArray(rawDays) ? rawDays.map(String) : typeof rawDays === "string" ? rawDays.split(",") : [];
    const daysValue = daysArr.map((d) => d.trim().toLowerCase()).filter((d) => ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"].includes(d)).join(",");

    const start_date = typeof body.start_date === "string" && /^\d{4}-\d{2}-\d{2}$/.test(body.start_date.trim()) ? body.start_date.trim() : null;
    const end_date = typeof body.end_date === "string" && /^\d{4}-\d{2}-\d{2}$/.test(body.end_date.trim()) ? body.end_date.trim() : body.end_date === "" ? "" : null;

    // Validate date range.
    if (start_date && end_date && start_date > end_date) {
      return reply.status(400).send({ error: "Start date must not be after end date." });
    }

    // If dates are being updated, fetch the doc first so we can run the clock-in guard.
    if (start_date !== null || end_date !== null) {
      try {
        const doc = await erp.getDoc(ctx.creds, "Shift Assignment", name);
        if (String((doc as any).company) !== ctx.company) {
          return reply.status(403).send({ error: "Shift assignment not in your Company" });
        }
        const docstatus = Number((doc as any).docstatus ?? 0);
        if (docstatus === 2) {
          return reply.status(409).send({ error: "Cannot update a cancelled shift assignment." });
        }

        // Guard: block if the employee is currently clocked in.
        const employeeId = String((doc as any).employee ?? "").trim();
        if (employeeId) {
          const recentCheckins = (await erp.getList(ctx.creds, "Employee Checkin", {
            fields: ["name", "log_type", "time"],
            filters: [["employee", "=", employeeId]],
            order_by: "time desc",
            limit_page_length: 1,
          })) as any[];
          if (recentCheckins.length && String(recentCheckins[0].log_type ?? "").toUpperCase() === "IN") {
            return reply.status(409).send({
              error: "Employee is currently clocked in. They must clock out before this shift assignment can be edited.",
              code: "HR_EMPLOYEE_CLOCKED_IN",
            });
          }
        }

        const updates: Record<string, string> = {};
        if (start_date !== null) updates.start_date = start_date;
        if (end_date !== null) updates.end_date = end_date;

        if (docstatus === 0) {
          await erp.updateDoc(ctx.creds, "Shift Assignment", name, updates);
        } else {
          for (const [fieldname, value] of Object.entries(updates)) {
            await erp.callMethod(ctx.creds, "frappe.client.set_value", {
              doctype: "Shift Assignment", name, fieldname, value,
            });
          }
        }
      } catch (e) {
        if (e instanceof ErpError) return replyErp(reply, e);
        throw e;
      }
    }

    // Always save days to local store (even if empty = clear restriction).
    dowSet(name, daysValue);

    return { data: { name, days_of_week: daysValue, start_date, end_date } };
  });

  /**
   * HR only: unassign (cancel) a Shift Assignment.
   *
   * Guard: if the employee currently has an open clock-in (most recent Employee Checkin
   * is log_type="IN" with no matching OUT), the request is rejected — the employee must
   * clock out and have their attendance recorded before the shift can be removed.
   */
  app.delete("/v1/attendance/shift-assignments/:name", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "Only HR can unassign shifts." });
    }

    const { name } = req.params as { name: string };
    if (!name) return reply.status(400).send({ error: "Shift assignment name is required." });

    try {
      // Fetch the assignment to confirm it belongs to this company and get employee id.
      const saDoc = await erp.getDoc(ctx.creds, "Shift Assignment", name);
      const employeeId = String(saDoc.employee ?? "").trim();
      if (!employeeId) return reply.status(400).send({ error: "Shift assignment has no employee." });

      const empDoc = await erp.getDoc(ctx.creds, "Employee", employeeId);
      if (String(empDoc.company) !== ctx.company) {
        return reply.status(403).send({ error: "Employee not in your Company." });
      }

      // Guard: block if the employee is currently clocked in.
      // The most recent Employee Checkin for this employee tells us their state.
      const recentCheckins = (await erp.getList(ctx.creds, "Employee Checkin", {
        fields: ["name", "log_type", "time"],
        filters: [["employee", "=", employeeId]],
        order_by: "time desc",
        limit_page_length: 1,
      })) as any[];

      if (recentCheckins.length && String(recentCheckins[0].log_type ?? "").toUpperCase() === "IN") {
        return reply.status(409).send({
          error: "Employee is currently clocked in. They must clock out and have their attendance recorded before this shift can be unassigned.",
          code: "HR_EMPLOYEE_CLOCKED_IN",
        });
      }

      const docstatus = Number(saDoc.docstatus ?? 0);

      if (docstatus === 0) {
        // Draft: can delete directly.
        await erp.deleteDoc(ctx.creds, "Shift Assignment", name);
      } else if (docstatus === 1) {
        // Submitted: must cancel first, then delete.
        await erp.callMethod(ctx.creds, "frappe.client.cancel", {
          doctype: "Shift Assignment",
          name,
        });
        await erp.deleteDoc(ctx.creds, "Shift Assignment", name);
      } else {
        // Already cancelled (docstatus=2) — treat as success.
      }

      return { data: { unassigned: name } };
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
    /**
     * When true the step-2 fallback (outside window but assignment active today)
     * will NOT return a match if the shift window has already ended.
     * Use for clock-in to prevent clocking in after a shift has finished.
     * Leave false (default) for my-shift display so overtime banner keeps working.
     */
    strictWindow?: boolean;
  }): Promise<{
    shift_assignment_name: string;
    shift_type_name: string;
    shift_start_date: string; // shift-start calendar day (YYYY-MM-DD)
    shift_window: { startMs: number; endMs: number };
    shift_start_time: string; // "HH:MM:SS"
    shift_end_time: string;   // "HH:MM:SS"
  }> {
    const { ctx, employeeId, at, strictWindow = false } = params;
    const atMs = at.getTime();
    const tzOff = await getErpTzOffsetMinutes(ctx.creds);
    const ddNow = toLocalDate(at, tzOff);
    // Look back 2 days to catch overnight shifts whose start_date is yesterday.
    const ddPrev = toLocalDate(new Date(atMs - 24 * 3600 * 1000), tzOff);

    // Fetch all assignments that started on or before today.
    // We intentionally do NOT filter by end_date here because ERPNext cannot
    // express "end_date IS NULL OR end_date >= today" in a single filter list.
    // Open-ended assignments (no end_date) would be excluded by such a filter.
    // Instead we filter end_date in code below.
    const allAssignments = (await erp.getList(ctx.creds, "Shift Assignment", {
      fields: ["name", "shift_type", "start_date", "end_date", "docstatus"],
      filters: [
        ["company", "=", ctx.company],
        ["employee", "=", employeeId],
        ["docstatus", "!=", 2],
        ["start_date", "<=", ddNow],
      ],
      order_by: "start_date desc",
      limit_page_length: 50,
    })) as any[];

    // Keep assignments that are still active today (end_date >= today or no end_date),
    // plus yesterday's start_date to catch overnight shifts.
    const assignments = allAssignments.filter((sa) => {
      const startDate = String(sa.start_date ?? "").slice(0, 10);
      const endDate = String(sa.end_date ?? "").slice(0, 10);
      // Must have started by today
      if (!startDate || startDate > ddNow) return false;
      // If end_date is set it must not have passed (allow yesterday for overnight shifts)
      if (endDate && endDate < ddPrev) return false;
      return true;
    });

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

    const DOW_NAMES_RS = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"] as const;
    function dowAllowsDate(saName: string, dateIso: string): boolean {
      const dowStr = dowGet(String(saName ?? ""));
      if (!dowStr) return true; // no restriction — all days allowed
      const allowed = dowStr.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
      const dayName = DOW_NAMES_RS[new Date(dateIso + "T00:00:00").getDay()];
      return allowed.includes(dayName);
    }

    // 1) Prefer assignments whose shift window (calculated relative to TODAY or
    //    YESTERDAY for overnight shifts) covers the current timestamp.
    //    We use ddNow/ddPrev as the calendar day — NOT sa.start_date — because
    //    the attendance record must be dated today, not when the assignment began.
    for (const sa of assignments) {
      const saStartDate = String(sa.start_date ?? "").slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(saStartDate)) continue;
      const shiftTypeName = String(sa.shift_type ?? "");
      const timings = stByName.get(shiftTypeName);
      if (!timings) continue;

      // Try yesterday first (overnight shift that started yesterday evening),
      // then today (normal or overnight shift starting today).
      for (const calDay of [ddPrev, ddNow]) {
        if (!dowAllowsDate(sa.name, calDay)) continue;
        const win = shiftWindowToMs({
          shift_start_date: calDay,
          start_time: timings.start_time,
          end_time: timings.end_time,
          tzOffsetMinutes: tzOff,
        });
        if (!win) continue;
        if (atMs >= win.startMs && atMs <= win.endMs) {
          return {
            shift_assignment_name: String(sa.name),
            shift_type_name: shiftTypeName,
            shift_start_date: calDay,
            shift_window: win,
            shift_start_time: timings.start_time,
            shift_end_time: timings.end_time,
          };
        }
      }
    }

    // 2) Fallback: assignment is active today but clock-in is outside the shift
    //    window (e.g. early arrival). Use TODAY as the attendance date.
    //    When strictWindow=true we skip shifts whose window has already ended so
    //    employees cannot clock in after their shift is over.
    for (const sa of assignments) {
      const saStartDate = String(sa.start_date ?? "").slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(saStartDate)) continue;
      const shiftTypeName = String(sa.shift_type ?? "");
      const timings = stByName.get(shiftTypeName);
      if (!timings) continue;
      if (!dowAllowsDate(sa.name, ddNow)) continue;

      const saEndRaw = sa.end_date == null ? "" : String(sa.end_date).slice(0, 10);
      const saEnd = saEndRaw && /^\d{4}-\d{2}-\d{2}$/.test(saEndRaw) ? saEndRaw : ddNow;
      if (ddNow >= saStartDate && ddNow <= saEnd) {
        const win = shiftWindowToMs({
          shift_start_date: ddNow,
          start_time: timings.start_time,
          end_time: timings.end_time,
          tzOffsetMinutes: tzOff,
        });
        if (!win) continue;
        // strictWindow: don't allow clock-in after the shift window has ended
        if (strictWindow && atMs > win.endMs) continue;
        return {
          shift_assignment_name: String(sa.name),
          shift_type_name: shiftTypeName,
          shift_start_date: ddNow,
          shift_window: win,
          shift_start_time: timings.start_time,
          shift_end_time: timings.end_time,
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
    in_time?: string; // Frappe datetime "YYYY-MM-DD HH:MM:SS"
  }): Promise<string> {
    const { ctx, employeeId, attendance_date, shift_type_name, in_time } = params;
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
        ...(in_time ? { in_time } : {}),
      });
      return name;
    }

    const created = await erp.createDoc(ctx.creds, "Attendance", {
      employee: employeeId,
      company: ctx.company,
      attendance_date,
      status: "Present",
      shift: shift_type_name,
      ...(in_time ? { in_time } : {}),
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

  // Ensure required Activity Types exist. Called lazily before clock-out so the
  // first clock-out on a fresh install self-heals instead of returning a 400.
  async function ensureActivityTypes(creds: HrContext["creds"]): Promise<void> {
    const seeds = [
      { activity_type: "Regular Hours", costing_rate: 0, billing_rate: 0 },
      { activity_type: "Overtime", costing_rate: 0, billing_rate: 0 },
      { activity_type: "Night Shift", costing_rate: 0, billing_rate: 0 },
      { activity_type: "Public Holiday", costing_rate: 0, billing_rate: 0 },
    ];
    try {
      const names = seeds.map((s) => s.activity_type);
      const existing = (await erp.getList(creds, "Activity Type", {
        fields: ["name"],
        filters: [["name", "IN", names]],
        limit_page_length: 50,
      })) as any[];
      const existingNames = new Set(existing.map((r) => String(r.name)));
      for (const seed of seeds) {
        if (!existingNames.has(seed.activity_type)) {
          await erp.createDoc(creds, "Activity Type", seed);
        }
      }
    } catch {
      // best-effort — if ERPNext rejects the creation the later resolveActivityTypeMap
      // will still throw a descriptive 400 rather than silently missing types.
    }
  }

  /**
   * Shared clock-out / manual-time logic: split interval vs shift window and append to the daily Draft Timesheet.
   */
  async function appendClockSegmentForShiftAssignment(params: {
    ctx: HrContext;
    employeeId: string;
    from_time: string;
    to_time: string;
    shift_assignment_name: string;
  }): Promise<{
    timesheet: string;
    attendance_date: string;
    shift_assignment: string;
    regular_hours: number;
    overtime_hours: number;
    warnings: string[];
  }> {
    const { ctx, employeeId, from_time, to_time, shift_assignment_name } = params;

    const from = parseFrappeDateTime(from_time);
    const to = parseFrappeDateTime(to_time);
    if (!from || !to) throw new HttpError("Invalid from_time/to_time format", 400);
    const fromMs = from.getTime();
    const toMs = to.getTime();
    if (toMs <= fromMs) throw new HttpError("to_time must be after from_time", 400);

    // Fetch the shift assignment — it may have been deleted after the employee clocked in.
    let shiftAssignmentDoc: Record<string, unknown> | null = null;
    try {
      const doc = await erp.getDoc(ctx.creds, "Shift Assignment", shift_assignment_name);
      if (String((doc as any)?.employee ?? "") !== employeeId) {
        throw new HttpError("This shift assignment does not belong to the selected employee", 400);
      }
      shiftAssignmentDoc = doc as Record<string, unknown>;
    } catch (e) {
      if (e instanceof HttpError) throw e;
      if (e instanceof ErpError && e.status === 404) {
        // Shift assignment was deleted — allow clock-out with all hours as regular (no window splitting).
        shiftAssignmentDoc = null;
      } else {
        throw e;
      }
    }

    // Derive the shift calendar day from the clock-in time, NOT the assignment’s
    // start_date. The assignment start_date is when the recurring schedule began
    // (e.g. April 7); the attendance date must be the actual day worked (e.g. April 14).
    // For overnight shifts (e.g. 22:00–06:00) where clock-in is in the early hours,
    // the shift started the previous calendar day — try both.
    const tzOff = await getErpTzOffsetMinutes(ctx.creds);
    const fromDateStr = toLocalDate(from, tzOff);
    const fromDatePrevStr = toLocalDate(new Date(fromMs - 24 * 3600 * 1000), tzOff);
    let shiftCalendarDay = fromDateStr;
    let win: ReturnType<typeof shiftWindowToMs> = null;
    let shiftTypeName = "";

    if (shiftAssignmentDoc) {
      shiftTypeName = String(shiftAssignmentDoc.shift_type ?? "");
      if (!shiftTypeName) {
        throw new HttpError("Invalid shift assignment (missing shift type)", 400);
      }
      const shiftTypeDoc = await erp.getDoc(ctx.creds, "Shift Type", shiftTypeName);
      win = shiftWindowToMs({
        shift_start_date: fromDateStr,
        start_time: String(shiftTypeDoc?.start_time ?? ""),
        end_time: String(shiftTypeDoc?.end_time ?? ""),
        tzOffsetMinutes: tzOff,
      });
      if (!win || fromMs < win.startMs) {
        const winPrev = shiftWindowToMs({
          shift_start_date: fromDatePrevStr,
          start_time: String(shiftTypeDoc?.start_time ?? ""),
          end_time: String(shiftTypeDoc?.end_time ?? ""),
          tzOffsetMinutes: tzOff,
        });
        if (winPrev && fromMs >= winPrev.startMs) {
          shiftCalendarDay = fromDatePrevStr;
          win = winPrev;
        }
      }
      if (!win) throw new HttpError("Invalid shift type timing", 400);
    }

    await ensureActivityTypes(ctx.creds);
    const activityMap = await resolveActivityTypeMap(ctx.creds);
    if (!activityMap.overtime) {
      throw new HttpError("Overtime isn’t set up for time tracking yet. Please ask HR to complete work-category setup.", 400);
    }

    const time_logs: Record<string, unknown>[] = [];

    if (win) {
      const regularStartMs = Math.max(fromMs, win.startMs);
      const regularEndMs = Math.min(toMs, win.endMs);
      const regularMs = Math.max(0, regularEndMs - regularStartMs);
      // Overtime is only recorded when the employee stays past the end of their shift.
      const overtimeLateMs = toMs > win.endMs ? Math.max(0, toMs - Math.max(fromMs, win.endMs)) : 0;

      const regularActivity =
        shiftTypeName.toLowerCase().includes("night") && activityMap.night ? activityMap.night : activityMap.regular;
      if (regularMs > 0 && !regularActivity) {
        throw new HttpError(
          "Regular or night-shift hours aren’t set up for time tracking yet. Please ask HR to complete work-category setup.",
          400
        );
      }

      if (regularMs > 0 && regularActivity) {
        time_logs.push({
          activity_type: regularActivity,
          from_time: toFrappeDateTime(new Date(regularStartMs)),
          to_time: toFrappeDateTime(new Date(regularEndMs)),
          hours: Number((regularMs / 3600000).toFixed(2)),
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
    } else {
      // Fallback: shift assignment was deleted — record all elapsed time as regular.
      if (!activityMap.regular) {
        throw new HttpError(
          "Regular hours aren’t set up for time tracking yet. Please ask HR to complete work-category setup.",
          400
        );
      }
      time_logs.push({
        activity_type: activityMap.regular,
        from_time: from_time,
        to_time: toFrappeDateTime(new Date(toMs)),
        hours: Number(((toMs - fromMs) / 3600000).toFixed(2)),
        is_billable: 0,
      });
    }

    if (time_logs.length === 0) {
      throw new HttpError("No time would be recorded — clock times did not overlap this shift’s window.", 400);
    }

    const projectName = shiftAssignmentDoc ? ((shiftAssignmentDoc as any)?.project ?? null) : null;
    const shift_location = shiftAssignmentDoc ? ((shiftAssignmentDoc as any)?.shift_location ?? null) : null;
    const timesheetName = await getOrCreateDraftTimesheet({
      ctx,
      employeeId,
      shift_start_date: shiftCalendarDay,
      project: projectName,
      shift_location,
      time_logsToAppend: time_logs,
    });

    // Auto-submit: employees should not need to manually submit their timesheets.
    // This is best-effort — a failed submit leaves the timesheet as a Draft which
    // HR can still submit manually if needed.
    try {
      await submitTimesheetWithRetries(ctx, timesheetName);
    } catch (submitErr) {
      console.warn(
        "[clock-out] timesheet auto-submit failed (leaving as draft):",
        submitErr instanceof Error ? submitErr.message : String(submitErr),
      );
    }

    const totalHours = Number(time_logs.reduce((sum, l) => sum + Number(l.hours ?? 0), 0).toFixed(2));
    const warnings: string[] = [];

    // Update the Attendance record's in_time, out_time, and working_hours.
    // ERPNext auto-attendance may have already submitted the record (docstatus=1),
    // so we try updateDoc for drafts first, then fall back to frappe.client.set_value
    // which respects allow_on_submit fields on the Attendance doctype.
    try {
      const attendanceRows = (await erp.getList(ctx.creds, "Attendance", {
        fields: ["name", "docstatus", "in_time"],
        filters: [
          ["employee", "=", employeeId],
          ["attendance_date", "=", shiftCalendarDay],
          ["docstatus", "!=", 2],
        ],
        limit_page_length: 1,
      })) as any[];

      if (attendanceRows.length) {
        const attName = String(attendanceRows[0].name);
        const attDocstatus = Number(attendanceRows[0].docstatus ?? 0);
        const existingInTime = String(attendanceRows[0].in_time ?? "").trim();

        // Build the field updates: always set out_time and working_hours;
        // set in_time only if ERPNext hasn't populated it yet.
        const updates: Record<string, unknown> = {
          out_time: to_time,
          working_hours: totalHours,
          ...(existingInTime ? {} : { in_time: from_time }),
        };

        if (attDocstatus === 0) {
          // Draft — standard update works fine.
          await erp.updateDoc(ctx.creds, "Attendance", attName, updates);
          console.log("[clock-out] attendance updated (draft)", { attName, totalHours });
        } else {
          // Submitted — use frappe.client.set_value which honours allow_on_submit
          // fields (working_hours, in_time, out_time are allow_on_submit in ERPNext).
          for (const [fieldname, value] of Object.entries(updates)) {
            try {
              await erp.callMethod(ctx.creds, "frappe.client.set_value", {
                doctype: "Attendance",
                name: attName,
                fieldname,
                value,
              });
            } catch (fieldErr) {
              console.warn(`[clock-out] set_value failed for Attendance.${fieldname}:`, erpMsg(fieldErr));
            }
          }
          console.log("[clock-out] attendance updated (submitted via set_value)", { attName, totalHours });
        }
      } else {
        warnings.push("No attendance record found for today — your hours are saved but the attendance entry may need manual review.");
        console.warn("[clock-out] no Attendance record found for", { employeeId, shiftCalendarDay });
      }
    } catch (e) {
      warnings.push("Attendance record could not be updated automatically. Your hours are saved but HR may need to manually correct the attendance entry.");
      console.warn("[clock-out] attendance update failed (non-fatal):", erpMsg(e));
    }

    const regularHours = time_logs
      .filter((l) => l.activity_type !== activityMap.overtime)
      .reduce((sum, l) => sum + Number(l.hours ?? 0), 0);
    const overtimeHours = time_logs
      .filter((l) => l.activity_type === activityMap.overtime)
      .reduce((sum, l) => sum + Number(l.hours ?? 0), 0);

    return {
      timesheet: timesheetName,
      attendance_date: shiftCalendarDay,
      shift_assignment: shift_assignment_name,
      regular_hours: Number(regularHours.toFixed(2)),
      overtime_hours: Number(overtimeHours.toFixed(2)),
      warnings,
    };
  }

  async function submitTimesheetWithRetries(
    ctx: HrContext,
    name: string
  ): Promise<{ submitted: boolean; alreadySubmitted: boolean }> {
    let lastErr: unknown = null;
    for (let attempt = 1; attempt <= 5; attempt++) {
      try {
        // Re-fetch the full document on every attempt so we always have the
        // current `modified` timestamp. Passing the complete doc (including
        // `modified`) to frappe.client.submit prevents TimestampMismatchError
        // because Frappe's check_if_latest compares doc.modified == db.modified
        // — they match when we just fetched the doc.
        const doc = await erp.getDoc(ctx.creds, "Timesheet", name);
        if (Number((doc as any).docstatus ?? 0) === 1) {
          return { submitted: false, alreadySubmitted: true };
        }

        if (attempt > 1) await new Promise((r) => setTimeout(r, 1000 * attempt));

        await erp.callMethod(ctx.creds, "frappe.client.submit", {
          doc: { ...doc, doctype: "Timesheet", name },
        });
        return { submitted: true, alreadySubmitted: false };
      } catch (e) {
        lastErr = e;
        if (e instanceof ErpError && e.status === 417) {
          console.warn("[timesheet submit] upstream 417 (will retry)", {
            name,
            attempt,
            body: typeof e.body === "string" ? e.body.slice(0, 300) : (e.body as any)?.exception?.slice?.(0, 300),
          });
          continue;
        }
        throw e;
      }
    }
    throw lastErr;
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

    // Use docstatus=0 (not submitted, not cancelled) — more reliable than status="Draft"
    const existing = (await erp.getList(ctx.creds, "Timesheet", {
      fields: ["name"],
      filters: [
        ["employee", "=", employeeId],
        ["company", "=", ctx.company],
        ["start_date", "=", shift_start_date],
        ["docstatus", "=", 0],
      ],
      order_by: "modified desc",
      limit_page_length: 1,
    })) as any[];

    if (existing.length) {
      const name = String(existing[0].name);
      const doc = await erp.getDoc(ctx.creds, "Timesheet", name);
      const existingRows = Array.isArray((doc as any).time_logs) ? ((doc as any).time_logs as Record<string, unknown>[]) : [];
      const next = [...existingRows, ...time_logsToAppend];

      const tryUpdate = async (logs: Record<string, unknown>[], includeLocation: boolean) => {
        await erp.updateDoc(ctx.creds, "Timesheet", name, {
          ...(project ? { project } : {}),
          ...(includeLocation && shift_location !== undefined ? { shift_location } : {}),
          time_logs: logs,
        });
      };

      try {
        await tryUpdate(next, true);
      } catch (e) {
        const msg = erpMsg(e);
        const conflictTs = extractOverlapConflict(msg);
        if (conflictTs) {
          // Some rows already exist in a submitted timesheet — prune duplicates and retry
          const pruned = await pruneLogsAgainstTimesheet(ctx.creds, conflictTs, time_logsToAppend);
          if (pruned.length === 0) return conflictTs; // all time already recorded
          try {
            await tryUpdate([...existingRows, ...pruned], true);
          } catch (e2) {
            if (erpMsg(e2).toLowerCase().includes("shift_location") || erpMsg(e2).toLowerCase().includes("shift location")) {
              await tryUpdate([...existingRows, ...pruned], false);
            } else throw e2;
          }
          return name;
        }
        if (msg.toLowerCase().includes("shift_location") || msg.toLowerCase().includes("shift location")) {
          await tryUpdate(next, false);
        } else {
          throw e;
        }
      }
      return name;
    }

    // No editable draft found — create a new timesheet.
    // Before creating, check for an already-submitted timesheet covering the same
    // period to avoid ERPNext's overlap validation error.
    let logsForNew = time_logsToAppend;

    const baseDoc: Record<string, unknown> = {
      employee: employeeId,
      company: ctx.company,
      start_date: shift_start_date,
      end_date: shift_start_date,
      time_logs: logsForNew,
      ...(project ? { project } : {}),
    };

    const tryCreate = async (includeLocation: boolean): Promise<string> => {
      if (includeLocation && shift_location !== undefined) (baseDoc as any).shift_location = shift_location;
      else delete (baseDoc as any).shift_location;
      baseDoc.time_logs = logsForNew;
      const created = await erp.createDoc(ctx.creds, "Timesheet", baseDoc);
      return String((created as any)?.name ?? "");
    };

    try {
      return await tryCreate(true);
    } catch (e) {
      const msg = erpMsg(e);
      const conflictTs = extractOverlapConflict(msg);
      if (conflictTs) {
        // A submitted timesheet already covers some of this time — prune and retry
        logsForNew = await pruneLogsAgainstTimesheet(ctx.creds, conflictTs, logsForNew);
        if (logsForNew.length === 0) return conflictTs; // all time already recorded in conflictTs
        try {
          return await tryCreate(true);
        } catch (e2) {
          if (erpMsg(e2).toLowerCase().includes("shift_location") || erpMsg(e2).toLowerCase().includes("shift location")) {
            return await tryCreate(false);
          }
          throw e2;
        }
      }
      if (msg.toLowerCase().includes("shift_location") || msg.toLowerCase().includes("shift location")) {
        return await tryCreate(false);
      }
      throw e;
    }
  }

  /**
   * Optional GPS audit: creates ERPNext `Employee Checkin` when `location` is posted.
   * Fails soft if the site’s doctype lacks lat/long fields or permissions differ.
   */
  async function tryRecordEmployeeCheckinWithLocation(params: {
    ctx: HrContext;
    employeeId: string;
    logType: "IN" | "OUT";
    at: Date;
    location: unknown;
  }): Promise<{ recorded: boolean; reason?: string }> {
    const loc = asRecord(params.location);
    if (!loc) return { recorded: false };
    const lat = Number(loc.latitude);
    const lng = Number(loc.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return { recorded: false };

    const doc: Record<string, unknown> = {
      employee: params.employeeId,
      log_type: params.logType,
      time: toFrappeDateTime(params.at),
      latitude: lat,
      longitude: lng,
    };
    const acc = loc.accuracy_meters;
    if (acc != null && Number.isFinite(Number(acc))) {
      doc.accuracy = Number(acc);
    }

    try {
      await erp.createDoc(params.ctx.creds, "Employee Checkin", doc);
      return { recorded: true };
    } catch (e) {
      const msg = e instanceof ErpError ? String((e as ErpError).message ?? "") : String(e);
      return { recorded: false, reason: msg.slice(0, 240) };
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

    const body = (req.body ?? {}) as Record<string, unknown>;
    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const employeeId = await resolveEmployeeIdForRequest(ctx, qEmp);
    if (!employeeId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });

    try {
      const now = new Date();
      let active: Awaited<ReturnType<typeof resolveActiveShiftForTimestamp>>;
      try {
        active = await resolveActiveShiftForTimestamp({ ctx, employeeId, at: now, strictWindow: true });
      } catch (e) {
        if (e instanceof ErpError && e.status === 404) {
          return reply.status(422).send({
            error: "No active shift right now. If your shift has ended, clock in will be available when your next shift begins.",
            code: "HR_NO_SHIFT",
          });
        }
        throw e;
      }

      // Guard: if this shift's Attendance record already has an out_time the
      // employee has already clocked out for this shift and cannot re-clock-in
      // until the next shift begins.
      try {
        const prevAtt = (await erp.getList(ctx.creds, "Attendance", {
          fields: ["name", "out_time"],
          filters: [
            ["employee", "=", employeeId],
            ["attendance_date", "=", active.shift_start_date],
            ["shift", "=", active.shift_type_name],
            ["docstatus", "!=", 2],
          ],
          limit_page_length: 1,
        })) as any[];
        if (prevAtt.length && prevAtt[0].out_time) {
          return reply.status(409).send({
            error: "You have already clocked out for this shift. You cannot clock in again until your next shift begins.",
            code: "HR_ALREADY_CLOCKED_OUT",
          });
        }
      } catch {
        // best-effort — proceed if the check fails
      }

      const attendanceName = await ensureAttendancePresent({
        ctx,
        employeeId,
        attendance_date: active.shift_start_date,
        shift_type_name: active.shift_type_name,
        in_time: toFrappeDateTime(now),
      });

      const shiftAssignmentDoc = await erp.getDoc(ctx.creds, "Shift Assignment", active.shift_assignment_name);

      const checkinMeta = await tryRecordEmployeeCheckinWithLocation({
        ctx,
        employeeId,
        logType: "IN",
        at: now,
        location: body.location,
      });

      return {
        data: {
          from_time: toFrappeDateTime(now),
          attendance: attendanceName,
          attendance_date: active.shift_start_date,
          shift_assignment: active.shift_assignment_name,
          shift_type: active.shift_type_name,
          shift_start_time: active.shift_start_time,
          shift_end_time: active.shift_end_time,
          project: (shiftAssignmentDoc as any)?.project ?? null,
          shift_location: (shiftAssignmentDoc as any)?.shift_location ?? null,
        },
        meta: { employee_checkin: checkinMeta },
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

  /**
   * GET /v1/attendance/my-shift
   * Returns the active shift assignment for the calling employee at the current time,
   * including the shift type's start/end times so the UI can display them before clock-in.
   * Returns { data: null } (not an error) when no shift is active.
   */
  app.get("/v1/attendance/my-shift", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const employeeId = await resolveSelfEmployee(ctx);
    if (!employeeId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });

    try {
      const now = new Date();
      let active: Awaited<ReturnType<typeof resolveActiveShiftForTimestamp>>;
      try {
        active = await resolveActiveShiftForTimestamp({ ctx, employeeId, at: now });
      } catch (e) {
        if (e instanceof ErpError && e.status === 404) {
          return { data: null };
        }
        throw e;
      }

      // Fetch the shift assignment to get start_date / end_date for the display.
      const saDoc = await erp.getDoc(ctx.creds, "Shift Assignment", active.shift_assignment_name) as any;
      // Fetch the shift type to get raw start_time / end_time strings.
      const stDoc = await erp.getDoc(ctx.creds, "Shift Type", active.shift_type_name) as any;

      return {
        data: {
          shift_type: active.shift_type_name,
          start_time: String(stDoc?.start_time ?? ""),
          end_time: String(stDoc?.end_time ?? ""),
          start_date: String(saDoc?.start_date ?? "").slice(0, 10),
          end_date: saDoc?.end_date ? String(saDoc.end_date).slice(0, 10) : null,
          shift_assignment_name: active.shift_assignment_name,
          shift_location: saDoc?.shift_location ?? null,
        },
      };
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

    const toResolved = to_time ? to_time : toFrappeDateTime(new Date());
    try {
      const result = await appendClockSegmentForShiftAssignment({
        ctx,
        employeeId,
        from_time,
        to_time: toResolved,
        shift_assignment_name,
      });
      const outAt = parseFrappeDateTime(toResolved) ?? new Date();
      const checkinMeta = await tryRecordEmployeeCheckinWithLocation({
        ctx,
        employeeId,
        logType: "OUT",
        at: outAt,
        location: body.location,
      });
      return { data: result, meta: { employee_checkin: checkinMeta } };
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * HR only: add worked time for an employee (same rules as clock-out — regular vs overtime from shift window).
   */
  app.post("/v1/attendance/time-logs/manual", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "Only HR can add time on behalf of employees." });

    const body = (req.body ?? {}) as Record<string, unknown>;
    const employee = String(body.employee ?? "").trim();
    const from_time = String(body.from_time ?? "").trim();
    const to_time = String(body.to_time ?? "").trim();
    const shift_assignment_name = String(body.shift_assignment_name ?? "").trim();

    if (!employee || !from_time || !to_time || !shift_assignment_name) {
      return reply.status(400).send({ error: "employee, from_time, to_time, and shift_assignment_name are required" });
    }

    try {
      const empDoc = await erp.getDoc(ctx.creds, "Employee", employee);
      if (String(empDoc.company) !== ctx.company) {
        return reply.status(403).send({ error: "Employee not in your Company" });
      }

      const result = await appendClockSegmentForShiftAssignment({
        ctx,
        employeeId: employee,
        from_time,
        to_time,
        shift_assignment_name,
      });
      return { data: result };
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
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
        const overtimeHours = logs
          .filter((l) => String(l?.activity_type ?? "") === "Overtime")
          .reduce((acc, l) => acc + Number(l?.hours ?? 0), 0);
        const activityTypes = Array.from(new Set(logs.map((l) => String(l?.activity_type ?? "")).filter(Boolean)));
        const activity_type = activityTypes.length === 1 ? activityTypes[0] : activityTypes.length > 1 ? "Mixed" : null;

        // Clock-in = earliest from_time across all time log entries
        // Clock-out = latest to_time across all time log entries
        const fromTimes = logs.map((l) => String(l?.from_time ?? "").trim()).filter(Boolean).sort();
        const toTimes = logs.map((l) => String(l?.to_time ?? "").trim()).filter(Boolean).sort();
        const clock_in = fromTimes[0] ?? null;
        const clock_out = toTimes[toTimes.length - 1] ?? null;

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
          overtime_hours: Number(overtimeHours.toFixed(2)),
          clock_in,
          clock_out,
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
      const result = await submitTimesheetWithRetries(ctx, name);
      return { data: { name, submitted: result.submitted, alreadySubmitted: result.alreadySubmitted } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Payroll prep: timesheets in range with draft/submit readiness flags.
   */
  app.get("/v1/attendance/payroll-timesheet-checklist", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "Only HR can view the payroll checklist." });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    if (!from || !to) return reply.status(400).send({ error: "from_date and to_date are required (YYYY-MM-DD)" });

    try {
      const employeeId = qEmp ? await resolveEmployeeIdForRequest(ctx, qEmp) : null;
      if (qEmp && !employeeId) return reply.status(403).send({ error: "Employee not in your Company" });

      const filters: unknown[] = [["company", "=", ctx.company], ["docstatus", "!=", 2]];
      if (employeeId) filters.push(["employee", "=", employeeId]);
      filters.push(["start_date", "<=", to]);
      filters.push(["end_date", ">=", from]);

      const rows = (await erp.getList(ctx.creds, "Timesheet", {
        fields: ["name", "employee", "employee_name", "start_date", "end_date", "status", "docstatus", "project"],
        filters,
        order_by: "employee asc, start_date asc",
        limit_page_length: 500,
      })) as any[];

      const items: Record<string, unknown>[] = [];
      let draftCount = 0;
      let submittedCount = 0;
      let needsSubmitCount = 0;
      let totalHoursSum = 0;

      for (const r of rows) {
        const doc = await erp.getDoc(ctx.creds, "Timesheet", String(r.name));
        const logs = Array.isArray((doc as any).time_logs) ? ((doc as any).time_logs as any[]) : [];
        const totalHours = Number(
          logs.reduce((acc, l) => acc + Number(l?.hours ?? 0), 0).toFixed(2)
        );
        const ds = Number((doc as any).docstatus ?? 0);
        const st = String((doc as any).status ?? r.status ?? "");
        const isDraft = st === "Draft" && ds === 0;
        const isSubmitted = ds === 1;
        if (isDraft) draftCount++;
        if (isSubmitted) submittedCount++;
        const needsSubmit = isDraft && totalHours > 0;
        if (needsSubmit) needsSubmitCount++;
        totalHoursSum += totalHours;

        const workflowStateRaw = (doc as any).workflow_state;
        const workflow_state =
          workflowStateRaw != null && String(workflowStateRaw).trim() !== "" ? String(workflowStateRaw) : null;

        const warnings: string[] = [];
        if (isDraft && totalHours === 0) warnings.push("Draft with no hours logged");
        if (needsSubmit) warnings.push("Ready to finalize for payroll");
        if (workflow_state && timesheetWorkflowPending(workflow_state, timesheetWorkflowTerminalStates())) {
          warnings.push(`Workflow: ${workflow_state} (pending action)`);
        }

        items.push({
          name: String(r.name),
          employee: r.employee,
          employee_name: r.employee_name,
          start_date: r.start_date,
          project: (doc as any).project ?? r.project ?? null,
          status: st,
          docstatus: ds,
          workflow_state,
          total_hours: totalHours,
          needs_submit: needsSubmit,
          warnings,
        });
      }

      return {
        data: {
          items,
          summary: {
            draft_count: draftCount,
            submitted_count: submittedCount,
            needs_submit_count: needsSubmitCount,
            total_hours: Number(totalHoursSum.toFixed(2)),
          },
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * HR: submit many draft timesheets (staggered to reduce version conflicts).
   */
  app.post("/v1/attendance/timesheets/bulk-submit", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "Only HR can submit timesheets." });

    const body = (req.body ?? {}) as Record<string, unknown>;
    const namesRaw = body.names;
    const from = parseDate(body.from_date ?? "");
    const to = parseDate(body.to_date ?? "");
    const employee = String(body.employee ?? "").trim();

    try {
      let names: string[] = [];
      if (Array.isArray(namesRaw) && namesRaw.length) {
        names = namesRaw.map((x) => String(x ?? "").trim()).filter(Boolean);
      } else if (from && to) {
        const empId = employee ? await resolveEmployeeIdForRequest(ctx, employee) : null;
        if (employee && !empId) return reply.status(403).send({ error: "Employee not in your Company" });
        const filters: unknown[] = [
          ["company", "=", ctx.company],
          ["docstatus", "!=", 2],
          ["status", "=", "Draft"],
          ["start_date", "<=", to],
          ["end_date", ">=", from],
        ];
        if (empId) filters.push(["employee", "=", empId]);
        const rows = (await erp.getList(ctx.creds, "Timesheet", {
          fields: ["name"],
          filters,
          limit_page_length: 200,
        })) as any[];
        names = rows.map((r) => String(r.name));
      } else {
        return reply.status(400).send({ error: "Provide either names[] or from_date + to_date" });
      }

      const results: { name: string; ok: boolean; submitted?: boolean; already_submitted?: boolean; error?: string }[] =
        [];

      for (let i = 0; i < names.length; i++) {
        const tsName = names[i];
        if (i > 0) await new Promise((r) => setTimeout(r, 1200));
        try {
          const outcome = await submitTimesheetWithRetries(ctx, tsName);
          results.push({
            name: tsName,
            ok: true,
            submitted: outcome.submitted,
            already_submitted: outcome.alreadySubmitted,
          });
        } catch (e) {
          const msg = e instanceof ErpError ? String(publicErpFailure(e).error ?? e.message) : String(e);
          results.push({ name: tsName, ok: false, error: msg });
        }
      }

      return {
        data: {
          processed: results.length,
          results,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Payroll file: one row per time log line (submitted timesheets by default).
   * format=json | csv — CSV is returned as raw text with Content-Disposition attachment.
   */
  app.get("/v1/attendance/timesheets/payroll-export", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "Only HR can export payroll timesheets." });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    const format = String(q.format ?? "json").trim().toLowerCase();
    const submittedOnly = !["0", "false", "no"].includes(String(q.submitted_only ?? "1").trim().toLowerCase());

    if (!from || !to) return reply.status(400).send({ error: "from_date and to_date are required (YYYY-MM-DD)" });
    if (format !== "json" && format !== "csv") {
      return reply.status(400).send({ error: "format must be json or csv" });
    }

    try {
      const employeeId = qEmp ? await resolveEmployeeIdForRequest(ctx, qEmp) : null;
      if (qEmp && !employeeId) return reply.status(403).send({ error: "Employee not in your Company" });

      const filters: unknown[] = [["company", "=", ctx.company], ["docstatus", "!=", 2]];
      if (employeeId) filters.push(["employee", "=", employeeId]);
      filters.push(["start_date", "<=", to]);
      filters.push(["end_date", ">=", from]);
      if (submittedOnly) filters.push(["docstatus", "=", 1]);

      const rows = (await erp.getList(ctx.creds, "Timesheet", {
        fields: ["name", "employee", "employee_name", "start_date", "end_date", "status", "docstatus"],
        filters,
        order_by: "employee asc, start_date asc",
        limit_page_length: 500,
      })) as any[];

      type ExportRow = Record<string, string | number | null | boolean>;
      const exportRows: ExportRow[] = [];

      for (const r of rows) {
        const doc = await erp.getDoc(ctx.creds, "Timesheet", String(r.name));
        const logs = Array.isArray((doc as any).time_logs) ? ((doc as any).time_logs as any[]) : [];
        const tsStart = String((doc as any).start_date ?? r.start_date ?? "").slice(0, 10);
        const tsEnd = String((doc as any).end_date ?? r.end_date ?? "").slice(0, 10);
        const project = (doc as any).project != null ? String((doc as any).project) : "";
        const wf =
          (doc as any).workflow_state != null && String((doc as any).workflow_state).trim() !== ""
            ? String((doc as any).workflow_state)
            : null;
        const emp = String((doc as any).employee ?? r.employee ?? "");
        const empName = String((doc as any).employee_name ?? r.employee_name ?? "");
        const ds = Number((doc as any).docstatus ?? r.docstatus ?? 0);

        for (let i = 0; i < logs.length; i++) {
          const log = logs[i];
          const activity = String(log?.activity_type ?? "").trim() || "(unspecified)";
          const fromTime = log?.from_time != null ? String(log.from_time) : "";
          const toTime = log?.to_time != null ? String(log.to_time) : "";
          const hours = Number(log?.hours ?? 0);
          exportRows.push({
            company: ctx.company,
            employee: emp,
            employee_name: empName,
            timesheet: String(r.name),
            timesheet_start_date: tsStart,
            timesheet_end_date: tsEnd,
            project,
            activity_type: activity,
            from_time: fromTime,
            to_time: toTime,
            hours: Number(hours.toFixed(2)),
            is_overtime: isActivityOvertimeLabel(activity),
            docstatus: ds,
            workflow_state: wf,
            time_log_row: i,
          });
        }
      }

      const meta = {
        generated_at: new Date().toISOString(),
        from_date: from,
        to_date: to,
        submitted_only: submittedOnly,
        row_count: exportRows.length,
      };

      if (format === "json") {
        return { data: { ...meta, rows: exportRows } };
      }

      const header = [
        "company",
        "employee",
        "employee_name",
        "timesheet",
        "timesheet_start_date",
        "timesheet_end_date",
        "project",
        "activity_type",
        "from_time",
        "to_time",
        "hours",
        "is_overtime",
        "docstatus",
        "workflow_state",
        "time_log_row",
      ];
      const lines = [
        header.map((h) => csvEscapeCell(h)).join(","),
        ...exportRows.map((row) =>
          header
            .map((h) => {
              const v = row[h];
              if (typeof v === "boolean") return csvEscapeCell(v ? "true" : "false");
              if (v == null) return "";
              return csvEscapeCell(String(v));
            })
            .join(",")
        ),
      ];
      const csv = lines.join("\r\n") + "\r\n";
      const filename = `payroll-timesheets-${from}-${to}.csv`;
      reply.header("Content-Type", "text/csv; charset=utf-8");
      reply.header("Content-Disposition", `attachment; filename="${filename}"`);
      return reply.send(csv);
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * List timesheets for a specific employee (HR admin view from employee profile).
   * GET /v1/attendance/timesheets?employee=<id>&from_date=<YYYY-MM-DD>&to_date=<YYYY-MM-DD>
   */
  app.get("/v1/attendance/timesheets", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    if (!qEmp) return reply.status(400).send({ error: "employee is required" });

    try {
      const employeeId = await resolveEmployeeIdForRequest(ctx, qEmp);
      if (!employeeId) return reply.status(403).send({ error: "Employee not in your Company" });

      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["employee", "=", employeeId],
        ["docstatus", "!=", 2],
      ];
      // Range overlap: timesheet overlaps [from, to] when start_date <= to AND end_date >= from
      if (from && to) {
        filters.push(["start_date", "<=", to]);
        filters.push(["end_date", ">=", from]);
      } else if (from) {
        filters.push(["end_date", ">=", from]);
      } else if (to) {
        filters.push(["start_date", "<=", to]);
      }

      const rows = (await erp.getList(ctx.creds, "Timesheet", {
        fields: ["name", "employee", "employee_name", "start_date", "end_date", "status", "docstatus", "total_hours"],
        filters,
        order_by: "start_date desc",
        limit_page_length: 200,
      })) as any[];

      // Batch-fetch overtime hours from Timesheet Detail child records
      const tsNames = rows.map((r) => String(r.name));
      const overtimeByTs = new Map<string, number>();
      if (tsNames.length) {
        try {
          const otDetails = (await erp.getList(ctx.creds, "Timesheet Detail", {
            fields: ["parent", "hours"],
            filters: [["parent", "IN", tsNames], ["activity_type", "=", "Overtime"]],
            limit_page_length: 5000,
          })) as any[];
          for (const d of otDetails) {
            const p = String(d.parent ?? "");
            overtimeByTs.set(p, (overtimeByTs.get(p) ?? 0) + Number(d.hours ?? 0));
          }
        } catch {
          // best-effort
        }
      }

      const items = rows.map((r) => {
        const total = Number(r.total_hours ?? 0);
        const overtime = Number((overtimeByTs.get(String(r.name)) ?? 0).toFixed(2));
        return {
          name: String(r.name),
          employee: r.employee,
          employee_name: r.employee_name,
          start_date: String(r.start_date ?? ""),
          end_date: String(r.end_date ?? ""),
          status: String(r.status ?? ""),
          docstatus: Number(r.docstatus ?? 0),
          total_hours: total,
          overtime_hours: overtime,
        };
      });

      return { data: items };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Timesheets in range that are submitted but still in a non-terminal workflow state (when ERP uses Workflow on Timesheet).
   */
  app.get("/v1/attendance/timesheets/approval-queue", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "Only HR can view the approval queue." });

    const q = (req.query ?? {}) as Record<string, unknown>;
    const qEmp = String(q.employee ?? "").trim();
    const from = parseDate(q.from_date ?? q.from);
    const to = parseDate(q.to_date ?? q.to);
    if (!from || !to) return reply.status(400).send({ error: "from_date and to_date are required (YYYY-MM-DD)" });

    const terminal = timesheetWorkflowTerminalStates();

    try {
      const employeeId = qEmp ? await resolveEmployeeIdForRequest(ctx, qEmp) : null;
      if (qEmp && !employeeId) return reply.status(403).send({ error: "Employee not in your Company" });

      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["docstatus", "=", 1],
        ["start_date", "<=", to],
        ["end_date", ">=", from],
      ];
      if (employeeId) filters.push(["employee", "=", employeeId]);

      const rows = (await erp.getList(ctx.creds, "Timesheet", {
        fields: ["name", "employee", "employee_name", "start_date", "end_date", "status", "docstatus"],
        filters,
        order_by: "start_date asc, employee asc",
        limit_page_length: 500,
      })) as any[];

      const items: Record<string, unknown>[] = [];

      for (const r of rows) {
        const doc = await erp.getDoc(ctx.creds, "Timesheet", String(r.name));
        const wfRaw = (doc as any).workflow_state;
        const workflow_state =
          wfRaw != null && String(wfRaw).trim() !== "" ? String(wfRaw) : null;
        if (!timesheetWorkflowPending(workflow_state, terminal)) continue;

        const logs = Array.isArray((doc as any).time_logs) ? ((doc as any).time_logs as any[]) : [];
        const totalHours = Number(logs.reduce((acc, l) => acc + Number(l?.hours ?? 0), 0).toFixed(2));

        items.push({
          name: String(r.name),
          employee: r.employee,
          employee_name: r.employee_name,
          start_date: r.start_date,
          end_date: r.end_date,
          status: String((doc as any).status ?? r.status ?? ""),
          docstatus: Number((doc as any).docstatus ?? 1),
          workflow_state,
          total_hours: totalHours,
        });
      }

      return {
        data: {
          items,
          summary: {
            pending_count: items.length,
            terminal_states_hint: [...terminal].sort(),
          },
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Apply an ERP Workflow transition (e.g. Approve / Reject). Action names must match the workflow configured on Timesheet.
   */
  app.post("/v1/attendance/timesheets/:name/workflow-action", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "Only HR can apply timesheet workflow actions." });

    const params = (req.params ?? {}) as Record<string, unknown>;
    const name = String(params.name ?? "").trim();
    const body = (req.body ?? {}) as Record<string, unknown>;
    const action = String(body.action ?? "").trim();
    if (!name) return reply.status(400).send({ error: "Timesheet name is required" });
    if (!action) return reply.status(400).send({ error: "action is required (e.g. Approve or Reject)" });

    try {
      const doc = await erp.getDoc(ctx.creds, "Timesheet", name);
      if (String((doc as any)?.company ?? "") !== ctx.company) {
        return reply.status(403).send({ error: "Timesheet not in your Company" });
      }

      const frappeDoc = { ...doc, doctype: "Timesheet", name };
      await erp.callMethod(ctx.creds, "frappe.model.workflow.apply_workflow", {
        doc: frappeDoc,
        action,
      });

      const after = await erp.getDoc(ctx.creds, "Timesheet", name);
      const workflow_state =
        (after as any).workflow_state != null && String((after as any).workflow_state).trim() !== ""
          ? String((after as any).workflow_state)
          : null;

      return {
        data: {
          name,
          action,
          workflow_state,
          docstatus: Number((after as any).docstatus ?? 0),
        },
      };
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * Roll up hours by activity, project, and period (for payroll / reporting).
   */
  app.get("/v1/attendance/time-logs/summary", async (req, reply) => {
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
    const group = String(q.group ?? "total").trim().toLowerCase();
    if (!from || !to) return reply.status(400).send({ error: "from_date and to_date are required (YYYY-MM-DD)" });

    try {
      const employeeId = await resolveEmployeeIdForRequest(ctx, qEmp);
      if (!employeeId) return reply.status(403).send({ error: "No Employee linked to this user for this Company" });

      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["employee", "=", employeeId],
        ["docstatus", "!=", 2],
        ["start_date", "<=", to],
        ["end_date", ">=", from],
      ];

      const rows = (await erp.getList(ctx.creds, "Timesheet", {
        fields: ["name"],
        filters,
        order_by: "start_date asc",
        limit_page_length: 500,
      })) as any[];

      const byActivity = new Map<string, number>();
      const byProject = new Map<string, number>();
      let overtimeHours = 0;
      let nonOvertimeHours = 0;

      type PeriodBucket = { period: string; hours: number; overtime_hours: number };
      const byPeriod = new Map<string, PeriodBucket>();

      function periodKeyForDate(dStr: string): string {
        if (!/^\d{4}-\d{2}-\d{2}$/.test(dStr)) return dStr;
        if (group === "day") return dStr;
        const [y, m, day] = dStr.split("-").map((x) => parseInt(x, 10));
        const d = new Date(Date.UTC(y, m - 1, day));
        if (group === "month") return `${y}-${String(m).padStart(2, "0")}`;
        if (group === "week") {
          const jan4 = new Date(Date.UTC(y, 0, 4));
          const week1 = new Date(jan4);
          week1.setUTCDate(jan4.getUTCDate() - ((jan4.getUTCDay() + 6) % 7));
          const diffDays = Math.floor((d.getTime() - week1.getTime()) / (24 * 3600 * 1000));
          const w = Math.floor(diffDays / 7) + 1;
          return `${y}-W${String(w).padStart(2, "0")}`;
        }
        return "total";
      }

      for (const r of rows) {
        const doc = await erp.getDoc(ctx.creds, "Timesheet", String(r.name));
        const logs = Array.isArray((doc as any).time_logs) ? ((doc as any).time_logs as any[]) : [];
        const startDate = String((doc as any).start_date ?? "").slice(0, 10);
        const projectLabel = String((doc as any).project ?? "").trim() || "(no project)";

        for (const log of logs) {
          const h = Number(log?.hours ?? 0);
          const act = String(log?.activity_type ?? "").trim() || "(unspecified)";
          byActivity.set(act, (byActivity.get(act) ?? 0) + h);
          byProject.set(projectLabel, (byProject.get(projectLabel) ?? 0) + h);
          const isOt = act.toLowerCase().includes("overtime");
          if (isOt) overtimeHours += h;
          else nonOvertimeHours += h;

          if (group === "day" || group === "week" || group === "month") {
            const pk = periodKeyForDate(startDate);
            const cur = byPeriod.get(pk) ?? { period: pk, hours: 0, overtime_hours: 0 };
            cur.hours += h;
            if (isOt) cur.overtime_hours += h;
            byPeriod.set(pk, cur);
          }
        }
      }

      const toArr = (m: Map<string, number>) =>
        [...m.entries()]
          .map(([key, hours]) => ({ key, hours: Number(hours.toFixed(2)) }))
          .sort((a, b) => b.hours - a.hours);

      const payload: Record<string, unknown> = {
        totals: {
          hours: Number((overtimeHours + nonOvertimeHours).toFixed(2)),
          regular_or_other_hours: Number(nonOvertimeHours.toFixed(2)),
          overtime_hours: Number(overtimeHours.toFixed(2)),
        },
        by_activity: toArr(byActivity),
        by_project: toArr(byProject),
      };

      if (group === "day" || group === "week" || group === "month") {
        payload.by_period = [...byPeriod.values()]
          .map((b) => ({
            period: b.period,
            hours: Number(b.hours.toFixed(2)),
            overtime_hours: Number(b.overtime_hours.toFixed(2)),
          }))
          .sort((a, b) => a.period.localeCompare(b.period));
      }

      return { data: payload };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};

