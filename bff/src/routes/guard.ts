/**
 * CentyGuard — security operations API (ERP `centy_guard` + HRMS).
 * Phase 0–2: attendance rollups, reconciliation, per-site views, posting vs check-in exceptions.
 * Phase 3+: site clock-in (geofence), Pay Hub exception queue; Phase 4: CSV / payroll readiness in Pay Hub.
 * Phase 5: POST exception-review-sync → Frappe DocType GUARD_EXCEPTION_REVIEW_DOCTYPE (install on site).
 */
import type { FastifyPluginAsync, FastifyReply } from "fastify";
import type { HrContext } from "../types.js";
import { defaultClient } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import { publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import * as config from "../config.js";

const erp = defaultClient();

function replyErp(reply: FastifyReply, e: ErpError): FastifyReply {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

async function resolveSelfEmployee(ctx: HrContext): Promise<string | null> {
  for (const field of ["user_id", "personal_email", "prefered_email"] as const) {
    const mine = await erp.listDocs(ctx.creds, "Employee", {
      filters: [[field, "=", ctx.userEmail], ["company", "=", ctx.company]],
      fields: ["name"],
      limit_page_length: 1,
    });
    const empName = asRecord(mine.data?.[0])?.name;
    if (typeof empName === "string" && empName) {
      const secret = (config.HR_BRIDGE_SECRET || "").trim();
      const internalUrl = config.PAY_HUB_INTERNAL_URL;
      if (secret && internalUrl) {
        fetch(`${internalUrl.replace(/\/+$/, "")}/api/internal/employee-link`, {
          method: "POST",
          headers: { "Content-Type": "application/json", Authorization: `Bearer ${secret}` },
          body: JSON.stringify({ user_email: ctx.userEmail, erp_employee_id: empName }),
        }).catch(() => { /* non-fatal */ });
      }
      return empName;
    }
  }
  return null;
}

function parseYmd(v: unknown): string {
  const s = String(v ?? "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : "";
}

function haversineMeters(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371000;
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 2 * R * Math.asin(Math.min(1, Math.sqrt(a)));
}

function toFrappeDateTime(d: Date): string {
  return d.toISOString().slice(0, 19).replace("T", " ");
}

/** First non-empty PSRA / guard grade signal on an Employee row. */
function psraGradeFromEmployee(row: Record<string, unknown>): string {
  const keys = [
    "custom_psra_grade",
    "custom_centy_psra_grade",
    "psra_grade",
    "centy_psra_grade",
    "custom_psra_security_grade",
  ];
  for (const k of keys) {
    const v = row[k];
    if (v != null && String(v).trim()) return String(v).trim();
  }
  return "";
}

async function listActiveEmployees(
  ctx: HrContext,
  extraFields: string[],
): Promise<Record<string, unknown>[]> {
  const baseFields = ["name", "employee_name", "status", "company"];
  const fields = Array.from(new Set([...baseFields, ...extraFields]));
  const out: Record<string, unknown>[] = [];
  let start = 0;
  const page = 300;
  for (;;) {
    const batch = (await erp.getList(ctx.creds, "Employee", {
      filters: [
        ["company", "=", ctx.company],
        ["status", "=", "Active"],
      ],
      fields,
      limit_page_length: page,
      limit_start: start,
      order_by: "name asc",
    })) as Record<string, unknown>[];
    out.push(...batch);
    if (batch.length < page) break;
    start += page;
    if (start > 20_000) break;
  }
  return out;
}

function trySplitPsraEligible(rows: Record<string, unknown>[]): {
  graded: Record<string, unknown>[];
  mode: "psra_graded" | "all_active";
} {
  const graded = rows.filter((r) => psraGradeFromEmployee(r).length > 0);
  if (graded.length > 0) return { graded, mode: "psra_graded" };
  return { graded: rows, mode: "all_active" };
}

function dayBoundsUtc(ymd: string): { from: string; to: string } {
  // ERPNext often stores local wall time; use inclusive calendar day strings.
  return { from: `${ymd} 00:00:00`, to: `${ymd} 23:59:59` };
}

async function checkinsInRange(
  ctx: HrContext,
  from: string,
  to: string,
  logType: "IN" | "OUT",
): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  let start = 0;
  const page = 500;
  for (;;) {
    const batch = (await erp.getList(ctx.creds, "Employee Checkin", {
      filters: [
        ["time", ">=", from],
        ["time", "<=", to],
        ["log_type", "=", logType],
      ],
      fields: ["name", "employee", "time", "log_type", "device_id", "shift"],
      limit_page_length: page,
      limit_start: start,
      order_by: "time asc",
    })) as Record<string, unknown>[];
    rows.push(...batch);
    if (batch.length < page) break;
    start += page;
    if (start > 15_000) break;
  }
  return rows;
}

function localDayFromTime(t: unknown): string {
  const s = String(t ?? "").trim();
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  return m ? m[1] : "";
}

function hhMmToMinutes(s: string): number | null {
  const m = String(s ?? "")
    .trim()
    .match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;
  return Number(m[1]) * 60 + Number(m[2]);
}

/** Minutes since midnight from ERP check-in `time` string. */
function minutesFromCheckinTime(t: unknown): number | null {
  const s = String(t ?? "");
  const m = s.match(/\s(\d{1,2}):(\d{2})(?::(\d{2}))?/) || s.match(/T(\d{1,2}):(\d{2})/);
  if (!m) return null;
  return Number(m[1]) * 60 + Number(m[2]);
}

function chunkArray<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/** Map Site Shift Pattern name → expected shift start (minutes), via Shift Type.start_time. */
async function loadPatternStartMinutes(
  ctx: HrContext,
  patternNames: string[],
): Promise<Map<string, number | null>> {
  const out = new Map<string, number | null>();
  const uniq = [...new Set(patternNames.filter(Boolean))];
  if (!uniq.length) return out;

  let patterns: Record<string, unknown>[] = [];
  try {
    for (const part of chunkArray(uniq, 40)) {
      const batch = (await erp.getList(ctx.creds, "Site Shift Pattern", {
        filters: [["name", "in", part]],
        fields: ["name", "shift_type"],
        limit_page_length: 200,
      })) as Record<string, unknown>[];
      patterns.push(...batch);
    }
  } catch {
    return out;
  }

  const stNames = [...new Set(patterns.map((p) => String(p.shift_type ?? "")).filter(Boolean))];
  const stStart = new Map<string, number | null>();
  try {
    for (const part of chunkArray(stNames, 40)) {
      const types = (await erp.getList(ctx.creds, "Shift Type", {
        filters: [["name", "in", part]],
        fields: ["name", "start_time"],
        limit_page_length: 200,
      })) as Record<string, unknown>[];
      for (const t of types) {
        stStart.set(String(t.name), hhMmToMinutes(String(t.start_time ?? "")));
      }
    }
  } catch {
    return out;
  }

  for (const p of patterns) {
    const nm = String(p.name ?? "");
    const st = String(p.shift_type ?? "");
    out.set(nm, stStart.get(st) ?? null);
  }
  return out;
}

async function listSiteAssignmentsForDate(
  ctx: HrContext,
  ymd: string,
): Promise<Record<string, unknown>[]> {
  try {
    return (await erp.getList(ctx.creds, "Site Assignment", {
      filters: [
        ["company", "=", ctx.company],
        ["posting_date", "=", ymd],
        ["docstatus", "!=", 2],
      ],
      fields: ["name", "employee", "client_site", "shift_pattern", "status", "posting_date"],
      limit_page_length: 5000,
      order_by: "client_site asc",
    })) as Record<string, unknown>[];
  } catch {
    return [];
  }
}

async function listSiteAssignmentsPostingInRange(
  ctx: HrContext,
  fromDate: string,
  toDate: string,
): Promise<Record<string, unknown>[]> {
  const rows: Record<string, unknown>[] = [];
  let start = 0;
  const page = 500;
  try {
    for (;;) {
      const batch = (await erp.getList(ctx.creds, "Site Assignment", {
        filters: [
          ["company", "=", ctx.company],
          ["docstatus", "!=", 2],
          ["posting_date", ">=", fromDate],
          ["posting_date", "<=", toDate],
        ],
        fields: ["name", "employee", "client_site", "posting_date"],
        limit_page_length: page,
        limit_start: start,
        order_by: "posting_date desc",
      })) as Record<string, unknown>[];
      rows.push(...batch);
      if (batch.length < page) break;
      start += page;
      if (start > 25_000) break;
    }
  } catch {
    return [];
  }
  return rows;
}

export const guardRoutes: FastifyPluginAsync = async (app) => {
  /**
   * GET /v1/guard/attendance/daily-summary?date=YYYY-MM-DD
   * Company-wide when caller has HR bridge rights; otherwise self-only snapshot.
   */
  app.get("/v1/guard/attendance/daily-summary", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const date = parseYmd(q.date) || new Date().toISOString().slice(0, 10);
    const { from, to } = dayBoundsUtc(date);

    try {
      if (!ctx.canSubmitOnBehalf) {
        const selfId = await resolveSelfEmployee(ctx);
        if (!selfId) {
          return {
            data: {
              date,
              clocked_in_count: 0,
              eligible_count: 0,
              pct: null,
              denominator_mode: "all_active",
              scope: "self",
              notice: "No Employee record linked to your login.",
            },
          };
        }
        const checkins = await checkinsInRange(ctx, from, to, "IN");
        const mine = checkins.filter((c) => String(c.employee) === selfId);
        const clocked = mine.length > 0 ? 1 : 0;
        return {
          data: {
            date,
            clocked_in_count: clocked,
            eligible_count: 1,
            pct: clocked ? 100 : 0,
            denominator_mode: "all_active",
            scope: "self",
            notice: "Company-wide totals require HR access.",
          },
        };
      }

      let employees: Record<string, unknown>[] = [];
      try {
        employees = await listActiveEmployees(ctx, [
          "custom_psra_grade",
          "custom_centy_psra_grade",
          "psra_grade",
          "centy_psra_grade",
          "custom_psra_security_grade",
        ]);
      } catch (e) {
        if (e instanceof ErpError) return replyErp(reply, e);
        throw e;
      }

      const { graded, mode } = trySplitPsraEligible(employees);
      const eligibleNames = new Set(graded.map((r) => String(r.name ?? "")).filter(Boolean));

      let checkins: Record<string, unknown>[] = [];
      try {
        checkins = await checkinsInRange(ctx, from, to, "IN");
      } catch (e) {
        if (e instanceof ErpError) return replyErp(reply, e);
        throw e;
      }

      const clockedIds = new Set<string>();
      for (const c of checkins) {
        const emp = String(c.employee ?? "");
        if (eligibleNames.has(emp)) clockedIds.add(emp);
      }

      const eligibleCount = eligibleNames.size;
      const clockedInCount = clockedIds.size;
      let pct: number | null = null;
      if (eligibleCount > 0) pct = Math.round((1000 * clockedInCount) / eligibleCount) / 10;

      return {
        data: {
          date,
          clocked_in_count: clockedInCount,
          eligible_count: eligibleCount,
          pct,
          denominator_mode: mode,
          scope: "company",
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * GET /v1/guard/attendance/drilldown?date=YYYY-MM-DD
   * Rows: one per employee with at least one IN that day (first IN time).
   */
  app.get("/v1/guard/attendance/drilldown", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const date = parseYmd(q.date) || new Date().toISOString().slice(0, 10);
    const { from, to } = dayBoundsUtc(date);

    try {
      if (!ctx.canSubmitOnBehalf) {
        const selfId = await resolveSelfEmployee(ctx);
        if (!selfId) return reply.status(403).send({ error: "No Employee linked to this user" });
        const checkins = await checkinsInRange(ctx, from, to, "IN");
        const mine = checkins.filter((c) => String(c.employee) === selfId);
        const rows = mine.slice(0, 1).map((c) => ({
          employee: selfId,
          employee_name: selfId,
          first_in_time: c.time,
          device_id: c.device_id ?? null,
          log_name: c.name,
        }));
        return { data: { date, rows, scope: "self" } };
      }

      const employees = await listActiveEmployees(ctx, [
        "custom_psra_grade",
        "custom_centy_psra_grade",
        "psra_grade",
        "centy_psra_grade",
        "custom_psra_security_grade",
      ]);
      const { graded } = trySplitPsraEligible(employees);
      const eligibleSet = new Set(graded.map((r) => String(r.name ?? "")).filter(Boolean));
      const nameById = new Map<string, string>();
      for (const r of graded) {
        const id = String(r.name ?? "");
        if (!id) continue;
        nameById.set(id, String(r.employee_name ?? r.name ?? id));
      }

      const checkins = await checkinsInRange(ctx, from, to, "IN");
      const firstByEmp = new Map<string, Record<string, unknown>>();
      for (const c of checkins) {
        const emp = String(c.employee ?? "");
        if (!eligibleSet.has(emp)) continue;
        if (!firstByEmp.has(emp)) firstByEmp.set(emp, c);
      }

      const siteAssignments = await listSiteAssignmentsForDate(ctx, date);
      const sitesForEmp = new Map<string, string[]>();
      for (const sa of siteAssignments) {
        const emp = String(sa.employee ?? "");
        const cs = String(sa.client_site ?? "");
        if (!emp || !cs) continue;
        if (!sitesForEmp.has(emp)) sitesForEmp.set(emp, []);
        sitesForEmp.get(emp)!.push(cs);
      }

      const rows = Array.from(firstByEmp.entries()).map(([emp, c]) => {
        const siteList = sitesForEmp.get(emp);
        let inferred_site: string | null = null;
        if (siteList && siteList.length === 1) inferred_site = siteList[0];
        else if (siteList && siteList.length > 1) inferred_site = [...new Set(siteList)].join(", ");
        return {
          employee: emp,
          employee_name: nameById.get(emp) ?? emp,
          first_in_time: c.time,
          device_id: c.device_id ?? null,
          log_name: c.name,
          inferred_site,
        };
      });
      rows.sort((a, b) => String(a.employee_name).localeCompare(String(b.employee_name)));

      return { data: { date, rows, scope: "company" } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * GET /v1/guard/attendance/reconciliation?from_date=&to_date=
   * Shift Assignment overlap count vs Employee Checkin IN (distinct local days) per employee.
   */
  app.get("/v1/guard/attendance/reconciliation", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR access is required for reconciliation" });
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const fromDate = parseYmd(q.from_date);
    const toDate = parseYmd(q.to_date);
    if (!fromDate || !toDate) {
      return reply.status(400).send({ error: "from_date and to_date (YYYY-MM-DD) are required" });
    }
    if (fromDate > toDate) return reply.status(400).send({ error: "from_date must be <= to_date" });

    const basisRaw = String(q.basis ?? "shift_assignment").trim();
    const basis = basisRaw === "site_assignment" ? "site_assignment" : "shift_assignment";

    const windowFrom = `${fromDate} 00:00:00`;
    const windowTo = `${toDate} 23:59:59`;

    try {
      const employees = await listActiveEmployees(ctx, []);
      const { graded } = trySplitPsraEligible(employees);
      const eligibleSet = new Set(graded.map((r) => String(r.name ?? "")).filter(Boolean));
      const nameById = new Map<string, string>();
      for (const r of graded) {
        const id = String(r.name ?? "");
        if (!id) continue;
        nameById.set(id, String(r.employee_name ?? r.name ?? id));
      }

      const shiftCountByEmp = new Map<string, number>();

      if (basis === "site_assignment") {
        const siteRows = await listSiteAssignmentsPostingInRange(ctx, fromDate, toDate);
        for (const r of siteRows) {
          const emp = String(r.employee ?? "");
          if (!eligibleSet.has(emp)) continue;
          shiftCountByEmp.set(emp, (shiftCountByEmp.get(emp) ?? 0) + 1);
        }
      } else {
        let shiftRows: Record<string, unknown>[] = [];
        let start = 0;
        const page = 500;
        for (;;) {
          const batch = (await erp.getList(ctx.creds, "Shift Assignment", {
            filters: [
              ["company", "=", ctx.company],
              ["docstatus", "!=", 2],
              ["start_date", "<=", toDate],
            ],
            fields: ["name", "employee", "start_date", "end_date", "shift_type"],
            limit_page_length: page,
            limit_start: start,
            order_by: "modified desc",
          })) as Record<string, unknown>[];
          shiftRows.push(...batch);
          if (batch.length < page) break;
          start += page;
          if (start > 20_000) break;
        }

        function assignmentOverlapsWindow(r: Record<string, unknown>): boolean {
          const sd = String(r.start_date ?? "").slice(0, 10);
          const edRaw = r.end_date;
          const ed = edRaw == null || String(edRaw).trim() === "" ? "" : String(edRaw).slice(0, 10);
          if (!sd) return false;
          if (sd > toDate) return false;
          if (!ed) return sd <= toDate;
          if (ed < fromDate) return false;
          return true;
        }

        for (const r of shiftRows) {
          if (!assignmentOverlapsWindow(r)) continue;
          const emp = String(r.employee ?? "");
          if (!eligibleSet.has(emp)) continue;
          shiftCountByEmp.set(emp, (shiftCountByEmp.get(emp) ?? 0) + 1);
        }
      }

      const checkins = await checkinsInRange(ctx, windowFrom, windowTo, "IN");
      const daysByEmp = new Map<string, Set<string>>();
      const eventsByEmp = new Map<string, number>();
      for (const c of checkins) {
        const emp = String(c.employee ?? "");
        if (!eligibleSet.has(emp)) continue;
        const day = localDayFromTime(c.time);
        if (!day || day < fromDate || day > toDate) continue;
        if (!daysByEmp.has(emp)) daysByEmp.set(emp, new Set());
        daysByEmp.get(emp)!.add(day);
        eventsByEmp.set(emp, (eventsByEmp.get(emp) ?? 0) + 1);
      }

      const rows: {
        employee: string;
        employee_name: string;
        shift_assignments_in_period: number;
        days_with_checkin: number;
        checkin_events: number;
        status: string;
      }[] = [];

      for (const id of eligibleSet) {
        const shifts = shiftCountByEmp.get(id) ?? 0;
        const daySet = daysByEmp.get(id);
        const daysWith = daySet ? daySet.size : 0;
        const ev = eventsByEmp.get(id) ?? 0;
        let status = "ok";
        if (shifts > 0 && daysWith === 0) status = "scheduled_no_attendance";
        else if (shifts === 0 && daysWith > 0) status = "attendance_no_shift";
        else if (shifts === 0 && daysWith === 0) status = "no_activity";
        else status = "matched";

        rows.push({
          employee: id,
          employee_name: nameById.get(id) ?? id,
          shift_assignments_in_period: shifts,
          days_with_checkin: daysWith,
          checkin_events: ev,
          status,
        });
      }

      rows.sort((a, b) => {
        const pri = (s: string) =>
          s === "scheduled_no_attendance" ? 0 : s === "attendance_no_shift" ? 1 : s === "no_activity" ? 3 : 2;
        const d = pri(a.status) - pri(b.status);
        if (d !== 0) return d;
        return a.employee_name.localeCompare(b.employee_name);
      });

      const flagged = rows.filter((r) => r.status === "scheduled_no_attendance" || r.status === "attendance_no_shift")
        .length;

      const note =
        basis === "site_assignment"
          ? "Site Assignment rows with posting_date in the window are counted; check-ins are IN logs with distinct calendar days."
          : "Shift Assignment rows are counted if they overlap the window; check-ins are IN logs with distinct calendar days.";

      return {
        data: {
          from_date: fromDate,
          to_date: toDate,
          basis,
          rows,
          summary: {
            eligible_employees: eligibleSet.size,
            flagged_mismatch: flagged,
            note,
          },
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * GET /v1/guard/attendance/by-site?date=YYYY-MM-DD
   * Per customer site: Site Assignment rows vs employees with an IN that day (HR only).
   */
  app.get("/v1/guard/attendance/by-site", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR access is required for per-site attendance" });
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const date = parseYmd(q.date) || new Date().toISOString().slice(0, 10);
    const { from, to } = dayBoundsUtc(date);

    try {
      const employees = await listActiveEmployees(ctx, []);
      const { graded } = trySplitPsraEligible(employees);
      const eligibleSet = new Set(graded.map((r) => String(r.name ?? "")).filter(Boolean));
      const siteNameById = new Map<string, string>();
      try {
        const sites = (await erp.getList(ctx.creds, "Client Site", {
          filters: [["company", "=", ctx.company]],
          fields: ["name", "site_name"],
          limit_page_length: 2000,
        })) as Record<string, unknown>[];
        for (const s of sites) siteNameById.set(String(s.name), String(s.site_name ?? s.name));
      } catch {
        /* empty */
      }

      const assignments = await listSiteAssignmentsForDate(ctx, date);
      const checkins = await checkinsInRange(ctx, from, to, "IN");
      const firstInByEmp = new Map<string, Record<string, unknown>>();
      for (const c of checkins) {
        const emp = String(c.employee ?? "");
        if (!eligibleSet.has(emp)) continue;
        if (!firstInByEmp.has(emp)) firstInByEmp.set(emp, c);
      }

      const scheduledEmpSet = new Set<string>();
      for (const a of assignments) {
        const emp = String(a.employee ?? "");
        if (eligibleSet.has(emp)) scheduledEmpSet.add(emp);
      }

      type SiteAgg = {
        client_site: string;
        site_name: string;
        assignment_rows: number;
        fulfilled_rows: number;
        no_show_rows: number;
      };
      const bySite = new Map<string, SiteAgg>();

      function ensureSite(cs: string): SiteAgg {
        let g = bySite.get(cs);
        if (!g) {
          g = {
            client_site: cs,
            site_name: siteNameById.get(cs) || cs,
            assignment_rows: 0,
            fulfilled_rows: 0,
            no_show_rows: 0,
          };
          bySite.set(cs, g);
        }
        return g;
      }

      for (const a of assignments) {
        const cs = String(a.client_site ?? "");
        const emp = String(a.employee ?? "");
        if (!cs || !eligibleSet.has(emp)) continue;
        const g = ensureSite(cs);
        g.assignment_rows += 1;
        if (firstInByEmp.has(emp)) g.fulfilled_rows += 1;
        else g.no_show_rows += 1;
      }

      /** Unique employees with IN who had no Site Assignment that day */
      let unscheduledCheckins = 0;
      for (const emp of firstInByEmp.keys()) {
        if (!scheduledEmpSet.has(emp)) unscheduledCheckins += 1;
      }

      const sites = [...bySite.values()].sort((a, b) => a.site_name.localeCompare(b.site_name));

      return {
        data: {
          date,
          summary: {
            assignment_rows: assignments.filter((a) => eligibleSet.has(String(a.employee ?? ""))).length,
            employees_scheduled: scheduledEmpSet.size,
            employees_with_checkin: firstInByEmp.size,
            unscheduled_checkins: unscheduledCheckins,
            note: "Per row: Site Assignment posting for this date. Check-in fulfilled if that employee has any IN that day.",
          },
          sites,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * GET /v1/guard/attendance/exceptions?date=YYYY-MM-DD&grace_minutes=15
   * no_show, unscheduled_in, late (when Site Shift Pattern → Shift Type resolves).
   */
  app.get("/v1/guard/attendance/exceptions", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR access is required for attendance exceptions" });
    }

    const q = (req.query ?? {}) as Record<string, unknown>;
    const date = parseYmd(q.date) || new Date().toISOString().slice(0, 10);
    const grace = Math.min(120, Math.max(0, Number(q.grace_minutes) || 15));
    const { from, to } = dayBoundsUtc(date);

    try {
      const employees = await listActiveEmployees(ctx, []);
      const { graded } = trySplitPsraEligible(employees);
      const eligibleSet = new Set(graded.map((r) => String(r.name ?? "")).filter(Boolean));
      const nameById = new Map<string, string>();
      for (const r of graded) {
        const id = String(r.name ?? "");
        if (!id) continue;
        nameById.set(id, String(r.employee_name ?? r.name ?? id));
      }

      const siteNameById = new Map<string, string>();
      try {
        const sites = (await erp.getList(ctx.creds, "Client Site", {
          filters: [["company", "=", ctx.company]],
          fields: ["name", "site_name"],
          limit_page_length: 2000,
        })) as Record<string, unknown>[];
        for (const s of sites) siteNameById.set(String(s.name), String(s.site_name ?? s.name));
      } catch {
        /* empty */
      }

      const assignments = await listSiteAssignmentsForDate(ctx, date);
      const patternNames = assignments.map((a) => String(a.shift_pattern ?? "")).filter(Boolean);
      const patternStarts = await loadPatternStartMinutes(ctx, patternNames);

      const checkins = await checkinsInRange(ctx, from, to, "IN");
      const firstInByEmp = new Map<string, Record<string, unknown>>();
      for (const c of checkins) {
        const emp = String(c.employee ?? "");
        if (!eligibleSet.has(emp)) continue;
        if (!firstInByEmp.has(emp)) firstInByEmp.set(emp, c);
      }

      const scheduledEmpSet = new Set<string>();
      for (const a of assignments) {
        const emp = String(a.employee ?? "");
        if (eligibleSet.has(emp)) scheduledEmpSet.add(emp);
      }

      const exceptions: {
        type: string;
        employee: string;
        employee_name: string;
        client_site?: string;
        client_site_name?: string;
        assignment_name?: string;
        shift_pattern?: string;
        first_in_time?: string;
        expected_start_minutes?: number | null;
        actual_minutes?: number | null;
        minutes_late?: number | null;
      }[] = [];

      for (const a of assignments) {
        const emp = String(a.employee ?? "");
        const cs = String(a.client_site ?? "");
        if (!eligibleSet.has(emp) || !cs) continue;
        const nm = String(a.name ?? "");
        const pat = String(a.shift_pattern ?? "");
        if (!firstInByEmp.has(emp)) {
          exceptions.push({
            type: "no_show",
            employee: emp,
            employee_name: nameById.get(emp) ?? emp,
            client_site: cs,
            client_site_name: siteNameById.get(cs) ?? cs,
            assignment_name: nm,
            shift_pattern: pat || undefined,
          });
          continue;
        }
        const cin = firstInByEmp.get(emp)!;
        const actualM = minutesFromCheckinTime(cin.time);
        const expM = pat ? patternStarts.get(pat) ?? null : null;
        if (expM != null && actualM != null && actualM > expM + grace) {
          exceptions.push({
            type: "late",
            employee: emp,
            employee_name: nameById.get(emp) ?? emp,
            client_site: cs,
            client_site_name: siteNameById.get(cs) ?? cs,
            assignment_name: nm,
            shift_pattern: pat || undefined,
            first_in_time: String(cin.time ?? ""),
            expected_start_minutes: expM,
            actual_minutes: actualM,
            minutes_late: actualM - expM,
          });
        }
      }

      for (const emp of firstInByEmp.keys()) {
        if (!scheduledEmpSet.has(emp)) {
          const cin = firstInByEmp.get(emp)!;
          exceptions.push({
            type: "unscheduled_in",
            employee: emp,
            employee_name: nameById.get(emp) ?? emp,
            first_in_time: String(cin.time ?? ""),
          });
        }
      }

      exceptions.sort((x, y) => {
        const pri = (t: string) =>
          t === "no_show" ? 0 : t === "late" ? 1 : t === "unscheduled_in" ? 2 : 3;
        const d = pri(x.type) - pri(y.type);
        if (d !== 0) return d;
        return x.employee_name.localeCompare(y.employee_name);
      });

      return {
        data: {
          date,
          grace_minutes: grace,
          exceptions,
          summary: {
            no_show: exceptions.filter((e) => e.type === "no_show").length,
            late: exceptions.filter((e) => e.type === "late").length,
            unscheduled_in: exceptions.filter((e) => e.type === "unscheduled_in").length,
          },
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * GET /v1/guard/attendance/my-postings-today?date=YYYY-MM-DD
   * Self-service: Site Assignment rows for the signed-in employee on a calendar day, with site coordinates for geofence UI.
   */
  app.get("/v1/guard/attendance/my-postings-today", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const date = parseYmd(q.date) || new Date().toISOString().slice(0, 10);
    const selfId = await resolveSelfEmployee(ctx);
    if (!selfId) {
      return reply.status(404).send({ error: "No Employee record linked to your login." });
    }
    try {
      const assignments = await listSiteAssignmentsForDate(ctx, date);
      const mine = assignments.filter((a) => String(a.employee) === selfId);
      const siteIds = [...new Set(mine.map((a) => String(a.client_site ?? "")).filter(Boolean))];
      const coordBySite = new Map<string, { lat: number | null; lng: number | null }>();
      for (const sid of siteIds) {
        try {
          const doc = await erp.getDoc(ctx.creds, "Client Site", sid);
          const la = doc.latitude != null && String(doc.latitude).trim() !== "" ? Number(doc.latitude) : NaN;
          const lo = doc.longitude != null && String(doc.longitude).trim() !== "" ? Number(doc.longitude) : NaN;
          coordBySite.set(sid, {
            lat: Number.isFinite(la) ? la : null,
            lng: Number.isFinite(lo) ? lo : null,
          });
        } catch {
          coordBySite.set(sid, { lat: null, lng: null });
        }
      }
      const siteNameById = new Map<string, string>();
      try {
        const sites = (await erp.getList(ctx.creds, "Client Site", {
          filters: [["company", "=", ctx.company]],
          fields: ["name", "site_name"],
          limit_page_length: 2000,
        })) as Record<string, unknown>[];
        for (const s of sites) siteNameById.set(String(s.name), String(s.site_name ?? s.name));
      } catch {
        /* empty */
      }
      const postings = mine.map((a) => {
        const cs = String(a.client_site ?? "");
        const c = coordBySite.get(cs);
        return {
          assignment_name: String(a.name ?? ""),
          client_site: cs,
          site_name: siteNameById.get(cs) ?? cs,
          shift_pattern: String(a.shift_pattern ?? "") || undefined,
          latitude: c?.lat ?? null,
          longitude: c?.lng ?? null,
        };
      });
      return {
        data: {
          date,
          employee: selfId,
          postings,
          default_radius_meters: config.GUARD_GEOFENCE_DEFAULT_METERS,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * POST /v1/guard/attendance/site-checkin
   * Geofence-validated IN check-in at a Client Site (optional Site Assignment enforcement for that day).
   */
  app.post("/v1/guard/attendance/site-checkin", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const body = (req.body ?? {}) as Record<string, unknown>;
    const clientSite = String(body.client_site ?? "").trim();
    const lat = Number(body.latitude);
    const lng = Number(body.longitude);
    const requirePosting = body.require_posting !== false && body.require_posting !== "false";
    const radiusM = Math.min(
      5000,
      Math.max(
        20,
        Number.isFinite(Number(body.radius_meters)) ? Number(body.radius_meters) : config.GUARD_GEOFENCE_DEFAULT_METERS,
      ),
    );
    const date = parseYmd(body.date) || new Date().toISOString().slice(0, 10);

    if (!clientSite || !Number.isFinite(lat) || !Number.isFinite(lng)) {
      return reply.status(400).send({ error: "client_site, latitude, and longitude are required" });
    }

    const selfId = await resolveSelfEmployee(ctx);
    if (!selfId) {
      return reply.status(404).send({ error: "No Employee record linked to your login." });
    }

    const secret = (config.HR_BRIDGE_SECRET || "").trim();
    const internalUrl = config.PAY_HUB_INTERNAL_URL;

    try {
      if (secret && internalUrl) {
        const checkRes = await fetch(
          `${internalUrl}/api/internal/compulsory-leave/active?employee=${encodeURIComponent(selfId)}&date=${encodeURIComponent(date)}`,
          { headers: { Authorization: `Bearer ${secret}` } },
        );
        if (checkRes.ok) {
          const j = (await checkRes.json()) as { active?: boolean; data?: { reason?: string }[] };
          if (j.active) {
            const reason = j.data?.[0]?.reason;
            return reply.status(403).send({
              error: reason
                ? `You are on compulsory leave: ${reason}. Check-in is not permitted during this period.`
                : "You are on compulsory leave. Check-in is not permitted during this period.",
              code: "HR_COMPULSORY_LEAVE",
            });
          }
        }
      }
    } catch {
      /* best-effort */
    }

    try {
      if (secret && internalUrl) {
        const checkRes = await fetch(
          `${internalUrl}/api/internal/approved-leave/active?employee=${encodeURIComponent(selfId)}&date=${encodeURIComponent(date)}`,
          { headers: { Authorization: `Bearer ${secret}` } },
        );
        if (checkRes.ok) {
          const j = (await checkRes.json()) as { active?: boolean };
          if (j.active) {
            return reply.status(403).send({
              error: "You have approved leave on this date. Site check-in is not available during approved leave.",
              code: "HR_ON_LEAVE",
            });
          }
        }
      }
    } catch {
      /* best-effort */
    }

    try {
      let siteDoc: Record<string, unknown>;
      try {
        siteDoc = await erp.getDoc(ctx.creds, "Client Site", clientSite);
      } catch (e) {
        if (e instanceof ErpError) return replyErp(reply, e);
        throw e;
      }
      if (String(siteDoc.company ?? "") !== ctx.company) {
        return reply.status(403).send({ error: "Site is not in your company" });
      }
      const siteLat =
        siteDoc.latitude != null && String(siteDoc.latitude).trim() !== "" ? Number(siteDoc.latitude) : NaN;
      const siteLng =
        siteDoc.longitude != null && String(siteDoc.longitude).trim() !== "" ? Number(siteDoc.longitude) : NaN;
      if (!Number.isFinite(siteLat) || !Number.isFinite(siteLng)) {
        return reply
          .status(422)
          .send({ error: "Client Site is missing latitude/longitude for geofence check" });
      }

      const distanceM = haversineMeters(lat, lng, siteLat, siteLng);
      if (distanceM > radiusM) {
        return reply.status(422).send({
          error: "Location is outside the site geofence",
          data: { distance_meters: Math.round(distanceM * 10) / 10, allowed_radius_meters: radiusM },
        });
      }

      if (requirePosting) {
        const assignments = await listSiteAssignmentsForDate(ctx, date);
        const ok = assignments.some(
          (a) => String(a.employee) === selfId && String(a.client_site ?? "") === clientSite,
        );
        if (!ok) {
          return reply.status(422).send({
            error: "No Site Assignment for this site on the selected date",
            data: { date, client_site: clientSite },
          });
        }
      }

      const now = new Date();
      const doc: Record<string, unknown> = {
        employee: selfId,
        log_type: "IN",
        time: toFrappeDateTime(now),
        latitude: lat,
        longitude: lng,
      };
      const acc = body.accuracy_meters;
      if (acc != null && Number.isFinite(Number(acc))) {
        doc.accuracy = Number(acc);
      }

      const created = await erp.createDoc(ctx.creds, "Employee Checkin", doc);
      return {
        data: {
          checkin: created,
          distance_meters: Math.round(distanceM * 10) / 10,
          radius_meters: radiusM,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /**
   * POST /v1/guard/attendance/exception-review-sync
   * Upsert Pay Hub resolution into ERP (`GUARD_EXCEPTION_REVIEW_DOCTYPE`). HR only.
   * Required Frappe fields: company, employee, attendance_date, exception_key, exception_type, resolution_status,
   * payhub_review_id, review_notes, reviewed_at, source; optional: client_site, site_assignment, first_in_time.
   */
  app.post("/v1/guard/attendance/exception-review-sync", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR access is required for exception review ERP sync" });
    }

    const body = (req.body ?? {}) as Record<string, unknown>;
    const payhubReviewId = String(body.payhub_review_id ?? "").trim();
    const attendanceDate = parseYmd(body.attendance_date) || "";
    const exceptionKey = String(body.exception_key ?? "").trim();
    const exceptionType = String(body.exception_type ?? "").trim();
    const employee = String(body.employee ?? "").trim();
    const resolutionStatus = String(body.resolution_status ?? "").trim().toLowerCase();
    if (!payhubReviewId || !attendanceDate || !exceptionKey || !exceptionType || !employee) {
      return reply.status(400).send({
        error:
          "payhub_review_id, attendance_date, exception_key, exception_type, and employee are required",
      });
    }
    if (!["approved", "rejected", "pending"].includes(resolutionStatus)) {
      return reply.status(400).send({ error: "resolution_status must be approved, rejected, or pending" });
    }

    const doctype = config.GUARD_EXCEPTION_REVIEW_DOCTYPE;
    const reviewNotes = String(body.review_notes ?? "").trim();
    let reviewedAt = String(body.reviewed_at ?? "").trim();
    if (!reviewedAt) reviewedAt = new Date().toISOString();
    const clientSite = String(body.client_site ?? "").trim();
    const assignmentName = String(body.assignment_name ?? "").trim();
    const firstInTime = String(body.first_in_time ?? "").trim();

    try {
      let rows = await erp.getList(ctx.creds, doctype, {
        filters: [
          ["company", "=", ctx.company],
          ["payhub_review_id", "=", payhubReviewId],
        ],
        fields: ["name"],
        limit_page_length: 1,
      });
      if (!rows.length) {
        rows = await erp.getList(ctx.creds, doctype, {
          filters: [
            ["company", "=", ctx.company],
            ["exception_key", "=", exceptionKey],
          ],
          fields: ["name"],
          limit_page_length: 1,
        });
      }

      const baseDoc: Record<string, unknown> = {
        company: ctx.company,
        employee,
        attendance_date: attendanceDate,
        exception_key: exceptionKey,
        exception_type: exceptionType,
        resolution_status: resolutionStatus,
        review_notes: reviewNotes,
        payhub_review_id: payhubReviewId,
        reviewed_at: reviewedAt,
        source: "Pay Hub",
      };
      if (clientSite) baseDoc.client_site = clientSite;
      if (assignmentName) baseDoc.site_assignment = assignmentName;
      if (firstInTime) baseDoc.first_in_time = firstInTime;

      let erpName: string;
      if (rows.length) {
        const name = String(asRecord(rows[0])?.name ?? "");
        if (!name) {
          return reply.status(500).send({ error: "ERP list returned an empty document name" });
        }
        await erp.updateDoc(ctx.creds, doctype, name, baseDoc);
        erpName = name;
      } else {
        const created = await erp.createDoc(ctx.creds, doctype, {
          ...baseDoc,
          doctype,
        });
        erpName = String(created.name ?? "");
      }

      return {
        data: {
          erp_doctype: doctype,
          erp_document_name: erpName,
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  /** --- Pass-through lists (ERP doctypes from `centy_guard`); empty on failure so UI stays usable. --- */

  app.get("/v1/guard/sites", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const activeOnly = String(q.active_only ?? "").toLowerCase() === "true";
    const lim = Math.min(500, Math.max(1, Number(q.limit) || 500));
    try {
      const filters: unknown[] = [["company", "=", ctx.company]];
      if (activeOnly) filters.push(["status", "=", "Active"]);
      const sites = (await erp.getList(ctx.creds, "Client Site", {
        filters,
        fields: [
          "name",
          "site_name",
          "customer",
          "status",
          "address",
          "assignment_code",
          "operations_supervisor",
          "site_grade_required",
          "latitude",
          "longitude",
        ],
        limit_page_length: lim,
        order_by: "modified desc",
      })) as Record<string, unknown>[];
      return { data: { sites } };
    } catch (e) {
      console.warn("[guard/sites]", e);
      return { data: { sites: [] } };
    }
  });

  app.get("/v1/guard/sites/form-options", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const employees = (await erp.getList(ctx.creds, "Employee", {
        filters: [
          ["company", "=", ctx.company],
          ["status", "=", "Active"],
        ],
        fields: ["name", "employee_name"],
        limit_page_length: 500,
        order_by: "employee_name asc",
      })) as Record<string, unknown>[];
      const customers = (await erp.getList(ctx.creds, "Customer", {
        filters: [["disabled", "=", 0]],
        fields: ["name", "customer_name"],
        limit_page_length: 500,
      })) as Record<string, unknown>[];
      return { data: { employees, customers } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/guard/sites", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR access required" });
    const body = (req.body ?? {}) as Record<string, unknown>;
    try {
      const doc: Record<string, unknown> = {
        doctype: "Client Site",
        company: ctx.company,
        site_name: String(body.site_name ?? "").trim(),
        customer: String(body.customer ?? "").trim(),
        operations_supervisor: String(body.operations_supervisor ?? "").trim(),
      };
      if (String(body.address ?? "").trim()) doc.address = String(body.address).trim();
      if (String(body.site_grade_required ?? "").trim()) doc.site_grade_required = String(body.site_grade_required).trim();
      if (body.latitude != null && body.longitude != null) {
        doc.latitude = Number(body.latitude);
        doc.longitude = Number(body.longitude);
      }
      const created = await erp.createDoc(ctx.creds, "Client Site", doc);
      return { data: { site: created } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/guard/patrol", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const from = parseYmd(q.from_date);
    const to = parseYmd(q.to_date);
    const lim = Math.min(500, Math.max(1, Number(q.limit) || 500));
    if (!from || !to) return reply.status(400).send({ error: "from_date and to_date are required" });
    try {
      const patrol_events = (await erp.getList(ctx.creds, "Patrol Event", {
        filters: [
          ["company", "=", ctx.company],
          ["event_datetime", ">=", `${from} 00:00:00`],
          ["event_datetime", "<=", `${to} 23:59:59`],
        ],
        fields: [
          "name",
          "event_datetime",
          "client_site",
          "employee",
          "checkpoint",
          "notes",
          "title",
        ],
        limit_page_length: lim,
        order_by: "event_datetime desc",
      })) as Record<string, unknown>[];
      return { data: { patrol_events } };
    } catch (e) {
      console.warn("[guard/patrol]", e);
      return { data: { patrol_events: [] } };
    }
  });

  app.get("/v1/guard/incidents", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const status = String(q.status ?? "").trim();
    const from = parseYmd(q.from_date);
    const to = parseYmd(q.to_date);
    const lim = Math.min(500, Math.max(1, Number(q.limit) || 500));
    try {
      const filters: unknown[] = [["company", "=", ctx.company]];
      if (status) filters.push(["status", "=", status]);
      if (from && to) {
        filters.push(["reported_datetime", ">=", `${from} 00:00:00`]);
        filters.push(["reported_datetime", "<=", `${to} 23:59:59`]);
      }
      const incidents = (await erp.getList(ctx.creds, "Guard Incident", {
        filters,
        fields: [
          "name",
          "title",
          "severity",
          "status",
          "reported_datetime",
          "client_site",
        ],
        limit_page_length: lim,
        order_by: "reported_datetime desc",
      })) as Record<string, unknown>[];
      return { data: { incidents } };
    } catch (e) {
      console.warn("[guard/incidents]", e);
      return { data: { incidents: [] } };
    }
  });

  app.get("/v1/guard/deployments", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const lim = Math.min(500, Math.max(1, Number((req.query as any)?.limit) || 500));
    try {
      const deployments = (await erp.getList(ctx.creds, "Site Assignment", {
        filters: [["company", "=", ctx.company]],
        fields: [
          "name",
          "employee",
          "client_site",
          "shift_pattern",
          "posting_date",
          "status",
          "modified",
        ],
        limit_page_length: lim,
        order_by: "modified desc",
      })) as Record<string, unknown>[];
      return { data: { deployments } };
    } catch (e) {
      console.warn("[guard/deployments]", e);
      return { data: { deployments: [] } };
    }
  });

  app.get("/v1/guard/assignments/form-options", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const clientSite = String(q.client_site ?? "").trim();
    try {
      const employees = (await erp.getList(ctx.creds, "Employee", {
        filters: [
          ["company", "=", ctx.company],
          ["status", "=", "Active"],
        ],
        fields: ["name", "employee_name"],
        limit_page_length: 500,
      })) as Record<string, unknown>[];
      const client_sites = (await erp.getList(ctx.creds, "Client Site", {
        filters: [["company", "=", ctx.company]],
        fields: ["name", "site_name"],
        limit_page_length: 500,
      })) as Record<string, unknown>[];
      let patterns: Record<string, unknown>[] = [];
      if (clientSite) {
        patterns = (await erp.getList(ctx.creds, "Site Shift Pattern", {
          filters: [["client_site", "=", clientSite]],
          fields: ["name", "pattern_name"],
          limit_page_length: 200,
        })) as Record<string, unknown>[];
      }
      return { data: { employees, client_sites, shift_patterns: patterns } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/guard/assignments", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR access required" });
    const body = (req.body ?? {}) as Record<string, unknown>;
    try {
      const doc: Record<string, unknown> = {
        doctype: "Site Assignment",
        company: ctx.company,
        employee: String(body.employee ?? "").trim(),
        client_site: String(body.client_site ?? "").trim(),
        shift_pattern: String(body.shift_pattern ?? "").trim(),
        posting_date: String(body.posting_date ?? "").trim(),
      };
      const created = await erp.createDoc(ctx.creds, "Site Assignment", doc);
      return { data: { assignment: created } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/guard/contracts", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const lim = Math.min(300, Math.max(1, Number((req.query as any)?.limit) || 300));
    try {
      const contracts = (await erp.getList(ctx.creds, "Site Contract", {
        filters: [["company", "=", ctx.company]],
        fields: [
          "name",
          "client_site",
          "start_date",
          "end_date",
          "coverage_type",
          "status",
          "billing_currency",
          "billing_frequency",
        ],
        limit_page_length: lim,
        order_by: "modified desc",
      })) as Record<string, unknown>[];
      return { data: { contracts } };
    } catch (e) {
      console.warn("[guard/contracts]", e);
      return { data: { contracts: [] } };
    }
  });

  app.get("/v1/guard/contracts/form-options", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const client_sites = (await erp.getList(ctx.creds, "Client Site", {
        filters: [["company", "=", ctx.company]],
        fields: ["name", "site_name"],
        limit_page_length: 500,
      })) as Record<string, unknown>[];
      const currencies = (await erp.getList(ctx.creds, "Currency", {
        fields: ["name"],
        limit_page_length: 50,
      })) as Record<string, unknown>[];
      return { data: { client_sites, currencies } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.post("/v1/guard/contracts", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR access required" });
    const body = (req.body ?? {}) as Record<string, unknown>;
    try {
      const doc: Record<string, unknown> = {
        doctype: "Site Contract",
        company: ctx.company,
        client_site: String(body.client_site ?? "").trim(),
        start_date: String(body.start_date ?? "").trim(),
        coverage_type: String(body.coverage_type ?? "").trim(),
        billing_currency: String(body.billing_currency ?? "").trim(),
        billing_frequency: String(body.billing_frequency ?? "").trim(),
        status: String(body.status ?? "Draft").trim(),
        auto_invoice: body.auto_invoice === true || body.auto_invoice === "true" ? 1 : 0,
      };
      if (String(body.end_date ?? "").trim()) doc.end_date = String(body.end_date).trim();
      const created = await erp.createDoc(ctx.creds, "Site Contract", doc);
      return { data: { contract: created } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/guard/shifts", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const q = (req.query ?? {}) as Record<string, unknown>;
    const start = parseYmd(q.start_date);
    const end = parseYmd(q.end_date);
    if (!start || !end) return reply.status(400).send({ error: "start_date and end_date are required" });
    try {
      const raw = (await erp.getList(ctx.creds, "Shift Assignment", {
        filters: [
          ["company", "=", ctx.company],
          ["start_date", "<=", end],
        ],
        fields: ["name", "employee", "shift_type", "start_date", "end_date"],
        limit_page_length: 500,
        order_by: "start_date desc",
      })) as Record<string, unknown>[];
      const shifts = raw.filter((r) => {
        const ed = r.end_date == null || String(r.end_date).trim() === "" ? null : String(r.end_date).slice(0, 10);
        if (ed && ed < start) return false;
        return true;
      });
      return { data: { shifts } };
    } catch (e) {
      console.warn("[guard/shifts]", e);
      return { data: { shifts: [] } };
    }
  });

  app.get("/v1/guard/service-items", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    const lim = Math.min(300, Math.max(1, Number((req.query as any)?.limit) || 300));
    try {
      const items = (await erp.getList(ctx.creds, "Item", {
        filters: [["item_group", "=", "CentyGuard Services"]],
        fields: ["name", "item_name", "item_code", "description"],
        limit_page_length: lim,
      })) as Record<string, unknown>[];
      return { data: { items } };
    } catch (e) {
      console.warn("[guard/service-items]", e);
      return { data: { items: [] } };
    }
  });

  app.post("/v1/guard/service-items", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR access required" });
    const body = (req.body ?? {}) as Record<string, unknown>;
    try {
      const doc: Record<string, unknown> = {
        doctype: "Item",
        item_code: String(body.item_code ?? "").trim(),
        item_name: String(body.item_name ?? "").trim(),
        item_group: "CentyGuard Services",
        is_stock_item: 0,
      };
      if (String(body.description ?? "").trim()) doc.description = String(body.description).trim();
      const created = await erp.createDoc(ctx.creds, "Item", doc);
      return { data: { item: created } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });

  app.get("/v1/guard/payments/batch-preview", async (req, reply) => {
    let ctx: HrContext;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR access required" });
    const q = (req.query ?? {}) as Record<string, unknown>;
    const ps = parseYmd(q.period_start);
    const pe = parseYmd(q.period_end);
    if (!ps || !pe) return reply.status(400).send({ error: "period_start and period_end are required" });
    try {
      const employees = await listActiveEmployees(ctx, ["grade"]);
      const shifts = (await erp.getList(ctx.creds, "Shift Assignment", {
        filters: [
          ["company", "=", ctx.company],
          ["start_date", "<=", pe],
          ["docstatus", "!=", 2],
        ],
        fields: ["employee", "start_date", "end_date"],
        limit_page_length: 5000,
      })) as Record<string, unknown>[];

      const inPeriod = (r: Record<string, unknown>): boolean => {
        const sd = String(r.start_date ?? "").slice(0, 10);
        const ed = r.end_date == null || String(r.end_date).trim() === "" ? "" : String(r.end_date).slice(0, 10);
        if (!sd) return false;
        if (sd > pe) return false;
        if (!ed) return sd <= pe && sd >= ps;
        if (ed < ps) return false;
        return true;
      };

      const shiftCount = new Map<string, number>();
      for (const r of shifts) {
        if (!inPeriod(r)) continue;
        const emp = String(r.employee ?? "");
        if (!emp) continue;
        shiftCount.set(emp, (shiftCount.get(emp) ?? 0) + 1);
      }

      const list = employees.map((e) => {
        const id = String(e.name ?? "");
        return {
          employee_name: String(e.employee_name ?? e.name ?? id),
          psra_grade: psraGradeFromEmployee(e) || String(e.grade ?? ""),
          shift_assignments_in_period: shiftCount.get(id) ?? 0,
        };
      });

      return {
        data: {
          employees: list,
          totals: {
            employees_considered: list.length,
            shift_assignment_rows_in_period: shifts.filter(inPeriod).length,
          },
        },
      };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
