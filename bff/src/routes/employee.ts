import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import multipart from "@fastify/multipart";
import * as appConfig from "../config.js";
import { defaultClient } from "../erpnext/client.js";
import { ErpError } from "../erpnext/client.js";
import {
  validateEmployeeDoc,
  validateKraPin,
  normalizeKraPin,
  normalizePhone,
  normalizeEmail,
} from "../lib/employeeValidation.js";
import { parseFrappeErrorBody, publicErpFailure } from "../erpnext/frappeResponse.js";
import { resolveHrContext, HttpError } from "../context/resolveHrContext.js";
import { insertEmployeeInvite, invitesAvailable } from "../lib/employeeInvites.js";
import type { HrContext } from "../types.js";

const erp = defaultClient();

// ── Simple in-process cache for expensive ERPNext list/summary calls ─────────
// Key: `${company}:${cacheKey}`, Value: { data: unknown; exp: number }
const ERP_CACHE_TTL_MS = Number(process.env.ERP_CACHE_TTL_MS ?? 30_000); // 30 s default
const _erpCache = new Map<string, { data: unknown; exp: number }>();
function erpCacheGet<T>(key: string): T | null {
  const entry = _erpCache.get(key);
  if (!entry || entry.exp < Date.now()) { _erpCache.delete(key); return null; }
  return entry.data as T;
}
function erpCacheSet(key: string, data: unknown): void {
  _erpCache.set(key, { data, exp: Date.now() + ERP_CACHE_TTL_MS });
}
/** Bust all cache entries for a company (call after create/update/delete). */
function erpCacheBust(company: string): void {
  for (const k of _erpCache.keys()) {
    if (k.startsWith(`${company}:`)) _erpCache.delete(k);
  }
}

function replyErp(reply: FastifyReply, e: ErpError) {
  const status = e.status >= 500 ? 502 : e.status;
  return reply.status(status).send(publicErpFailure(e));
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

const SUMMARY_SCAN_CAP = 5000;
/** Max expense claim rows scanned for per-employee aggregates (snapshot + heatmap). */
const EMPLOYEE_INSIGHTS_CLAIM_CAP = 500;
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
): Promise<{ ok: true; doc: Record<string, unknown> } | { ok: false; status: number; error: string }> {
  const r = await loadEmployeeReadableByCaller(ctx, employeeId);
  if (!r.ok) return r;
  return { ok: true, doc: r.doc };
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
  "date_of_birth",
  "date_of_joining",
  "department",
  "designation",
  "branch",
  "cell_number",
  "prefered_email",
  "company_email",
  "personal_email",
  "reports_to",
  // Identity / statutory (can be set at onboarding)
  "tax_id",         // KRA PIN
  "id_number",      // National ID (custom field — silently skipped if not present on ERP)
  "salutation",
  "employment_type",
  "grade",
]);

const EMPLOYEE_LIST_FIELDS = [
  "name",
  "employee_name",
  "creation",
  "image",
  "department",
  "designation",
  "branch",
  "employment_type",
  "grade",
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
  // Identity
  "salutation", "first_name", "last_name", "image",
  // Contact
  "cell_number", "prefered_email", "personal_email", "company_email",
  "expense_approver", "current_address", "permanent_address",
  // Employment
  "department", "designation", "branch", "reports_to", "employment_type", "grade",
  "gender", "date_of_birth", "date_of_joining", "marital_status", "blood_group",
  "nationality", "notice_number_of_days",
  // Joining
  "job_applicant", "scheduled_confirmation_date", "final_confirmation_date",
  "contract_end_date", "date_of_retirement",
  // Personal
  "family_background", "health_details", "health_insurance_provider",
  "passport_number", "valid_upto", "date_of_issue", "place_of_issue", "bio",
  // Attendance & Leaves
  "attendance_device_id", "holiday_list", "default_shift",
  "leave_approver", "shift_request_approver",
  // Payroll & Banking
  "ctc", "payroll_cost_center", "salary_mode", "salary_currency",
  "bank_name", "bank_ac_no", "iban",
  // Exit
  "resignation_letter_date", "relieving_date", "held_on", "new_workplace",
  "leave_encashed", "reason_for_leaving", "feedback",
  // Statutory / Tax compliance
  "tax_id",                                          // KRA PIN (standard ERPNext field)
  // Common custom field name variants for Kenya statutory numbers:
  "nssf_no", "custom_nssf_no", "nssf_number", "custom_nssf_number",
  "nhif_no", "custom_nhif_no", "nhif_number", "custom_nhif_number",
  "shif_no", "custom_shif_no", "shif_number", "custom_shif_number",
  "nita_no", "custom_nita_no", "nita_number", "custom_nita_number",
  "kra_pin", "custom_kra_pin",
]);

const EMPLOYEE_DYNAMIC_STATUTORY_FIELD_RE = /^(custom_)?(nssf|nhif|shif|nita|kra)(_[a-z0-9]+)*$/i;

function isAllowedEmployeePatchField(field: string): boolean {
  return EMPLOYEE_PATCH_WHITELIST.has(field) || EMPLOYEE_DYNAMIC_STATUTORY_FIELD_RE.test(field);
}

function isDynamicStatutoryPatchField(field: string): boolean {
  return EMPLOYEE_DYNAMIC_STATUTORY_FIELD_RE.test(field) && !EMPLOYEE_PATCH_WHITELIST.has(field);
}


const PHOTO_MAX_BYTES = 5 * 1024 * 1024; // 5 MB
const PHOTO_MIME_RE = /^image\/(jpeg|png|webp|gif)$/;

export const employeeRoutes: FastifyPluginAsync = async (app) => {
  await app.register(multipart, { limits: { fileSize: PHOTO_MAX_BYTES, files: 1 } });
  /** Bio / master data for the logged-in user's Employee row (same Company). */
  app.get("/v1/me/employee", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    async function resolveCompanyDocName(): Promise<string> {
      const raw = String(ctx!.company ?? "").trim();
      if (!raw) return raw;

      // Fast path: company is already the ERP docname
      try {
        await erp.getDoc(ctx!.creds, "Company", raw);
        return raw;
      } catch (e) {
        if (!(e instanceof ErpError)) throw e;
      }

      // Fallback: look up by the `company_name` field (docname may differ)
      try {
        const rows = await erp.getList(ctx!.creds, "Company", {
          filters: [["company_name", "=", raw]],
          fields: ["name", "company_name"],
          limit_page_length: 1,
        });
        const found = rows?.[0] as any;
        const name = String(found?.name ?? "").trim();
        return name || raw;
      } catch {
        return raw;
      }
    }

    const companyDocName = await resolveCompanyDocName();

    // Cache per user+company (30 s) — avoids hitting ERPNext on every page load
    const cacheKey = `${companyDocName}:me:${ctx.userEmail}`;
    const cached = erpCacheGet<{ data: unknown }>(cacheKey);
    if (cached) return cached;

    try {
      /**
       * Resolve the employee docname for this user.
       * Uses ["name"] only — avoids 417s from fields that don't exist in the installation.
       */
      async function resolveEmployeeName(field: "user_id" | "personal_email"): Promise<string | null> {
        try {
          const rows = await erp.getList(ctx!.creds, "Employee", {
            filters: [[field, "=", ctx!.userEmail], ["company", "=", companyDocName]],
            fields: ["name"],
            limit_page_length: 1,
          });
          const row = rows?.[0] as { name?: string } | undefined;
          return row?.name ? String(row.name) : null;
        } catch (e) {
          if (e instanceof ErpError && e.status >= 500) {
            const res = await erp.listDocs(ctx!.creds, "Employee", {
              filters: [[field, "=", ctx!.userEmail], ["company", "=", companyDocName]],
              fields: ["name"],
              limit_page_length: 1,
            });
            const row = res.data?.[0] as { name?: string } | undefined;
            return row?.name ? String(row.name) : null;
          }
          throw e;
        }
      }

      // Primary: look up by user_id; fallback: personal_email
      let empName = await resolveEmployeeName("user_id");
      if (!empName) empName = await resolveEmployeeName("personal_email");

      if (!empName) {
        return reply.status(404).send({
          error: "No employee record for your account in this company.",
          code: "HR_NO_EMPLOYEE",
          company: companyDocName,
        });
      }

      // getDoc returns all fields the doctype has — no field whitelist, never throws "Field not permitted"
      const doc = (await erp.getDoc(ctx.creds, "Employee", empName)) as Record<string, unknown>;
      const { company: _c, ...data } = doc;
      const result = { data };
      erpCacheSet(cacheKey, result);
      return result;
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

  /**
   * Admin helper: ensure an Employee exists for the caller's email (in this Company).
   * This is used by Pay Hub to "auto-link" the admin to an Employee record without ERP UI access.
   */
  app.post("/v1/me/employee/ensure", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required" });
    }

    async function resolveCompanyDocName(): Promise<string> {
      const raw = String(ctx!.company ?? "").trim();
      if (!raw) return raw;
      try {
        await erp.getDoc(ctx!.creds, "Company", raw);
        return raw;
      } catch (e) {
        if (!(e instanceof ErpError)) throw e;
      }
      try {
        const rows = await erp.getList(ctx!.creds, "Company", {
          filters: [["company_name", "=", raw]],
          fields: ["name", "company_name"],
          limit_page_length: 1,
        });
        const found = rows?.[0] as any;
        const name = String(found?.name ?? "").trim();
        return name || raw;
      } catch {
        return raw;
      }
    }

    const companyDocName = await resolveCompanyDocName();
    const email = String(ctx.userEmail || "").trim();
    if (!email) return reply.status(400).send({ error: "Missing email context" });

    const fetchExisting = async (): Promise<Record<string, unknown> | null> => {
      for (const field of ["user_id", "personal_email"] as const) {
        try {
          // Fetch only name to avoid "Field not permitted" errors from non-standard fields
          const rows = await erp.getList(ctx!.creds, "Employee", {
            filters: [[field, "=", email], ["company", "=", companyDocName]],
            fields: ["name"],
            limit_page_length: 1,
          });
          const row = rows?.[0] as { name?: string } | undefined;
          if (row?.name) {
            // getDoc returns all fields the doctype has without a whitelist
            return (await erp.getDoc(ctx!.creds, "Employee", String(row.name))) as Record<string, unknown>;
          }
        } catch (e) {
          if (!(e instanceof ErpError)) throw e;
        }
      }
      return null;
    };

    // If already exists, return it (idempotent)
    const existing = await fetchExisting();
    if (existing) {
      return { data: existing };
    }

    const localPart = email.split("@")[0] || email;
    const firstName = localPart.replace(/[._-]+/g, " ").trim().slice(0, 140) || "Admin";
    const todayIso = new Date().toISOString().slice(0, 10);
    // Default DOB to 25 years before DOJ — ERPNext requires both gender and date_of_birth
    const dobDefault = new Date(todayIso);
    dobDefault.setFullYear(dobDefault.getFullYear() - 25);
    const baseDoc = {
      company: companyDocName,
      first_name: firstName,
      last_name: "",
      employee_name: firstName,
      date_of_joining: todayIso,
      date_of_birth: dobDefault.toISOString().slice(0, 10),
      gender: "Male",
      status: "Active",
      personal_email: email,
    };

    const isMissingUserIdLink = (e: ErpError): boolean => {
      const hint = parseFrappeErrorBody(e.body);
      const h = String(hint ?? "").toLowerCase();
      return h.includes("could not find user id");
    };

    try {
      let created: unknown;
      try {
        // Preferred: attach `user_id` when ERP User exists.
        created = await erp.createDoc(ctx.creds, "Employee", {
          ...baseDoc,
          user_id: email,
        });
      } catch (inner) {
        if (!(inner instanceof ErpError)) throw inner;
        if (isMissingUserIdLink(inner)) {
          // Fallback for tenants where ERP User isn't provisioned for the login email yet.
          created = await erp.createDoc(ctx.creds, "Employee", baseDoc);
        } else {
          throw inner;
        }
      }
      const createdName = String((created as any)?.name ?? "").trim();
      if (!createdName) return { data: { created: true } };
      const doc = await erp.getDoc(ctx.creds, "Employee", createdName);
      return { data: doc };
    } catch (e) {
      if (e instanceof ErpError) {
        // If another concurrent request created/updated the employee, return the now-existing row.
        if (e.status === 409 || e.status === 417) {
          const row = await fetchExisting();
          if (row) return { data: row };
        }
        const status = e.status >= 500 ? 502 : e.status;
        return reply.status(status).send(publicErpFailure(e));
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
      if (!isAllowedEmployeePatchField(k)) continue;
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

  /**
   * Colleagues with a Frappe `user_id` in this Company — suitable values for `Employee.expense_approver` (User link).
   * Scoped to ERP; Pay Hub does not maintain a parallel user list.
   */
  app.get("/v1/meta/expense-approver-users", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }

    const raw = (req.query ?? {}) as Record<string, unknown>;
    const q = String(raw.q ?? "").trim().slice(0, 120);
    const limitRaw = parseInt(String(raw.limit ?? "30"), 10) || 30;
    const limit = Math.min(50, Math.max(5, limitRaw));
    const esc = q.replace(/\\/g, "\\\\").replace(/%/g, "\\%");
    const like = esc ? `%${esc}%` : "";

    try {
      // Base AND filters always applied
      const filters: unknown[] = [
        ["company", "=", ctx.company],
        ["user_id", "!=", ""],
      ];
      // OR filters for search — frappe.client.get_list combines these as
      // WHERE (base filters AND) AND (or1 OR or2 OR or3)
      const or_filters: unknown[] = like
        ? [
            ["employee_name", "like", like],
            ["user_id", "like", like],
            ["name", "like", like],
          ]
        : [];

      const rows = await erp.getList(ctx.creds, "Employee", {
        filters,
        or_filters: or_filters.length ? or_filters : undefined,
        fields: ["name", "employee_name", "user_id", "status"],
        order_by: "employee_name asc",
        limit_page_length: limit + 5,
      });
      const seen = new Set<string>();
      const data: { value: string; label: string; employee_id: string }[] = [];
      for (const r of (rows as unknown[])) {
        const rec = asRecord(r);
        if (!rec) continue;
        const uid = String(rec.user_id ?? "").trim();
        if (!uid || seen.has(uid)) continue;
        seen.add(uid);
        const label = String(rec.employee_name ?? "").trim() || uid;
        data.push({
          value: uid,
          label,
          employee_id: String(rec.name ?? ""),
        });
        if (data.length >= limit) break;
      }
      return { data };
    } catch (e) {
      if (e instanceof ErpError) {
        console.warn("[hr] meta/expense-approver-users ERP error:", e.status, e.body);
        return { data: [] as { value: string; label: string; employee_id: string }[] };
      }
      throw e;
    }
  });

  /** Names from ERP `Employment Type` (Link target for `Employee.employment_type` on standard HR sites). */
  app.get("/v1/meta/employment-types", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const rows = await erp.getList(ctx.creds, "Employment Type", {
        fields: ["name"],
        limit_page_length: 100,
      });
      const names = (rows as { name?: string }[])
        .map((r) => String(r.name ?? "").trim())
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b));
      return { data: names.map((name) => ({ name })) };
    } catch (e) {
      if (e instanceof ErpError) {
        console.warn("[hr] meta/employment-types:", e.status, e.body);
        return { data: [] as { name: string }[] };
      }
      throw e;
    }
  });

  /** Names from ERP `Employee Grade` (common Link target for `Employee.grade` in HR). */
  app.get("/v1/meta/employee-grades", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const rows = await erp.getList(ctx.creds, "Employee Grade", {
        fields: ["name"],
        limit_page_length: 100,
      });
      const names = (rows as { name?: string }[])
        .map((r) => String(r.name ?? "").trim())
        .filter(Boolean)
        .sort((a, b) => a.localeCompare(b));
      return { data: names.map((name) => ({ name })) };
    } catch (e) {
      if (e instanceof ErpError) {
        console.warn("[hr] meta/employee-grades:", e.status, e.body);
        return { data: [] as { name: string }[] };
      }
      throw e;
    }
  });

  /** Enabled currencies from the HR directory (for salary_currency picker). */
  app.get("/v1/meta/currencies", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const rows = await erp.getList(ctx.creds, "Currency", {
        filters: [["enabled", "=", 1]],
        fields: ["name", "currency_name"],
        order_by: "name asc",
        limit_page_length: 300,
      });
      const data = (rows as { name?: string; currency_name?: string }[])
        .map((r) => ({
          code: String(r.name ?? "").trim(),
          label: String(r.currency_name ?? "").trim(),
        }))
        .filter((r) => r.code);
      return { data };
    } catch (e) {
      if (e instanceof ErpError) {
        console.warn("[hr] meta/currencies:", e.status, e.body);
        return { data: [] as { code: string; label: string }[] };
      }
      throw e;
    }
  });

  /**
   * Custom fields defined on the Employee doctype.
   * Lets the UI discover what statutory fields (NSSF, NHIF, SHIF, NITA …) are installed
   * without hard-coding field names that differ between ERPNext deployments.
   */
  app.get("/v1/meta/employee-custom-fields", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    try {
      const rows = await erp.getList(ctx.creds, "Custom Field", {
        filters: [["dt", "=", "Employee"]],
        fields: ["fieldname", "label", "fieldtype", "options", "insert_after"],
        order_by: "idx asc",
        limit_page_length: 300,
      });
      return { data: rows as { fieldname: string; label: string; fieldtype: string; options?: string; insert_after?: string }[] };
    } catch (e) {
      if (e instanceof ErpError) {
        // Custom Field may not be accessible with some permission sets — return empty gracefully
        console.warn("[hr] meta/employee-custom-fields:", e.status, e.body);
        return { data: [] as { fieldname: string; label: string; fieldtype: string }[] };
      }
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

    const summaryKey = `${ctx.company}:summary`;
    const cachedSummary = erpCacheGet<unknown>(summaryKey);
    if (cachedSummary) return cachedSummary;

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
      const result = {
        data: {
          total,
          active,
          inactive,
          left,
          scan_capped: rows.length >= SUMMARY_SCAN_CAP,
        },
      };
      erpCacheSet(summaryKey, result);
      return result;
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
    if (!first) return reply.status(400).send({ error: "first_name is required" });
    const last = String(doc.last_name ?? "").trim();
    doc.first_name = first;
    doc.last_name = last;
    doc.employee_name = last ? `${first} ${last}`.trim() : first;

    const rawGender = String(doc.gender ?? "").trim().toLowerCase();
    if (rawGender === "m" || rawGender === "male") doc.gender = "Male";
    else if (rawGender === "f" || rawGender === "female") doc.gender = "Female";
    else if (!rawGender) doc.gender = "Male"; // ERPNext mandatory field — default when not supplied

    const doj = String(doc.date_of_joining ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(doj)) {
      doc.date_of_joining = new Date().toISOString().slice(0, 10);
    }

    const dob = String(doc.date_of_birth ?? "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dob)) {
      const dobDefault = new Date(doc.date_of_joining as string);
      dobDefault.setFullYear(dobDefault.getFullYear() - 25);
      doc.date_of_birth = dobDefault.toISOString().slice(0, 10);
    }

    // ── Normalize & validate identity / contact fields ───────────────────────
    if (doc.cell_number) doc.cell_number = normalizePhone(String(doc.cell_number));
    if (doc.prefered_email) doc.prefered_email = normalizeEmail(String(doc.prefered_email));
    if (doc.company_email) doc.company_email = normalizeEmail(String(doc.company_email));
    if (doc.personal_email) doc.personal_email = normalizeEmail(String(doc.personal_email));
    if (doc.tax_id) doc.tax_id = normalizeKraPin(String(doc.tax_id));

    const fieldErrors = validateEmployeeDoc(doc);
    if (fieldErrors.length > 0) {
      return reply.status(422).send({
        error: "Validation failed",
        fields: fieldErrors,
        // Also expose as flat message for clients that only read .error
        message: fieldErrors.map((e) => e.message).join("; "),
      });
    }

    // ── Duplicate detection — check by KRA PIN and phone within company ───────
    const duplicateFilters: Array<{ field: string; value: string; label: string }> = [];
    if (doc.tax_id) duplicateFilters.push({ field: "tax_id",      value: String(doc.tax_id),      label: "KRA PIN"       });
    if (doc.cell_number) duplicateFilters.push({ field: "cell_number", value: String(doc.cell_number), label: "phone number"  });
    if (doc.company_email) duplicateFilters.push({ field: "company_email", value: String(doc.company_email), label: "company email" });
    if (doc.id_number) duplicateFilters.push({ field: "id_number", value: String(doc.id_number), label: "National ID" });

    for (const { field, value, label } of duplicateFilters) {
      try {
        const existing = await erp.getList(ctx.creds, "Employee", {
          filters: [[field, "=", value], ["company", "=", ctx.company]],
          fields: ["name", "employee_name"],
          limit_page_length: 1,
        });
        if (Array.isArray(existing) && existing.length > 0) {
          const dup = existing[0] as Record<string, unknown>;
          return reply.status(409).send({
            error: `An employee with this ${label} already exists in this company`,
            code: "DUPLICATE_EMPLOYEE",
            duplicate: { id: String(dup.name ?? ""), name: String(dup.employee_name ?? "") },
            field,
          });
        }
      } catch {
        // If ERP field doesn't exist (e.g. custom id_number field not installed),
        // skip that duplicate check rather than blocking creation.
      }
    }

    try {
      const created = await erp.createDoc(ctx.creds, "Employee", doc);
      erpCacheBust(ctx.company);
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
      const q = parseSearchQuery(req);
      const listCacheKey = `${ctx.company}:list:p${page}:ps${pageSize}:q${q}`;
      const cachedList = erpCacheGet<unknown>(listCacheKey);
      if (cachedList) return cachedList;
      const take = pageSize + 1;
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
          order_by: "creation desc, employee_name asc",
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
            order_by: "creation desc, employee_name asc",
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
      const listResult = {
        data,
        meta: { page, page_size: pageSize, has_more: hasMore, q: q || undefined },
      };
      erpCacheSet(listCacheKey, listResult);
      return listResult;
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
      // Reuse the Employee doc already fetched by the access gate — no second getDoc needed.
      // All 4 count queries run in parallel.
      const left = normalizeStatus(gate.doc.status) === "left" ? 1 : 0;
      const [claims, advances, leaveApps, leaveAllocs] = await Promise.all([
        cappedCount("Expense Claim", [
          ["company", "=", ctx.company],
          ["employee", "=", name],
        ]),
        cappedCount("Employee Advance", [
          ["company", "=", ctx.company],
          ["employee", "=", name],
        ]),
        cappedCount("Leave Application", [
          ["company", "=", ctx.company],
          ["employee", "=", name],
        ]),
        cappedCount("Leave Allocation", [
          ["company", "=", ctx.company],
          ["employee", "=", name],
        ]),
      ]);

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

  /**
   * GET /v1/employees/:id/photo — proxy employee's profile image from ERPNext.
   * Returns the raw image bytes with the correct Content-Type so the browser
   * can display it via <img src="/api/hr/v1/employees/{id}/photo">.
   */
  app.get("/v1/employees/:id/photo", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }

    const name = (req.params as { id: string }).id;
    const access = await loadEmployeeReadableByCaller(ctx, name);
    if (!access.ok) return reply.status(access.status).send({ error: access.error });

    const imageField = String(access.doc.image ?? "").trim();
    if (!imageField) return reply.status(404).send({ error: "No photo set" });

    // imageField is a Frappe file path like /files/photo.jpg or /private/files/photo.jpg
    const imageUrl = imageField.startsWith("http") ? imageField : `${appConfig.ERP_BASE_URL}${imageField}`;

    try {
      const res = await fetch(imageUrl, {
        headers: { Authorization: `token ${ctx.creds.apiKey}:${ctx.creds.apiSecret}` },
      });
      if (!res.ok) return reply.status(404).send({ error: "Photo not found" });
      const contentType = res.headers.get("content-type") ?? "image/jpeg";
      const buf = Buffer.from(await res.arrayBuffer());
      reply.header("Content-Type", contentType);
      reply.header("Cache-Control", "private, max-age=300");
      return reply.send(buf);
    } catch {
      return reply.status(502).send({ error: "Could not fetch photo from HR directory" });
    }
  });

  /**
   * POST /v1/employees/:id/photo — upload a profile photo (multipart/form-data, field name "file").
   * Stores in ERPNext and sets Employee.image to the uploaded file URL.
   */
  app.post("/v1/employees/:id/photo", async (req, reply) => {
    let ctx;
    try { ctx = resolveHrContext(req); }
    catch (e) { if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message }); throw e; }
    if (!ctx.canSubmitOnBehalf) return reply.status(403).send({ error: "HR admin privileges required" });

    const name = (req.params as { id: string }).id;
    try {
      const emp = await erp.getDoc(ctx.creds, "Employee", name) as Record<string, unknown>;
      if (String(emp.company) !== ctx.company) return reply.status(403).send({ error: "Employee not in your company" });
    } catch (e) {
      if (e instanceof ErpError) return reply.status(404).send({ error: "Employee not found" });
      throw e;
    }

    let fileBuffer: Buffer;
    let fileName: string;
    let contentType: string;
    try {
      const data = await req.file();
      if (!data) return reply.status(400).send({ error: "No file provided" });
      fileName = data.filename || "photo.jpg";
      contentType = data.mimetype || "image/jpeg";
      if (!PHOTO_MIME_RE.test(contentType)) {
        return reply.status(415).send({ error: "Only JPEG, PNG, WebP, or GIF images are allowed." });
      }
      const chunks: Buffer[] = [];
      let total = 0;
      for await (const chunk of data.file) {
        total += (chunk as Buffer).length;
        if (total > PHOTO_MAX_BYTES) return reply.status(413).send({ error: "Photo must be under 5 MB." });
        chunks.push(chunk as Buffer);
      }
      fileBuffer = Buffer.concat(chunks);
    } catch {
      return reply.status(400).send({ error: "Could not read the uploaded photo." });
    }

    try {
      const uploaded = await erp.uploadFile(ctx.creds, {
        buffer: fileBuffer,
        filename: fileName,
        contentType,
        isPrivate: false,
        doctype: "Employee",
        docname: name,
        fieldname: "image",
      });
      const u = uploaded as Record<string, unknown>;
      const fileDoc = (u.message as Record<string, unknown>) ?? u;
      const fileUrl = String(fileDoc.file_url ?? "");
      // Persist the image URL on the Employee record
      if (fileUrl) {
        await erp.updateDoc(ctx.creds, "Employee", name, { image: fileUrl });
      }
      return { data: { image: fileUrl } };
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
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
      if (!isAllowedEmployeePatchField(k)) continue;
      if (v === null || v === undefined) continue;
      patch[k] = typeof v === "string" ? v.trim() : v;
    }
    if (Object.keys(patch).length === 0) {
      return reply.status(400).send({ error: "No allowed fields to update" });
    }

    // Normalize identifiers / contact fields before write
    if (patch.cell_number) patch.cell_number = normalizePhone(String(patch.cell_number));
    if (patch.prefered_email) patch.prefered_email = normalizeEmail(String(patch.prefered_email));
    if (patch.company_email) patch.company_email = normalizeEmail(String(patch.company_email));
    if (patch.personal_email) patch.personal_email = normalizeEmail(String(patch.personal_email));
    if (patch.tax_id) patch.tax_id = normalizeKraPin(String(patch.tax_id));

    const fieldErrors = validateEmployeeDoc(patch);
    if (fieldErrors.length > 0) {
      return reply.status(422).send({
        error: "Validation failed",
        fields: fieldErrors,
        message: fieldErrors.map((e) => e.message).join("; "),
      });
    }

    // Uniqueness check — only for fields that are being changed
    const uniqueChecks: Array<{ field: string; value: string; label: string }> = [];
    if (patch.tax_id) uniqueChecks.push({ field: "tax_id", value: String(patch.tax_id), label: "KRA PIN" });
    if (patch.cell_number) uniqueChecks.push({ field: "cell_number", value: String(patch.cell_number), label: "phone number" });
    if (patch.company_email) uniqueChecks.push({ field: "company_email", value: String(patch.company_email), label: "company email" });
    for (const { field, value, label } of uniqueChecks) {
      try {
        const existing = await erp.getList(ctx.creds, "Employee", {
          filters: [[field, "=", value], ["company", "=", ctx.company], ["name", "!=", name]],
          fields: ["name", "employee_name"],
          limit_page_length: 1,
        });
        if (Array.isArray(existing) && existing.length > 0) {
          const dup = existing[0] as Record<string, unknown>;
          return reply.status(409).send({
            error: `Another employee in this company already has this ${label}`,
            code: "DUPLICATE_EMPLOYEE",
            duplicate: { id: String(dup.name ?? ""), name: String(dup.employee_name ?? "") },
            field,
          });
        }
      } catch { /* skip if field doesn't exist in ERP */ }
    }

    try {
      // Split patch: custom/statutory fields go directly via set_value (more reliable than PUT
      // for ERPNext custom fields); standard fields go via updateDoc (PUT).
      const isCustomOrStatutory = (field: string) =>
        field.startsWith("custom_") || EMPLOYEE_DYNAMIC_STATUTORY_FIELD_RE.test(field);

      const standardPatch: Record<string, unknown> = {};
      const statutoryPatch: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(patch)) {
        if (isCustomOrStatutory(k)) {
          statutoryPatch[k] = v;
        } else {
          standardPatch[k] = v;
        }
      }

      // Apply standard fields via PUT
      if (Object.keys(standardPatch).length > 0) {
        await erp.updateDoc(ctx.creds, "Employee", name, standardPatch);
      }

      // Apply custom/statutory fields one-by-one via frappe.client.set_value
      // (PUT /api/resource silently drops custom fields on some ERPNext versions)
      for (const [field, value] of Object.entries(statutoryPatch)) {
        console.warn(`[hr:patch] set_value field="${field}" value="${String(value).slice(0, 40)}"`);
        try {
          await erp.callMethod(ctx.creds, "frappe.client.set_value", {
            doctype: "Employee",
            name,
            fieldname: field,
            value,
          });
        } catch (svErr) {
          console.warn(`[hr:patch] set_value failed for field="${field}":`, String(svErr).slice(0, 200));
        }
      }

      // Fetch the updated record and verify statutory fields were persisted
      let record = (await erp.getDoc(ctx.creds, "Employee", name)) as Record<string, unknown>;

      // Second-pass: any statutory field still mismatched → retry set_value once more
      const stillMismatched = Object.entries(statutoryPatch).filter(([field, value]) => {
        const saved = String(record[field] ?? "").trim();
        const expected = typeof value === "string" ? value.trim() : String(value ?? "").trim();
        const mismatch = saved !== expected;
        if (mismatch) console.warn(`[hr:patch] mismatch field="${field}" saved="${saved}" expected="${expected}"`);
        return mismatch;
      });

      if (stillMismatched.length > 0) {
        console.warn(`[hr:patch] ${stillMismatched.length} field(s) still mismatched after set_value — retrying`);
        for (const [field, value] of stillMismatched) {
          try {
            await erp.callMethod(ctx.creds, "frappe.client.set_value", {
              doctype: "Employee",
              name,
              fieldname: field,
              value,
            });
          } catch (svErr) {
            console.warn(`[hr:patch] retry set_value failed for "${field}":`, String(svErr).slice(0, 200));
          }
        }
        record = (await erp.getDoc(ctx.creds, "Employee", name)) as Record<string, unknown>;
      }

      if (String(record.company) !== ctx.company) {
        return reply.status(403).send({ error: "Employee not in your Company" });
      }
      const { company: _drop, ...data } = record;
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

  /** HR admin: permanently delete an Employee from ERP. Irreversible — caller must confirm. */
  app.delete("/v1/employees/:id", async (req, reply) => {
    let ctx;
    try {
      ctx = resolveHrContext(req);
    } catch (e) {
      if (e instanceof HttpError) return reply.status(e.status).send({ error: e.message });
      throw e;
    }
    if (!ctx.canSubmitOnBehalf) {
      return reply.status(403).send({ error: "HR admin privileges required to delete employee records" });
    }
    const name = (req.params as { id: string }).id;
    try {
      await erp.deleteDoc(ctx.creds, "Employee", name);
      return reply.status(200).send({ data: { deleted: name } });
    } catch (e) {
      if (e instanceof ErpError) return replyErp(reply, e);
      throw e;
    }
  });
};
