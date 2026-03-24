import "dotenv/config";

function req(name: string, fallback?: string): string {
  const v = process.env[name] ?? fallback;
  if (v === undefined || v === "") throw new Error(`Missing env ${name}`);
  return v;
}

/** Public base of the Frappe site, e.g. https://erp.tarakilishicloud.com */
export const ERP_BASE_URL = req("ERP_BASE_URL").replace(/\/+$/, "");

/** Optional: set if your reverse proxy expects `X-Frappe-Site-Name` (usually unnecessary when Host matches the site). */
export const ERP_SITE_NAME = process.env.ERP_SITE_NAME ?? "";

export const PORT = Number(process.env.PORT ?? "3040");

/** Public URL prefix when mounted behind a reverse proxy (e.g. /hr-api). No trailing slash. */
export const BASE_PATH = (process.env.BASE_PATH ?? "").replace(/\/+$/, "");

/** Bind address: use 127.0.0.1 so the BFF is not reachable from the public network. */
export const HR_BFF_HOST = process.env.HR_BFF_HOST ?? "127.0.0.1";

/**
 * Shared secret with Pay Hub proxy. When set, `X-Bridge-Auth` (HMAC) is required — do not use dev headers in production.
 */
export const HR_BRIDGE_SECRET = process.env.HR_BRIDGE_SECRET ?? "";

/**
 * Development only: trust X-Dev-User-Email + X-Dev-Company + dev API keys.
 * Use only when `HR_BRIDGE_SECRET` is not set (local dev).
 */
export const DEV_INSECURE_HEADERS = process.env.DEV_INSECURE_HEADERS === "1";

/** Fallback when per-user keys are not passed from the proxy */
export const ERP_API_KEY = process.env.ERP_API_KEY ?? "";
export const ERP_API_SECRET = process.env.ERP_API_SECRET ?? "";

/**
 * Optional: Supabase (service role) for expense policy / workflow config tables.
 * When unset, BFF serves empty rules and skips enforcement (ERP remains authoritative).
 */
export const SUPABASE_URL = process.env.SUPABASE_URL ?? "";
export const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY ?? "";

/**
 * Optional: max leave days a document `leave_approver` (line manager) may approve without HR.
 * If a request exceeds this, only `canSubmitOnBehalf` may approve — mirrors expense
 * `approve_ceiling_for_non_finance`. Unset = no day ceiling (ERP / single approver field still applies).
 */
export const LEAVE_MANAGER_APPROVE_MAX_DAYS: number | null = (() => {
  const raw = process.env.LEAVE_MANAGER_APPROVE_MAX_DAYS?.trim();
  if (raw === undefined || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : null;
})();

/** When `1`, leave uses a custom Check field for the first approver, then HR finalises `status`. See docs/ERP_TWO_STAGE_CUSTOM_FIELDS.md */
export const LEAVE_TWO_STAGE_APPROVAL = process.env.LEAVE_TWO_STAGE_APPROVAL === "1";

/** Custom field on Leave Application (Allow on Submit = Yes). */
export const LEAVE_FIRST_APPROVER_FIELD =
  process.env.LEAVE_FIRST_APPROVER_FIELD?.trim() || "custom_centy_first_approver_done";

/** HR may final-approve leave without first-approver flag (use sparingly). */
export const LEAVE_HR_BYPASS_FIRST_APPROVER = process.env.LEAVE_HR_BYPASS_FIRST_APPROVER === "1";

export const EXPENSE_TWO_STAGE_APPROVAL = process.env.EXPENSE_TWO_STAGE_APPROVAL === "1";

export const EXPENSE_FIRST_APPROVER_FIELD =
  process.env.EXPENSE_FIRST_APPROVER_FIELD?.trim() || "custom_centy_first_approver_done";

export const EXPENSE_HR_BYPASS_FIRST_APPROVER = process.env.EXPENSE_HR_BYPASS_FIRST_APPROVER === "1";
