/**
 * Shared helper: resolve the canonical ERPNext Company docname from a
 * display-name / short-name that ctx.company may carry.
 * Also resolves the performance methodology (BSC vs OKR) from ERPNext Company settings.
 */
import { defaultClient, ErpCredentials, ErpError } from "../erpnext/client.js";

const erp = defaultClient();

/**
 * Resolve the canonical ERPNext Company docname from `company`.
 * `company` may be a display name that differs from the ERP docname —
 * we first try a direct getDoc hit, then fall back to a name-field search.
 */
export async function resolveCompanyDocName(creds: ErpCredentials, company: string): Promise<string> {
  const raw = String(company ?? "").trim();
  if (!raw) return raw;
  try {
    await erp.getDoc(creds, "Company", raw);
    return raw;
  } catch (e) {
    if (!(e instanceof ErpError)) throw e;
  }
  try {
    const rows = await erp.getList(creds, "Company", {
      filters: [["company_name", "=", raw]],
      fields: ["name"],
      limit_page_length: 1,
    });
    const found = (rows?.[0] as { name?: unknown } | undefined)?.name;
    return typeof found === "string" && found.trim() ? found.trim() : raw;
  } catch {
    return raw;
  }
}

/**
 * Read the performance methodology configured on the ERPNext Company doc.
 * Falls back to "bsc" if the field is absent or unrecognised.
 */
export async function readPerformanceMethodology(
  creds: ErpCredentials,
  company: string,
): Promise<"bsc" | "okr"> {
  try {
    const docName = await resolveCompanyDocName(creds, company);
    const doc = await erp.getDoc(creds, "Company", docName) as Record<string, unknown>;
    const raw = String(doc.custom_performance_methodology ?? doc.performance_methodology ?? "").toLowerCase().trim();
    if (raw === "okr") return "okr";
    return "bsc";
  } catch {
    return "bsc";
  }
}
