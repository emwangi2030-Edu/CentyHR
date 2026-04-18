/**
 * Shared helper: resolve the canonical ERPNext Company docname from a
 * display-name / short-name that ctx.company may carry.
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
    const found = (rows?.[0] as any)?.name;
    return typeof found === "string" && found.trim() ? found.trim() : raw;
  } catch {
    return raw;
  }
}
