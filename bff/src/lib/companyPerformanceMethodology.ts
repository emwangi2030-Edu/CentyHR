import { defaultClient, ErpError } from "../erpnext/client.js";
import type { ErpCredentials } from "../erpnext/client.js";

const erp = defaultClient();

/** Custom field on `Company` (created by `centy_company_guard` after_install). */
export const COMPANY_PERFORMANCE_METHODOLOGY_FIELD = "centy_performance_methodology";

export type PerformanceMethodology = "bsc" | "okr";

export function normalizePerformanceMethodology(raw: unknown): PerformanceMethodology {
  const s = String(raw ?? "").trim().toLowerCase();
  if (s === "okr") return "okr";
  return "bsc";
}

/**
 * Resolve ERPNext `Company.name` from bridge token company (name or `company_name`).
 */
export async function resolveCompanyDocName(creds: ErpCredentials, tokenCompany: string): Promise<string> {
  const t = tokenCompany.trim();
  if (!t) throw new Error("Missing company");

  try {
    await erp.getDoc(creds, "Company", t);
    return t;
  } catch (e) {
    if (!(e instanceof ErpError)) throw e;
  }

  const rows = await erp.getList(creds, "Company", {
    filters: [["company_name", "=", t]],
    fields: ["name"],
    limit_page_length: 1,
  });
  const first = (rows as { name?: string }[])[0];
  if (first?.name) return String(first.name).trim();
  return t;
}

export async function readPerformanceMethodology(
  creds: ErpCredentials,
  tokenCompany: string
): Promise<PerformanceMethodology> {
  try {
    const docName = await resolveCompanyDocName(creds, tokenCompany);
    const doc = await erp.getDoc(creds, "Company", docName);
    const v = doc[COMPANY_PERFORMANCE_METHODOLOGY_FIELD];
    return normalizePerformanceMethodology(v);
  } catch {
    return "bsc";
  }
}

export async function writePerformanceMethodology(
  creds: ErpCredentials,
  tokenCompany: string,
  methodology: PerformanceMethodology
): Promise<{ companyDocName: string }> {
  const docName = await resolveCompanyDocName(creds, tokenCompany);
  await erp.updateDoc(creds, "Company", docName, {
    [COMPANY_PERFORMANCE_METHODOLOGY_FIELD]: methodology,
  });
  return { companyDocName: docName };
}
