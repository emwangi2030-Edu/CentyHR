import { defaultClient, ErpError } from "../erpnext/client.js";
import type { ErpCredentials } from "../erpnext/client.js";

const erp = defaultClient();

export const COMPANY_PERFORMANCE_METHODOLOGY_FIELD = "centy_performance_methodology";
export type PerformanceMethodology = "bsc" | "okr";

export function normalizePerformanceMethodology(raw: unknown): PerformanceMethodology {
  const s = String(raw ?? "").trim().toLowerCase();
  return s === "okr" ? "okr" : "bsc";
}

export async function resolveCompanyDocName(creds: ErpCredentials, tokenCompany: string): Promise<string> {
  const t = tokenCompany.trim();
  if (!t) throw new ErpError(400, "Missing company context");

  try {
    await erp.getDoc(creds, "Company", t);
    return t;
  } catch {
    // fallback to lookup by company_name
  }

  const rows = await erp.getList(creds, "Company", {
    fields: ["name"],
    filters: [["company_name", "=", t]],
    limit_page_length: 1,
  });
  const first = (rows as { name?: string }[])[0];
  if (!first?.name) throw new ErpError(404, "Company not found for context: " + t);
  return first.name;
}

export async function readPerformanceMethodology(
  creds: ErpCredentials,
  tokenCompany: string,
): Promise<PerformanceMethodology> {
  const docName = await resolveCompanyDocName(creds, tokenCompany);
  const doc = await erp.getDoc(creds, "Company", docName);
  const v = doc[COMPANY_PERFORMANCE_METHODOLOGY_FIELD];
  return normalizePerformanceMethodology(v);
}

export async function writePerformanceMethodology(
  creds: ErpCredentials,
  tokenCompany: string,
  methodology: PerformanceMethodology,
): Promise<{ companyDocName: string; methodology: PerformanceMethodology }> {
  const docName = await resolveCompanyDocName(creds, tokenCompany);
  await erp.callMethod(creds, "frappe.client.set_value", {
    doctype: "Company",
    name: docName,
    fieldname: COMPANY_PERFORMANCE_METHODOLOGY_FIELD,
    value: methodology,
  });
  return { companyDocName: docName, methodology };
}
