import type { HrContext } from "../types.js";
import { hrCompanyDocumentRoles } from "../config.js";

/**
 * Company vault (CP Employer Document): requires bridge `canHr` **and** an explicit portal role
 * in `HR_COMPANY_DOCUMENT_ROLES` (default: super_admin, admin only).
 */
export function canManageCompanyDocuments(ctx: HrContext): boolean {
  if (!ctx.canSubmitOnBehalf) return false;
  const role = (ctx.appRole ?? "").trim();
  if (!role) return false;
  return hrCompanyDocumentRoles().includes(role);
}
