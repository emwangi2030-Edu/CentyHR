import type { ErpCredentials } from "./erpnext/client.js";

/** Request context: tenant = ERPNext Company. */
export interface HrContext {
  /** Logged-in Frappe user email (matches Employee.user_id). */
  userEmail: string;
  /** ERPNext `Company.name` for this request (from auth, not from client body). */
  company: string;
  /** API credentials for this Frappe user — required for User Permissions on Company. */
  creds: ErpCredentials;
  /** HR can create/submit claims for employees in the same company. */
  canSubmitOnBehalf: boolean;
}
