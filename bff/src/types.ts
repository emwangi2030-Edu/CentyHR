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

/** ERPNext Asset doctype fields. */
export interface Asset {
  name: string;
  asset_name: string;
  item_code?: string;
  asset_category?: string;
  location?: string;
  custodian?: string; // Employee name
  status: string;
  image?: string;
  purchase_date?: string;
  // Add more fields as needed
}
