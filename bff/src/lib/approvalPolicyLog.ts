/**
 * Structured stderr logs for HR policy denials (grep / ship to log drain in production).
 */

export type HrPolicyDenialEvent =
  | "leave_approve_day_ceiling"
  | "expense_approve_amount_ceiling";

export function logHrPolicyDenial(event: HrPolicyDenialEvent, fields: Record<string, unknown>): void {
  const payload = {
    ts: new Date().toISOString(),
    level: "warn",
    scope: "centy_hr_policy",
    event,
    ...fields,
  };
  console.warn(JSON.stringify(payload));
}
