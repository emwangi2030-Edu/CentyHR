/**
 * Line-manager leave approval vs HR-only threshold (see LEAVE_MANAGER_APPROVE_MAX_DAYS).
 */

export function leaveManagerBlockedByDayCeiling(
  totalLeaveDays: unknown,
  maxDays: number | null,
  canSubmitOnBehalf: boolean
): boolean {
  if (maxDays == null || canSubmitOnBehalf) return false;
  const days = Number(totalLeaveDays ?? 0);
  return Number.isFinite(days) && days > maxDays;
}

export function leaveManagerDayCeilingMessage(maxDays: number): string {
  return `Only HR may approve leave longer than ${maxDays} day(s) (configure LEAVE_MANAGER_APPROVE_MAX_DAYS)`;
}
