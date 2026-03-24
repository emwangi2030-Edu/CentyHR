/**
 * Two-stage approval via ERPNext custom Check fields (recommended vs Workflow for Centy BFF).
 */
import * as config from "../config.js";

export function isFirstApproverFlagSet(doc: Record<string, unknown>, field: string): boolean {
  if (!field) return false;
  const v = doc[field];
  return v === true || v === 1 || v === "1" || String(v ?? "").toLowerCase() === "yes";
}

export function attachCentyTwoStageLeaveRow(row: Record<string, unknown>): Record<string, unknown> {
  if (!config.LEAVE_TWO_STAGE_APPROVAL) {
    return { ...row, centy_two_stage: false, centy_first_approver_done: false };
  }
  const f = config.LEAVE_FIRST_APPROVER_FIELD;
  return {
    ...row,
    centy_two_stage: true,
    centy_first_approver_done: isFirstApproverFlagSet(row, f),
  };
}

export function attachCentyTwoStageExpenseRow(row: Record<string, unknown>): Record<string, unknown> {
  if (!config.EXPENSE_TWO_STAGE_APPROVAL) {
    return { ...row, centy_two_stage: false, centy_first_approver_done: false };
  }
  const f = config.EXPENSE_FIRST_APPROVER_FIELD;
  return {
    ...row,
    centy_two_stage: true,
    centy_first_approver_done: isFirstApproverFlagSet(row, f),
  };
}
