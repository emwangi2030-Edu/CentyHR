# ERPNext: two-stage approval (custom fields only)

Centy uses **custom fields on the document** (not ERPNext Workflow) so the BFF can drive first vs second approver with `frappe.client.set_value`.

## Leave Application

1. Open **Customize Form** → *Leave Application*.
2. Add a **Check** field:
   - **Fieldname:** `custom_centy_first_approver_done` (or match `LEAVE_FIRST_APPROVER_FIELD`)
   - **Label:** Centy — First approver done
   - **Allow on Submit:** Yes (required so submitted leaves can be updated)
3. Save.

## Expense Claim

1. **Customize Form** → *Expense Claim*.
2. Add the same pattern:
   - **Fieldname:** `custom_centy_first_approver_done` (or match `EXPENSE_FIRST_APPROVER_FIELD`)
   - **Allow on Submit:** Yes

## BFF environment

| Variable | Meaning |
|----------|---------|
| `LEAVE_TWO_STAGE_APPROVAL=1` | Enable two-step leave (custom field + final `status`) |
| `LEAVE_FIRST_APPROVER_FIELD` | Defaults to `custom_centy_first_approver_done` |
| `LEAVE_HR_BYPASS_FIRST_APPROVER=1` | HR may final-approve without first step (emergency / migration) |
| `EXPENSE_TWO_STAGE_APPROVAL=1` | Enable two-step expense claims |
| `EXPENSE_FIRST_APPROVER_FIELD` | Defaults to `custom_centy_first_approver_done` |
| `EXPENSE_HR_BYPASS_FIRST_APPROVER=1` | Finance may final-approve without first step |

## Behaviour

1. **First approver** (document `leave_approver` / `expense_approver` = user): action sets the custom Check to `1` only (does not set final approved state).
2. **HR / finance** (`canSubmitOnBehalf`): may set final approval **after** the first flag is set, or immediately if bypass env is set.

API responses include `centy_two_stage` and `centy_first_approver_done` for UI hints.
