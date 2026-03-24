# Deploy two-stage approvals (steps 1 → 3)

## 1) ERPNext — custom fields

Follow **`ERP_TWO_STAGE_CUSTOM_FIELDS.md`** (Option A script or Option B manual).

Confirm in ERPNext that **Leave Application** and **Expense Claim** show the Check field and **Allow on Submit** is checked.

---

## 2) Centy HR BFF — env + restart

On the **BFF host** (Pay Hub app server or wherever `centy-hr-bff` runs), set at least:

```bash
export LEAVE_TWO_STAGE_APPROVAL=1
export EXPENSE_TWO_STAGE_APPROVAL=1
# optional overrides (defaults shown):
# export LEAVE_FIRST_APPROVER_FIELD=custom_centy_first_approver_done
# export EXPENSE_FIRST_APPROVER_FIELD=custom_centy_first_approver_done
# export LEAVE_HR_BYPASS_FIRST_APPROVER=0
# export EXPENSE_HR_BYPASS_FIRST_APPROVER=0
```

Persist in `bff/.env` or your process manager env file, then:

```bash
cd /opt/centy-hr-integration/bff   # or your clone path
git pull && npm ci && npm run build && npm test
pm2 restart centy-hr-bff
```

Smoke (from Pay Hub host, with bridge auth as usual):

```bash
curl -sS -H "Cookie: …" "http://127.0.0.1:3040/hr-api/v1/meta/hr-approval" | head -c 400
```

Expect JSON with `"two_stage_custom_field": true` for leave/expense when env is set.

---

## 3) Pay Hub — deploy web app

From your Pay Hub repo (e.g. `staging` branch already contains UI changes):

```bash
./scripts/push-to-staging-rsync.sh
# or, if regressions block locally:
SKIP_REGRESSION=1 ./scripts/push-to-staging-rsync.sh
```

See Pay Hub **`docs/PUSH-TO-STAGING-FROM-CURSOR.md`** for SSH and host details.

---

## Order matters

1 → 2 → 3 is safest: ERP fields first, then BFF so API matches ERP, then frontend.
