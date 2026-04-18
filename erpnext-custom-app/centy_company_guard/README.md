# centy_company_guard

Frappe app for Centy tenants: **expense guardrails** and **tenant performance methodology** metadata on `Company`.

## What ships in this repository

- **`hooks.py`**: `Expense Claim` validation (`events/expense_claim.py`) plus `after_install` for the methodology custom field.
- **`install.py`**: Ensures `Company.centy_performance_methodology` (`Select`: `bsc` / `okr`, default `bsc`).

## Production / Docker overlay (BSC bridge)

Some deployments mount an **extended** `centy_company_guard` tree (e.g. under `apps_extra`) that adds **BSC** doc events, class overrides, and scheduler hooks. That bundle is **not** in this Git tree; operators should:

1. Keep this repo’s **`install.py`** and **`after_install`** line in `hooks.py` when merging overlays.
2. Run `bench --site <site> execute centy_company_guard.install.after_install` once after adding `install.py` so the Company field exists on existing sites.
