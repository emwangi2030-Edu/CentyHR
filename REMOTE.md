# Remotes — Centy HR integration

Canonical **Git** and **Supabase** references for this bundle (no secrets in git).

## GitHub

| | |
|--|--|
| **Repository** | [github.com/emwangi2030-Edu/CentyHR](https://github.com/emwangi2030-Edu/CentyHR) |

Push this tree (e.g. `bff/`, `erpnext-custom-app/`) from your workstation; keep `.env` out of version control.

```bash
git remote add origin https://github.com/emwangi2030-Edu/CentyHR.git
# or SSH: git@github.com:emwangi2030-Edu/CentyHR.git
```

## Supabase (expense rules / workflow)

| | |
|--|--|
| **Dashboard** | [Project dashboard](https://supabase.com/dashboard/project/ezepspwuahmtvhwcqhew) |
| **Project ref** | `ezepspwuahmtvhwcqhew` |
| **API URL** (public) | `https://ezepspwuahmtvhwcqhew.supabase.co` |

On the BFF host, set (values from **Project Settings → API** in the dashboard):

- `SUPABASE_URL` — API URL above  
- `SUPABASE_SERVICE_ROLE_KEY` — **service_role** key — **server-only**; never commit or expose to the browser  

Apply SQL: `bff/supabase/migrations/001_expense_hub_company_rules.sql` (SQL editor or CLI).

After deploy: `pm2 restart centy-hr-bff --update-env` and confirm `GET /hr-api/v1/expenses/rules` returns `supabase_configured: true` when env is set.
