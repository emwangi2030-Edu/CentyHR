# Supabase migrations (HR BFF)

**Dashboard:** [CentyHR Supabase project](https://supabase.com/dashboard/project/ezepspwuahmtvhwcqhew) · **API URL:** `https://ezepspwuahmtvhwcqhew.supabase.co`

Run `001_expense_hub_company_rules.sql` and `002_employee_invites.sql` in the SQL editor (or `psql` against the project DB). The latter enables HR-issued **self-onboarding links** (`POST /hr-api/v1/employee-invites` + public `/employee-onboard` in Pay Hub).

Set on the **BFF** host (from **Project Settings → API**):

- `SUPABASE_URL` — `https://ezepspwuahmtvhwcqhew.supabase.co`
- `SUPABASE_SERVICE_ROLE_KEY` — **service_role** key — **server only**; never expose to the browser or commit to git

Then insert a row per ERP company (use ERPNext `Company.name` as `company_key`), or use **Policy & workflow** in Pay Hub (finance users) to save via `PUT /hr-api/v1/expenses/rules`.

See also [`../REMOTE.md`](../REMOTE.md) for GitHub + Supabase pointers.
