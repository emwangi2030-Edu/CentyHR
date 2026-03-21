# Centy HR ↔ ERPNext integration

Implementation bundle for **staging.centycapital.com** (and mobile) talking to **ERPNext** at `erp.tarakilishicloud.com`, with **tenant = Company** and **approval before payment**.

**Git:** [github.com/emwangi2030-Edu/CentyHR](https://github.com/emwangi2030-Edu/CentyHR) · **Supabase + push targets:** see [`REMOTE.md`](./REMOTE.md).

## Staging deployment (live)

| Item | Value |
|------|--------|
| **Public base URL** | `https://staging.centycapital.com/hr-api` |
| **Health check** | `GET https://staging.centycapital.com/hr-api/health` → `{"ok":true}` |
| **Server path** | `/opt/centy-hr-integration/` |
| **Process** | `pm2` app name **`centy-hr-bff`** (port **3040** on localhost) |
| **Reverse proxy** | OpenLiteSpeed `context /hr-api` → `127.0.0.1:3040` (same vhost as Pay Hub staging) |

**Collaborators:** clone or rsync this repo, deploy under `/opt/centy-hr-integration/bff`, run `npm ci && npm run build`, `pm2 start ecosystem.config.cjs`, add `.env` with `ERP_API_KEY` / `ERP_API_SECRET` for integration tests.

**Note:** If CyberPanel rewrites the staging vhost, re-apply the **`/hr-api`** `extprocessor` + `context` blocks (or add an equivalent rule in the panel) so traffic reaches port **3040**.

Environment on the app server: `BASE_PATH=/hr-api`, `PORT=3040`, `ERP_BASE_URL=https://erp.tarakilishicloud.com` (see `bff/.env`).

## Contents

| Path | Purpose |
|------|---------|
| `bff/` | Node (Fastify) **BFF**: employee profile, expense claims, attachments, submit. |
| `erpnext-custom-app/centy_company_guard/` | **Frappe app**: server-side validation so **Expense Claim.company** matches **Employee.company** and approver is in the same company. |

## ERPNext (Desk) — do this before relying on the BFF

1. **Companies** — One `Company` per Centy tenant; users get **User Permissions** on `Company` so they only see their tenant.
2. **Employees** — Every user has **`user_id`** set to their Frappe `User.name`; **`company`** set; **`expense_approver`** set (HR Settings require it).
3. **Roles & permissions**
   - **Employee Self Service**: grant `DocPerm` on **Expense Claim** (read/write/create/**submit** as per your policy).
   - **HR User / HR Manager**: submit on behalf (same `Company` via User Permissions).
   - **Expense Approver**: approve within their `Company`.
4. **API keys (critical for tenancy)**  
   Frappe **API Key / Secret** on a **User** runs API calls **as that user**, so **User Permissions on Company** apply.  
   For production, the BFF must use **per-user keys** (stored encrypted) or your own JWT→impersonation flow — **not** a single global admin key.

## Frappe custom app (`centy_company_guard`)

Install on the bench that serves `erp.tarakilishicloud.com`:

```bash
# Copy centy_company_guard into bench apps, then:
cd /path/to/frappe-bench
./env/bin/pip install -e apps/centy_company_guard
bench --site erp.tarakilishicloud.com install-app centy_company_guard
bench restart
```

Uninstall: `bench --site erp.tarakilishicloud.com uninstall-app centy_company_guard` (only if needed).

The hook enforces **Company** alignment on **Expense Claim** validate. If your **Expense Approver** is not always an **Employee** row (edge case), adjust `events/expense_claim.py`.

## BFF (`bff/`)

```bash
cd bff
cp .env.example .env
# Set ERP_BASE_URL, ERP_API_KEY, ERP_API_SECRET for a test Frappe user
# Optional: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY + run supabase/migrations/001_*.sql for expense policy/workflow
```

**Development only** (insecure — never expose publicly):

```bash
export DEV_INSECURE_HEADERS=1
export ERP_API_KEY=...
export ERP_API_SECRET=...
npm run dev
```

Call examples:

```http
GET /health
GET /v1/me/employee
  X-Dev-User-Email: user@company.com
  X-Dev-Company: "Your Company Name"
```

```http
GET /v1/expenses
  X-Dev-User-Email: ...
  X-Dev-Company: ...
  X-Dev-HR: 1    # optional: list all claims in company (HR)
```

```http
POST /v1/expenses
  Content-Type: application/json
  X-Dev-User-Email: ...
  X-Dev-Company: ...

  { "posting_date": "2026-03-21", "expenses": [ { "expense_type": "Travel", "expense_date": "2026-03-21", "amount": 100 } ] }
```

```http
POST /v1/expenses/EXCLAIM-00001/submit
```

```http
POST /v1/expenses/EXCLAIM-00001/attachments
  Content-Type: multipart/form-data
  file=<receipt.pdf>
```

### Production auth

Implement `resolveHrContext()` in `bff/src/context/resolveHrContext.ts`: validate your portal JWT, load **`Company`** and **Frappe API credentials** for that user from your DB, set `canSubmitOnBehalf` from your roles. Remove reliance on `DEV_INSECURE_HEADERS`.

### Submit API note

The BFF calls `frappe.client.submit`. If your Frappe version rejects it, switch to the documented REST submit for your version (see Frappe docs) and update `ErpNextClient.submitDoc`.

## Security checklist

- [ ] No global admin API key in production clients.
- [ ] Per-user Frappe keys or verified impersonation.
- [ ] Company always from **auth**, never from untrusted body alone.
- [ ] CORS on ERP only if browser calls ERP directly; BFF avoids that.
- [ ] Payment only after **approved** claims (process + ERP roles).
