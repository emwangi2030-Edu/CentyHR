# Deploy CentyHR BFF for `staging.centyhq.com`

Target host: **`tarakilishi-web-01`** (`172.239.110.187`). Pay Hub staging proxies **`/hr-api`** to **`127.0.0.1:3041`**; the BFF must listen on **3041** with **`BASE_PATH=/hr-api`**.

## 1. Prerequisites

- Git read access to [CentyHR](https://github.com/emwangi2030-Edu/CentyHR) **`performance`** branch.
- `.env` in `bff/` (copy from `bff/.env.example` on first install): `ERP_BASE_URL`, `ERP_API_KEY`, `ERP_API_SECRET`, `HR_BRIDGE_SECRET` (match Pay Hub staging), optional `SUPABASE_*`, `HR_CAPABILITIES_JSON`.

## 2. Install / update

```bash
cd /opt/centy-hr-integration-clean
git fetch origin
git checkout performance
git pull --ff-only origin performance
cd bff
npm ci
npm run build
pm2 restart centy-hr-bff-clean --update-env
```

## 3. Verify

```bash
curl -sS http://127.0.0.1:3041/hr-api/health
curl -sS https://staging.centyhq.com/hr-api/health
```

Expect `{"ok":true}`.

## 4. Pay Hub alignment

On the same host, CentyPay / Pay Hub staging (`centypay-staging`, port **5010**) should have:

- `HR_BFF_PORT=3041` (or equivalent) for server-side HR proxy calls to the BFF.
- Default `HR_BFF_PATH_PREFIX` unset → **`/hr-api`** prefix on outbound BFF URLs (see Pay Hub `hrBffPathPrefix()`).

## 5. Rollback

```bash
cd /opt/centy-hr-integration-clean
git checkout main   # or a known-good tag
cd bff && npm ci && npm run build
pm2 restart centy-hr-bff-clean --update-env
```

---

*Last updated: 2026-04-16*
