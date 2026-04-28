# Deploy CentyPack on Docker ERPNext (erp.tarakilishicloud.com)

Typical v15 stacks bind-mount a host folder as `apps_extra` (or place custom apps under `apps/`). Paths differ per host: confirm in `compose.yml` and with `docker compose exec backend ls /home/frappe/frappe-bench`.

## 1. Copy the app onto the server

From your machine (adjust source and `HOST`):

```bash
rsync -avz --delete /path/to/centypack/ root@HOST:/opt/erpnext/data/apps_extra/centypack/
```

The destination must be the **same directory** the `backend` container mounts as `/home/frappe/frappe-bench/apps_extra/centypack` (or merge `centypack` next to `frappe` / `erpnext` under `apps/` if that is how your bench is laid out).

Sanity check (expects `setup.py`):

```bash
docker compose exec -T backend test -f /home/frappe/frappe-bench/apps_extra/centypack/setup.py && echo OK
```

If your layout uses `apps/centypack` instead of `apps_extra/centypack`, change the `bench pip install -e` path in the next section accordingly.

## 2. Install inside the `backend` container

**First install** of the app on the site:

```bash
docker compose exec -T backend bash -lc '
  cd /home/frappe/frappe-bench
  bench pip install -e ./apps_extra/centypack
  bench --site erp.tarakilishicloud.com install-app centypack
  bench --site erp.tarakilishicloud.com migrate
  bench --site erp.tarakilishicloud.com clear-cache
'
```

Replace `erp.tarakilishicloud.com` with your site name (`sites/apps.txt` or `sites/*/site_config.json`).

**Upgrades** (app already installed): copy new code, then:

```bash
docker compose exec -T backend bash -lc '
  cd /home/frappe/frappe-bench
  bench pip install -e ./apps_extra/centypack
  bench --site erp.tarakilishicloud.com migrate
  bench --site erp.tarakilishicloud.com clear-cache
'
```

Avoid `bench install-app centypack --force` on production unless you intend to reinstall the app module.

## 3. Assign roles

In **Users**, assign **CentyPack POC Admin** or **CentyPack POC User** to test accounts. **System Manager** already has access from the shipped DocType permissions.

## 4. Verify sample data

After a fresh `install-app`, `after_install` seeds POC rows when no crop **CHILI** exists yet:

| DocType        | Name / code        |
|----------------|--------------------|
| Crop           | CHILI              |
| Variety        | Birds Eye, Jalapeño |
| Carton Type    | EXP-5KG            |
| Worker Category| Packer             |
| Farmer         | FRM-SAMPLE-001     |
| Farm           | FARM-SAMPLE-001    |
| Block          | BLK-SAMPLE-001      |

If the app was installed before seeding logic existed, delete the sample crop **CHILI** (and dependent rows if the DB allows) or insert equivalents in Desk. To re-run the seed routine from a shell (only inserts when **CHILI** is missing):

```bash
bench --site erp.tarakilishicloud.com execute centypack.install.seed_masters_if_empty
```

## Notes

- `bench pip install -e` must point at the directory that contains `setup.py` (this repo layout does).
- `bench clear-cache` is per site in multi-site benches; use `bench --site <site> clear-cache`.
