# CentyPack (Frappe / ERPNext v15)

POC custom app: packhouse master data (farmers, farms, blocks, crops, varieties, carton types, worker categories).

## Install (Docker bench, e.g. `erp.tarakilishicloud.com`)

1. Copy or clone this folder into the bench `apps` or `apps_extra` volume (same layout as other custom apps).
2. Inside the backend container, from `frappe-bench`:

```bash
bench pip install -e apps/centypack
bench --site <site> install-app centypack
bench --site <site> migrate
bench --site <site> clear-cache
```

3. Assign **CentyPack POC Admin** or **CentyPack POC User** to users who should see the module (or use **System Manager** during POC).

**Company (ERP mirror):** Custom Fields on **Company** — `centypack_disabled`, `centyhq_business_id`, `centypack_hub_industry` — plus permission hooks so non–System Manager users lose list/doc access when disabled or industry slug is set and not `agriculture`.

**Transactional POC:** **CentyPack Warehouse** (Company + ERPNext **Warehouse** + purpose). **CentyPack GDN** (submittable): on submit creates either a **Delivery Note** (Customer + From Warehouse) or **Stock Entry** Material Transfer (From + To). **CentyPack GDN Item** supports optional **Batch**. **CentyPack Grading Run** + lines (**CentyPack Grade**, **CentyPack Defect Type**) record kg by grade/defect (seed: CLASS-A/B/C, BRUISING). **CentyPack Pack Session** (submittable): **Material Receipt** into a warehouse; serialized items get generated serials + **trace_token** / **trace_url** / **qr_payload** on each line (set **CentyPack Trace Public Base URL** on Company for full HTTPS links).

`after_install` seeds sample rows when no crop **CHILI** exists yet: CHILI crop, varieties **Birds Eye** and **Jalapeño**, sample farmer/farm/block, EXP-5KG carton, packer category.

Re-run seeding only when CHILI is absent (e.g. after deleting test data):

```bash
bench --site <site> execute centypack.install.seed_masters_if_empty
```

### CSV data loader (masters)

UTF-8 CSV with a header row. Run from the bench host (path readable inside the backend container if using Docker):

```bash
bench --site <site> execute centypack.data_loader.import_farmers_from_file --kwargs "{'path':'/path/to/farmers.csv'}"
bench --site <site> execute centypack.data_loader.import_farms_from_file --kwargs "{'path':'/path/to/farms.csv'}"
bench --site <site> execute centypack.data_loader.import_blocks_from_file --kwargs "{'path':'/path/to/blocks.csv'}"
```

Or pass CSV text: `import_farmers_from_text`, `import_farms_from_text`, `import_blocks_from_text` (see `centypack/data_loader.py` for column names).

### Reports & runway

- **Query Reports** (module CentyPack): **CentyPack GDN Register**, **CentyPack Grading Summary** (open from **Workspace → CentyPack** or **Report List** after migrate).
- **CentyPack Production Day**: one row per **company + log date**. Enable **Auto roll-up from transactions** to fill kg / cartons / GDN count / pack session count from submitted **Grading Runs**, **GD_ns**, and **Pack Sessions** for that date.

### Pay Hub → ERP mirror (Company flags)

ERP `site_config.json`:

```json
"centypack_hub_mirror_token": "<same secret as Hub>"
```

Pay Hub `.env` (staging):

```bash
CENTYPACK_ERP_MIRROR_TOKEN=<same secret>
FRAPPE_BASE_URL=https://erp.tarakilishicloud.com
```

When company admins toggle **CentyPack preview** or a Centy super admin changes org flags, Hub POSTs to `centypack.api.hub_mirror.apply_from_hub` (guest + shared token). ERP updates **Company** fields `centyhq_business_id`, `centypack_disabled`, `centypack_hub_industry` (beta ⇒ `agriculture` for the ERP gate). The ERP **Company** row must match by **`centyhq_business_id`** or **Company name** = Hub **business name**.

Docker-oriented steps: [docs/DEPLOY_ERP_DOCKER.md](docs/DEPLOY_ERP_DOCKER.md).

## Pay Hub

Eligibility and deep links remain in the Pay Hub repo; ERP remains the system of record for stock and packhouse transactions (GDN, grading, etc. in later POC days).
