# CentyPack Handover (Phase 1 + Traceability Extensions)

## Branch and Location

- Repository: `git@github.com:emwangi2030-Edu/CentyHR.git`
- Branch: `feature/centypack-phase2-traceability`
- CentyPack app path: `erpnext-custom-app/centypack`

## Functional Overview (End-to-End)

### 1) Master Data

- `Farmer`: captures farmer biodata and classification.
- `Customer`: captures buyer profile and trade attributes.
- `Crop` (Product Master): unique product name/code, default minimum buying rate, pack rate per box, optional inventory item link.
- `CentyPack Product Daily Price`: date-based override buying rates with duplicate-active guard per product/date.

### 2) Pricing Resolution

- Effective buying rate rule:
  - `daily override (if active for date)` else `default minimum`
- Exposed through:
  - API: `centypack.api.pricing.get_effective_buying_rate`
  - Product form helper fields (date, effective rate, source)
  - Query report: `CentyPack Effective Buying Rates`

### 3) Packhouse Transactions

- `CentyPack Grading Run`: quality/grade capture.
- `CentyPack Pack Session`: packing operation, creates stock entry.
- `CentyPack GDN`: dispatch operation, customer-linked shipment/transfer routing.
- Pack-line trace fields include:
  - `batch_no`
  - `trace_token`, `trace_url`, `qr_payload`
  - plus enriched payload context (`batch_no`, `packed_by`)

### 4) Batch Control + Mass Balance

New control doctype: `CentyPack Batch Control`

- Intake and tolerance:
  - `farm_weight_kg`
  - `packhouse_weight_kg`
  - `allowable_variance_kg` (default `20`)
  - Guard: block if `abs(packhouse - farm) > allowable`
- Running allocation totals:
  - `packed_kg`
  - `rejected_kg`
  - `returned_to_stock_kg`
  - `available_kg`
- Mass-balance rules:
  - `packed + rejected + returned <= packhouse`
  - `available = packhouse - packed - rejected`
- Pack session submit/cancel updates and reverses these totals.

### 5) Operations Reporting

- `CentyPack Batch Traceability`:
  - batch -> farmer/customer -> pack session/packer -> shipping lines
- `CentyPack Batch Exceptions`:
  - variance breach
  - no available balance
  - high rejection-rate threshold
- `CentyPack Effective Buying Rates`:
  - product effective buy rate by date and source

## Test Plan (Njamba)

## A. Setup

1. Clone and checkout:
   - `git clone git@github.com:emwangi2030-Edu/CentyHR.git`
   - `cd CentyHR`
   - `git checkout feature/centypack-phase2-traceability`
2. Ensure app exists at `erpnext-custom-app/centypack`.
3. In bench/site:
   - `bench --site <site> migrate`
   - `bench --site <site> clear-cache`

## B. Pricing Resolver

1. Create a `Crop` with `default_min_buying_rate`.
2. Add `CentyPack Product Daily Price` override for same product/date (active).
3. Validate:
   - product form effective rate section
   - `CentyPack Effective Buying Rates` report output
   - source switches correctly between `daily_override` and `default_minimum`.

## C. Variance Guard

1. Create `CentyPack Batch Control` with:
   - farm `1000`, packhouse `1015` -> should pass.
2. Try:
   - farm `1000`, packhouse `1030` -> should fail (`> +/-20kg`).

## D. Mass-Balance Scenario (Reference Case)

Target case:
- intake `1000kg`
- packed `950kg` (190 boxes x 5kg)
- rejected `25kg`
- returned to stock `25kg`

Steps:
1. Batch control: packhouse `1000kg`.
2. Pack session line for same `batch_no` with:
   - packed weight `950`
   - rejected `25`
   - returned `25`
3. Submit and verify in batch control:
   - packed `950`
   - rejected `25`
   - returned `25`
   - available `25`

## E. Reuse Remaining Balance

1. Create another pack session using same batch.
2. Consume part/all of `available_kg`.
3. Verify over-allocation protection blocks invalid totals.

## F. Shipment Traceability

1. Create and submit `CentyPack GDN` with same `batch_no`.
2. Run `CentyPack Batch Traceability`.
3. Confirm record includes:
   - batch, farmer, customer
   - pack session and `packed_by`
   - shipped customer/item/qty linkage

## G. Exception Monitoring

1. Run `CentyPack Batch Exceptions`.
2. Validate detection for:
   - variance breaches
   - no available balance
   - high rejection % (adjust filter threshold)

## H. Reversal Integrity

1. Cancel a submitted pack session.
2. Confirm batch totals reverse correctly.
3. Confirm report values update after cancellation.

## Notes

- `CentyPack Batch Control` should be created before packing against a batch.
- `batch_no` consistency is required to maintain end-to-end traceability.
- Keep generated artifacts out of source control (`__pycache__`, `.egg-info`).
