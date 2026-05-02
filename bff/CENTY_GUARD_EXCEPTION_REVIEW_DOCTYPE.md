# Frappe DocType: Centy Guard Exception Review

Pay Hub calls the HR BFF `POST /v1/guard/attendance/exception-review-sync`, which **creates or updates** one row per Pay Hub review in ERP.

## Install

Add this DocType to your **`centy_guard`** (or equivalent) custom app, migrate, and grant **HR / payroll** roles **Create**, **Write**, and **Read**.

The BFF uses `GUARD_EXCEPTION_REVIEW_DOCTYPE` (default **`Centy Guard Exception Review`**) — override via env if your DocType name differs.

## Fields (fieldname → type)

| Fieldname | Type | Required | Notes |
|-----------|------|----------|--------|
| `company` | Link → Company | Yes | Filter + set from bridge context |
| `employee` | Link → Employee | Yes | |
| `attendance_date` | Date | Yes | |
| `exception_key` | Data | Yes | Stable key from Pay Hub (date\|type\|employee\|…) |
| `exception_type` | Select | Yes | Options: `no_show`, `late`, `unscheduled_in` (add more if needed) |
| `resolution_status` | Select | Yes | `pending`, `approved`, `rejected` |
| `review_notes` | Small Text | No | |
| `payhub_review_id` | Data | Yes | Pay Hub UUID — used for upsert |
| `reviewed_at` | Datetime | No | ISO string from Pay Hub |
| `source` | Data | No | Set to `Pay Hub` by BFF |
| `client_site` | Link → Client Site | No | |
| `site_assignment` | Link → Site Assignment | No | From ERP exception payload when present |
| `first_in_time` | Data | No | For unscheduled_in |

**Recommended:** unique constraint on (`company`, `exception_key`) or (`company`, `payhub_review_id`) in ERP to avoid duplicates outside Pay Hub.

## Permissions

The integration user (API keys used by the BFF) must be allowed to create/update this DocType in the target **Company**.
