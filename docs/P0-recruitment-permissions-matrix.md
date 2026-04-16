# P0 — Frappe HR recruitment ↔ Centy (permissions & DocTypes)

**Purpose:** Single reference for engineering before wiring BFF routes. **Tenant** = ERPNext `Company` (same name as Pay Hub business / bridge `company`).

## Roles (conceptual)

| Role | Typical Frappe roles | Centy / Pay Hub |
|------|----------------------|-----------------|
| Employee | Employee Self Service | Portal user; sees self + internal jobs / referrals where enabled |
| Hiring manager | HR User + reports access | People hub — requisitions, applicants for own dept (policy TBD) |
| HR ops | HR Manager | People hub — full recruitment module, interviews, offers |
| Company admin | System Manager / Admin (rare) | `canSubmitOnBehalf` + company settings incl. **performance methodology** |

## DocType → BFF route map (target)

| Frappe DocType | Read | Write | Notes |
|----------------|------|-------|--------|
| Job Requisition | `GET /v1/recruitment/requisitions` | `POST /v1/recruitment/requisitions` | Manager raise; HR approve |
| Staffing Plan | `GET/POST /v1/recruitment/staffing-plans` | same | If not in HRMS package, custom DocType |
| Job Opening | `GET /v1/recruitment/openings` | `POST/PATCH …/openings` | Internal vs external audience field |
| Job Applicant | `GET …/applicants` | `POST …/applicants` | Pipeline |
| Interview | `GET/POST …/interviews` | schedule / reschedule |
| Interview Feedback | `GET/POST …/feedback` | multi-interviewer |
| Job Offer | `GET/POST …/offers` | templates + e-sign provider TBD |

## Company-level HR settings (implemented P0.1)

| Field | Location | Values |
|-------|----------|--------|
| `centy_performance_methodology` | `Company` (Custom Field) | `bsc` \| `okr` |

**API:** `GET /v1/capabilities` includes `performance.methodology`. **Write:** `POST /v1/company/performance-methodology` (HR admin / `canSubmitOnBehalf`).

## Open items (fill in kickoff)

- [ ] Exact Frappe role names per site (`erp.tarakilishicloud.com`).
- [ ] Whether Job Requisition is visible to employees or manager-only.
- [ ] Public careers page scope (separate PRD section).

---

*Version 1.0 — 2026-04-16*
