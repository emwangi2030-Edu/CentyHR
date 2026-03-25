# CentyHR — Bench install runbook (§3 server + §4 Kenya payroll config)

**Audience:** Operators provisioning a **dedicated** payroll/HR Frappe stack (not Pay Hub app code).

**Pair with:**

- Your internal **CentyHR Kenya Payroll Compliance Engine** spec (formulas, examples A/B/C, CSF KE reports).
- [`CENTYHR_KENYA_PAYROLL_HYBRID_INTEGRATION.md`](./CENTYHR_KENYA_PAYROLL_HYBRID_INTEGRATION.md) — how this site ties to Pay Hub, BFF, and timesheets.

---

## 0. Preflight (mandatory)

| Rule | Detail |
|------|--------|
| **New VPS** | Use a **separate** Linode (or equivalent). **Do not** install on the Zimbra host or any server where **80, 443, 25, 465, 587, 993, 995** are dedicated to mail — port and resource conflicts will break production mail or Frappe. |
| **OS** | Ubuntu **22.04** LTS (supported baseline for Frappe v15 / this runbook). |
| **DNS** | `A` record for `payroll.example.com` (or your chosen subdomain) → new VPS **before** TLS. |
| **Secrets** | Keep MariaDB root password, site `admin-password`, and Frappe API keys in a vault — not in shell history. |

**Automated helper (packages + wkhtmltopdf + `frappe` user):**

```bash
sudo bash scripts/centyhr-frappe-payroll-prereqs.sh
```

---

## 1. Non-root user

SSH as root, then:

```bash
adduser frappe
usermod -aG sudo frappe
su - frappe
```

Remaining bench commands are as **`frappe`** unless noted.

---

## 2. Node 18 and Yarn (frappe user)

```bash
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
source ~/.bashrc   # or: export NVM_DIR="$HOME/.nvm" && . "$NVM_DIR/nvm.sh"
nvm install 18 && nvm use 18
npm install -g yarn
```

---

## 3. MariaDB (run as root or sudo)

1. Hardening:

   ```bash
   sudo mysql_secure_installation
   ```

2. **Character set** — under `[mysqld]` in e.g. `/etc/mysql/mariadb.conf.d/50-server.cnf`:

   ```ini
   character-set-server = utf8mb4
   collation-server     = utf8mb4_unicode_ci
   ```

   **Note (MariaDB 10.6+):** `innodb_file_format`, `innodb_large_prefix`, and related options from older guides are usually **obsolete** (Barracuda / large prefix are default). If you follow a legacy snippet, **drop deprecated keys** if MariaDB warns on restart.

   ```bash
   sudo systemctl restart mariadb
   ```

3. Ensure Redis:

   ```bash
   sudo systemctl enable --now redis-server
   ```

---

## 4. Bench CLI and bench directory

As **`frappe`** (with Node 18 active):

```bash
sudo pip3 install frappe-bench
cd ~
bench init --frappe-branch version-15 frappe-bench
cd frappe-bench
```

---

## 5. Get apps (order matters)

```bash
bench get-app --branch version-15 erpnext
bench get-app --branch version-15 hrms
bench get-app https://github.com/navariltd/navari_csf_ke.git
```

App directory name for Kenya locale is typically **`csf_ke`** (confirm with `ls apps` after get-app).

---

## 6. New site and install apps

Replace placeholders:

```bash
export SITE_NAME="payroll.yourdomain.com"
export DB_ROOT_PASSWORD="********"
export ADMIN_PASSWORD="********"

bench new-site "$SITE_NAME" \
  --db-root-password "$DB_ROOT_PASSWORD" \
  --admin-password "$ADMIN_PASSWORD"

bench --site "$SITE_NAME" install-app erpnext
bench --site "$SITE_NAME" install-app hrms
bench --site "$SITE_NAME" install-app csf_ke
```

---

## 7. Production wiring

As **root** (adjust user/group to your `frappe` user):

```bash
sudo bench setup production frappe
```

Then as **`frappe`** inside `frappe-bench`:

```bash
bench --site "$SITE_NAME" enable-scheduler
bench --site "$SITE_NAME" set-maintenance-mode off
sudo supervisorctl restart all
```

TLS: terminate HTTPS at Nginx (Certbot or provider certificate) for `$SITE_NAME`.

---

## 8. ERP company and payroll foundation (ERP UI)

1. Log in as Administrator → **Company**: create with currency **KES**, fiscal year **Jan–Dec** (or your policy).
2. Complete **HR / Payroll** setup wizards as prompted by HRMS (cost centers, departments, etc.).
3. Confirm **CSF KE** appears under installed apps and **Payroll Reports** show Kenya reports after first payroll data exists.

---

## 9. §4 Kenya salary components (configuration)

Create **Salary Components** in **exact processing order** (HRMS calculates in list order; later formulas depend on earlier component abbreviations).

**Abbreviations** (must match Frappe “Abbreviation” / variable names used in formulas):  
`nssf_employee`, `shif`, `ahl_employee`, `paye`, `nssf_employer`, `ahl_employer`, NITA as fixed employer component.

### 9.1 Earnings (examples — adjust names to your policy)

| Order | Name | Type |
|------:|------|------|
| 1 | Basic Salary | Earning |
| 2 | House Allowance | Earning |
| 3 | Transport Allowance | Earning |
| 4 | Other Allowances | Earning |

`gross_pay` is supplied by Frappe as the sum of earnings.

### 9.2 NSSF Employee (Deduction, formula)

```python
LOWER_LIMIT = 9000
UPPER_CEILING = 108000
RATE = 0.06

if gross_pay <= LOWER_LIMIT:
    result = gross_pay * RATE
elif gross_pay <= UPPER_CEILING:
    tier1 = LOWER_LIMIT * RATE
    tier2 = (gross_pay - LOWER_LIMIT) * RATE
    result = tier1 + tier2
else:
    tier1 = LOWER_LIMIT * RATE
    tier2 = (UPPER_CEILING - LOWER_LIMIT) * RATE
    result = tier1 + tier2
```

Abbreviation: `nssf_employee`

### 9.3 SHIF (Deduction, formula)

```python
RATE = 0.0275
MINIMUM = 300

result = max(gross_pay * RATE, MINIMUM)
```

Abbreviation: `shif`

### 9.4 AHL Employee (Deduction, formula)

```python
result = gross_pay * 0.015
```

Abbreviation: `ahl_employee`

### 9.5 PAYE (Deduction, formula)

Uses **taxable income** = gross − NSSF employee − SHIF − AHL employee (Dec 2024 onward).  
Ensure component abbreviations match yours.

```python
PERSONAL_RELIEF = 2400

taxable = gross_pay - nssf_employee - shif - ahl_employee
taxable = max(taxable, 0)

if taxable <= 24000:
    tax = taxable * 0.10
elif taxable <= 32333:
    tax = (24000 * 0.10) + ((taxable - 24000) * 0.25)
elif taxable <= 500000:
    tax = (24000 * 0.10) + (8333 * 0.25) + ((taxable - 32333) * 0.30)
elif taxable <= 800000:
    tax = (24000 * 0.10) + (8333 * 0.25) + (467667 * 0.30) + ((taxable - 500000) * 0.325)
else:
    tax = (24000 * 0.10) + (8333 * 0.25) + (467667 * 0.30) + (300000 * 0.325) + ((taxable - 800000) * 0.35)

result = max(tax - PERSONAL_RELIEF, 0)
```

Abbreviation: `paye`

### 9.6 HELB (Deduction)

Configure per your process (flat, table, or zero until loan data exists).

### 9.7 NSSF Employer (Employer Contribution)

Same formula as **NSSF Employee** (§9.2). Abbreviation: `nssf_employer`

### 9.8 AHL Employer (Employer Contribution)

```python
result = gross_pay * 0.015
```

Abbreviation: `ahl_employer`

### 9.9 NITA (Employer Contribution)

Fixed **KES 50** employer-only (not deducted from employee net pay).

```python
result = 50
```

---

## 10. Salary Structure

1. Create a **Salary Structure** assigning all components **in the order listed in §9** (earnings first, then deductions, then employer contributions).
2. Assign the structure to employees (Assignment) with effective dates.
3. If HRMS offers separate **Tax Slab** linkage, align with your compliance approach: **formula-based PAYE above is authoritative** — avoid double taxation; disable or align slab with finance.

---

## 11. Verification (from your spec §8)

| Check | Action |
|--------|--------|
| Examples A / B / C | Create **Salary Slip** (draft) for a test employee with known gross; compare PAYE and net to spec. |
| NSSF tier | Confirm employee NSSF matches tier logic at 60k / 20k / 200k. |
| Reports | Run CSF KE: P9A, NSSF, SHIF, Housing Levy, bank advice. |
| PDF | Print payslip; confirm wkhtmltopdf path works. |
| Backup | Schedule DB + files backup **before** first live run. |

---

## 12. Link to Pay Hub / BFF

- Point **Pay Hub** `HR_*` / BFF bridge at this site’s URL and an **Integration User** API key with Company-scoped permissions.
- **Timesheets** and **Salary Slips** remain in ERP; Pay Hub uses BFF read APIs for slips after payroll run (see hybrid integration doc).

---

## Change log

| Date | Change |
|------|--------|
| 2026-03 | Initial runbook + `centyhr-frappe-payroll-prereqs.sh` |
