#!/usr/bin/env python3
"""seed_country_payroll.py — seed a country's payroll template into ERPNext.

Idempotent. Reads a country JSON profile and creates / updates:

  1. Currency (enabled)
  2. Holiday List + child holidays (set as Company.default_holiday_list)
  3. Salary Components (country-tagged; never modifies pre-existing globals)
  4. Income Tax Slab (per Company + Currency + effective_from) — slab-driven PAYE
  5. Payroll Period for the year (per Company)
  6. Salary Structure linked to the Income Tax Slab
  7. Salary Component Account (per-Company GL mapping)
  8. Sample Employees + Salary Structure Assignments (optional, for demo)

Usage:
    python3 seed_country_payroll.py \
        --country tanzania --company "Upeo TZ Demo" --year 2026

    python3 seed_country_payroll.py \
        --country uganda --company "Upeo UG Demo" --year 2026 --skip-employees

Env:
    ENV_FILE (default /opt/centy-hr-integration/bff/.env)
    or ERP_BASE_URL + HR_ERP_API_KEY + HR_ERP_API_SECRET

Profiles live under data/country-profiles/<country>.json (same dir as this script).
Re-runs are safe: every step is upsert by natural key (name) or composite key.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
PROFILE_DIR = SCRIPT_DIR / "data" / "country-profiles"


@dataclasses.dataclass
class ErpClient:
    base_url: str
    api_key: str
    api_secret: str

    def _auth_header(self) -> str:
        return f"token {self.api_key}:{self.api_secret}"

    def _request(self, method: str, path: str, *, data: Any = None, params: dict | None = None, ignore_status: tuple[int, ...] = ()) -> Any:
        url = self.base_url.rstrip("/") + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        body = None
        headers = {"Authorization": self._auth_header(), "Accept": "application/json"}
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code in ignore_status:
                return None
            err_body = e.read().decode("utf-8", errors="replace") if e.fp else ""
            raise RuntimeError(f"HTTP {e.code} {method} {path}: {err_body[:400]}") from None
        try:
            return json.loads(payload)
        except Exception:
            return payload

    def get_value(self, doctype: str, filters: dict, fieldname: Any = "name") -> Any:
        fn = json.dumps(fieldname) if isinstance(fieldname, (list, tuple)) else fieldname
        out = self._request(
            "GET",
            "/api/method/frappe.client.get_value",
            params={"doctype": doctype, "filters": json.dumps(filters), "fieldname": fn},
        )
        msg = (out or {}).get("message") if isinstance(out, dict) else None
        if msg is None or msg == "":
            return None
        if isinstance(msg, dict):
            if fieldname == "name":
                return msg.get("name") or None
            return msg
        return msg

    def get_doc(self, doctype: str, name: str) -> dict | None:
        out = self._request("GET", "/api/method/frappe.client.get", params={"doctype": doctype, "name": name})
        if isinstance(out, dict):
            return out.get("message")
        return None

    def insert(self, doc: dict) -> dict:
        out = self._request("POST", "/api/method/frappe.client.insert", data={"doc": doc})
        if not isinstance(out, dict) or "message" not in out:
            raise RuntimeError(f"insert failed for {doc.get('doctype')}: {out}")
        return out["message"]

    def save(self, doc: dict) -> dict:
        out = self._request("POST", "/api/method/frappe.client.save", data={"doc": doc})
        if not isinstance(out, dict) or "message" not in out:
            raise RuntimeError(f"save failed for {doc.get('doctype')}: {out}")
        return out["message"]

    def set_value(self, doctype: str, name: str, fieldname: str, value: Any) -> None:
        self._request(
            "POST",
            "/api/method/frappe.client.set_value",
            data={"doctype": doctype, "name": name, "fieldname": fieldname, "value": value},
        )

    def submit(self, doc: dict) -> dict:
        out = self._request("POST", "/api/method/frappe.client.submit", data={"doc": doc})
        if not isinstance(out, dict) or "message" not in out:
            raise RuntimeError(f"submit failed: {out}")
        return out["message"]


def load_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.is_file():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def make_client(env_file: Path) -> ErpClient:
    file_env = load_env_file(env_file)
    base = os.environ.get("ERP_BASE_URL") or file_env.get("ERP_BASE_URL")
    key = os.environ.get("HR_ERP_API_KEY") or file_env.get("HR_ERP_API_KEY") or file_env.get("ERP_API_KEY")
    sec = os.environ.get("HR_ERP_API_SECRET") or file_env.get("HR_ERP_API_SECRET") or file_env.get("ERP_API_SECRET")
    if not (base and key and sec):
        sys.exit("Missing ERP_BASE_URL or HR_ERP_API_KEY/HR_ERP_API_SECRET (check --env-file).")
    return ErpClient(base_url=base, api_key=key, api_secret=sec)


def template_str(s: str, *, year: int, company: str) -> str:
    return s.format(year=year, company=company)


def template_value(v: Any, *, year: int, company: str) -> Any:
    if isinstance(v, str):
        return template_str(v, year=year, company=company)
    if isinstance(v, list):
        return [template_value(x, year=year, company=company) for x in v]
    if isinstance(v, dict):
        return {k: template_value(val, year=year, company=company) for k, val in v.items()}
    return v


def step(title: str) -> None:
    print(f"\n>>> {title}")


def ensure_currency(erp: ErpClient, currency: str) -> None:
    step(f"Currency {currency}")
    cur = erp.get_value("Currency", {"name": currency}, fieldname=["name", "enabled"])
    if not cur:
        sys.exit(f"Currency '{currency}' not found in ERPNext. Add Currency master first.")
    if isinstance(cur, dict) and not cur.get("enabled"):
        erp.set_value("Currency", currency, "enabled", 1)
        print(f"  enabled {currency}")
    else:
        print(f"  {currency} already enabled")


def ensure_holiday_list(erp: ErpClient, profile: dict, year: int, company: str) -> str:
    step("Holiday List")
    hl = template_value(profile["holiday_list"], year=year, company=company)
    name = hl["name_template"]
    existing = erp.get_value("Holiday List", {"name": name})
    if existing:
        print(f"  exists: {existing}")
        hl_name = existing
    else:
        holidays = list(hl.get("holidays_fixed", []))
        ydict = hl.get("holidays_by_year", {}) or {}
        holidays += list(ydict.get(str(year), []))
        doc = {
            "doctype": "Holiday List",
            "holiday_list_name": name,
            "from_date": hl["from_date"],
            "to_date": hl["to_date"],
            "country": hl.get("country"),
            "holidays": [{"holiday_date": d, "description": desc} for (d, desc) in holidays],
        }
        created = erp.insert(doc)
        hl_name = created["name"]
        print(f"  created: {hl_name}  ({len(holidays)} holidays)")
    # Attach as Company default
    company_doc = erp.get_doc("Company", company) or {}
    if (company_doc.get("default_holiday_list") or "") != hl_name:
        erp.set_value("Company", company, "default_holiday_list", hl_name)
        print(f"  set Company.default_holiday_list = {hl_name}")
    return hl_name


def ensure_salary_components(erp: ErpClient, profile: dict) -> None:
    step("Salary Components")
    for comp in profile["salary_components"]:
        name = comp["name"]
        existing = erp.get_value("Salary Component", {"name": name})
        if existing:
            if comp.get("reuse_existing"):
                print(f"  reuse: {name}")
                continue
            # Patch flags if they differ
            doc = erp.get_doc("Salary Component", existing) or {}
            patch_fields = ("variable_based_on_taxable_salary", "is_tax_applicable", "statistical_component", "do_not_include_in_total", "depends_on_payment_days", "type")
            patched = False
            for f in patch_fields:
                if f in comp and (doc.get(f) or 0) != comp[f]:
                    erp.set_value("Salary Component", existing, f, comp[f])
                    patched = True
            print(f"  exists{' (patched)' if patched else ''}: {name}")
            continue
        if comp.get("reuse_existing"):
            sys.exit(f"Component '{name}' marked reuse_existing but missing in ERP. Aborting.")
        doc = {
            "doctype": "Salary Component",
            "salary_component": name,
            "salary_component_abbr": comp.get("abbr"),
            "type": comp.get("type", "Deduction"),
        }
        for f in ("variable_based_on_taxable_salary", "is_tax_applicable", "statistical_component", "do_not_include_in_total", "depends_on_payment_days"):
            if f in comp:
                doc[f] = comp[f]
        if comp.get("description"):
            doc["description"] = comp["description"]
        created = erp.insert(doc)
        print(f"  created: {created['name']}")


def ensure_income_tax_slab(erp: ErpClient, profile: dict, year: int, company: str, currency: str) -> str:
    step("Income Tax Slab")
    cfg = template_value(profile["income_tax_slab"], year=year, company=company)
    name = cfg["name_template"]
    existing = erp.get_value("Income Tax Slab", {"name": name})
    if existing:
        print(f"  exists: {existing}")
        return existing
    doc = {
        "doctype": "Income Tax Slab",
        "name": name,
        "effective_from": cfg["effective_from"],
        "company": company,
        "currency": currency,
        "allow_tax_exemption": cfg.get("allow_tax_exemption", 0),
        "standard_tax_exemption_amount": cfg.get("standard_tax_exemption_amount", 0),
        "tax_relief_limit": cfg.get("tax_relief_limit", 0),
        "disabled": 0,
        "slabs": [
            {
                "from_amount": s["from_amount"],
                "to_amount": s["to_amount"],
                "percent_deduction": s["percent_deduction"],
                **({"condition": s["condition"]} if s.get("condition") else {}),
            }
            for s in cfg["slabs"]
        ],
    }
    created = erp.insert(doc)
    created_name = created["name"]
    print(f"  created (draft): {created_name}  ({len(doc['slabs'])} bands)")
    # Submit so it's usable
    try:
        full = erp.get_doc("Income Tax Slab", created_name)
        if full and full.get("docstatus", 0) == 0:
            erp.submit(full)
            print(f"  submitted: {created_name}")
    except Exception as e:
        print(f"  (submit deferred: {e})")
    return created_name


def ensure_payroll_period(erp: ErpClient, profile: dict, year: int, company: str) -> str:
    step("Payroll Period")
    cfg = template_value(profile["payroll_period"], year=year, company=company)
    name = cfg["name_template"]
    existing = erp.get_value("Payroll Period", {"name": name})
    if existing:
        print(f"  exists: {existing}")
        return existing
    doc = {
        "doctype": "Payroll Period",
        "name": name,
        "company": company,
        "start_date": cfg["start_date"],
        "end_date": cfg["end_date"],
    }
    created = erp.insert(doc)
    print(f"  created: {created['name']}")
    return created["name"]


def ensure_salary_structure(erp: ErpClient, profile: dict, year: int, company: str, currency: str, income_tax_slab: str) -> str:
    step("Salary Structure")
    cfg = template_value(profile["salary_structure"], year=year, company=company)
    name = cfg["name_template"]
    existing = erp.get_value("Salary Structure", {"name": name})
    if existing:
        print(f"  exists: {existing}")
        return existing

    def _row(r: dict) -> dict:
        row = {
            "salary_component": r["salary_component"],
            "amount_based_on_formula": r.get("amount_based_on_formula", 0),
        }
        if r.get("formula"):
            row["formula"] = r["formula"]
        for f in ("statistical_component", "do_not_include_in_total", "depends_on_payment_days"):
            if f in r:
                row[f] = r[f]
        return row

    doc = {
        "doctype": "Salary Structure",
        "name": name,
        "company": company,
        "currency": currency,
        "payroll_frequency": cfg.get("payroll_frequency", "Monthly"),
        "is_active": "Yes",
        "income_tax_slab": income_tax_slab,
        "earnings": [_row(r) for r in cfg.get("earnings", [])],
        "deductions": [_row(r) for r in cfg.get("deductions", [])],
    }
    created = erp.insert(doc)
    created_name = created["name"]
    # Submit so it can hold SSAs
    try:
        full = erp.get_doc("Salary Structure", created_name)
        if full and full.get("docstatus", 0) == 0:
            erp.submit(full)
            print(f"  created & submitted: {created_name}")
        else:
            print(f"  created: {created_name}")
    except Exception as e:
        print(f"  created (submit failed: {e}): {created_name}")
    return created_name


def ensure_component_accounts(erp: ErpClient, profile: dict, company: str) -> None:
    step("Salary Component Accounts (GL mapping per Company)")
    company_doc = erp.get_doc("Company", company) or {}
    pay_acct = company_doc.get("default_payroll_payable_account") or company_doc.get("default_payable_account")
    if not pay_acct:
        print("  no payable account on Company; skipping (set Company.default_payroll_payable_account later)")
        return
    for comp in profile["salary_components"]:
        if comp.get("reuse_existing"):
            continue
        cname = comp["name"]
        # Get the component doc; check accounts child table
        doc = erp.get_doc("Salary Component", cname)
        if not doc:
            continue
        accounts = doc.get("accounts") or []
        already = any(str(a.get("company") or "") == company for a in accounts)
        if already:
            print(f"  {cname}: already mapped for {company}")
            continue
        accounts.append({"company": company, "account": pay_acct})
        doc["accounts"] = accounts
        try:
            erp.save(doc)
            print(f"  {cname}: mapped -> {pay_acct}")
        except Exception as e:
            print(f"  {cname}: mapping failed: {e}")


def ensure_designation(erp: ErpClient, designation: str) -> None:
    if not designation:
        return
    if erp.get_value("Designation", {"name": designation}):
        return
    erp.insert({"doctype": "Designation", "designation_name": designation})


def ensure_sample_employees(erp: ErpClient, profile: dict, year: int, company: str, currency: str, salary_structure: str, payroll_period: str, income_tax_slab: str) -> None:
    step("Sample Employees + Salary Structure Assignments")
    samples = profile.get("sample_employees", [])
    if not samples:
        print("  (no samples in profile)")
        return
    for emp in samples:
        emp_resolved = template_value(emp, year=year, company=company)
        first = emp_resolved["first_name"]
        last = emp_resolved["last_name"]
        employee_name = f"{first} {last}"
        ensure_designation(erp, emp_resolved.get("designation"))
        # Idempotency: lookup by (employee_name, company)
        existing = erp.get_value("Employee", {"employee_name": employee_name, "company": company})
        if existing:
            emp_id = existing
            print(f"  employee exists: {emp_id} ({employee_name})")
        else:
            doc = {
                "doctype": "Employee",
                "employee_name": employee_name,
                "first_name": first,
                "last_name": last,
                "gender": emp_resolved["gender"],
                "date_of_birth": emp_resolved["dob"],
                "date_of_joining": emp_resolved["doj"],
                "company": company,
                "status": "Active",
                "salary_currency": currency,
                "cell_number": emp_resolved.get("phone"),
                "designation": emp_resolved.get("designation"),
            }
            created = erp.insert(doc)
            emp_id = created["name"]
            print(f"  created employee: {emp_id} ({employee_name})")
        # SSA
        ssa_existing = erp.get_value("Salary Structure Assignment", {"employee": emp_id, "salary_structure": salary_structure, "docstatus": ["!=", 2]})
        if ssa_existing:
            print(f"    SSA already exists: {ssa_existing}")
            continue
        ssa = {
            "doctype": "Salary Structure Assignment",
            "employee": emp_id,
            "salary_structure": salary_structure,
            "company": company,
            "currency": currency,
            "base": emp_resolved["base"],
            "from_date": emp_resolved["doj"],
            "income_tax_slab": income_tax_slab,
        }
        try:
            created = erp.insert(ssa)
            created_name = created["name"]
            full = erp.get_doc("Salary Structure Assignment", created_name)
            if full and full.get("docstatus", 0) == 0:
                erp.submit(full)
            print(f"    created SSA: {created_name} base={emp_resolved['base']} {currency}")
        except Exception as e:
            print(f"    SSA failed for {emp_id}: {e}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--country", required=True, help="country profile filename (no .json)")
    p.add_argument("--company", required=True, help="ERPNext Company name")
    p.add_argument("--year", type=int, default=date.today().year)
    p.add_argument("--env-file", type=Path, default=Path("/opt/centy-hr-integration/bff/.env"))
    p.add_argument("--skip-employees", action="store_true", help="do not create sample employees / SSAs")
    p.add_argument("--profile-dir", type=Path, default=PROFILE_DIR)
    args = p.parse_args()

    profile_path = args.profile_dir / f"{args.country.lower()}.json"
    if not profile_path.is_file():
        sys.exit(f"Profile not found: {profile_path}")
    profile = json.loads(profile_path.read_text(encoding="utf-8"))

    erp = make_client(args.env_file)

    print(f"\n=== Seeding payroll template for {args.company} ({args.country}, {args.year}) ===")
    print(f"ERP: {erp.base_url}")

    # 1) Currency
    ensure_currency(erp, profile["currency"])
    # 2) Verify Company exists
    cdoc = erp.get_doc("Company", args.company)
    if not cdoc:
        sys.exit(f"Company '{args.company}' not found. Run onboard-tenant-company.sh first.")
    cdoc_country = cdoc.get("country")
    cdoc_currency = cdoc.get("default_currency")
    if cdoc_country != profile["country"] or cdoc_currency != profile["currency"]:
        sys.exit(f"Company {args.company} country={cdoc_country}/{cdoc_currency} does not match profile {profile['country']}/{profile['currency']}.")

    # 3) Holiday List
    ensure_holiday_list(erp, profile, args.year, args.company)
    # 4) Components
    ensure_salary_components(erp, profile)
    # 5) Income Tax Slab
    slab = ensure_income_tax_slab(erp, profile, args.year, args.company, profile["currency"])
    # 6) Payroll Period
    period = ensure_payroll_period(erp, profile, args.year, args.company)
    # 7) Salary Structure
    structure = ensure_salary_structure(erp, profile, args.year, args.company, profile["currency"], slab)
    # 8) GL mappings
    ensure_component_accounts(erp, profile, args.company)
    # 9) Sample employees
    if not args.skip_employees:
        ensure_sample_employees(erp, profile, args.year, args.company, profile["currency"], structure, period, slab)
    else:
        print("\n>>> Skipping sample employees (--skip-employees)")

    print(f"\n=== DONE. {args.company} payroll template seeded. ===")
    print(f"Slab: {slab}\nStructure: {structure}\nPayroll Period: {period}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
