from __future__ import annotations

import json
from datetime import date
from typing import Any

import frappe
import holidays as pyholidays
from frappe import _

DEFAULT_COUNTRY_CODES = ("KE", "UG", "TZ", "RW")


def _get_configured_country_codes() -> list[str]:
    configured = frappe.conf.get("centy_holiday_countries")
    if isinstance(configured, str):
        stripped = configured.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                loaded = json.loads(stripped)
                if isinstance(loaded, list):
                    parsed = [str(code).strip().upper() for code in loaded if str(code).strip()]
                    if parsed:
                        return parsed
            except json.JSONDecodeError:
                pass
        parsed = [code.strip().upper() for code in configured.split(",") if code.strip()]
        if parsed:
            return parsed
    elif isinstance(configured, (list, tuple)):
        parsed = [str(code).strip().upper() for code in configured if str(code).strip()]
        if parsed:
            return parsed
    return list(DEFAULT_COUNTRY_CODES)


def _country_label(country_code: str) -> str:
    code = (country_code or "").upper().strip()
    labels = {
        "KE": "Kenya",
        "UG": "Uganda",
        "TZ": "Tanzania",
        "RW": "Rwanda",
    }
    return labels.get(code, code)


def _upsert_holiday_list(country_code: str, year: int) -> dict[str, Any]:
    country_code = (country_code or "").upper().strip()
    if not country_code:
        frappe.throw(_("Country code is required."))

    holiday_map = pyholidays.country_holidays(country_code, years=[int(year)])
    if not holiday_map:
        frappe.throw(_("No holidays found for {0} in {1}.").format(country_code, year))

    year = int(year)
    list_name = f"{_country_label(country_code)} {year}"
    existing_name = frappe.db.get_value("Holiday List", {"holiday_list_name": list_name}, "name")

    if existing_name:
        holiday_list = frappe.get_doc("Holiday List", existing_name)
        holiday_list.holidays = []
    else:
        holiday_list = frappe.new_doc("Holiday List")
        holiday_list.holiday_list_name = list_name
        holiday_list.from_date = date(year, 1, 1)
        holiday_list.to_date = date(year, 12, 31)

    for holiday_date, description in sorted(holiday_map.items()):
        holiday_list.append(
            "holidays",
            {
                "holiday_date": holiday_date,
                "description": description,
                "weekly_off": 0,
            },
        )

    holiday_list.total_holidays = len(holiday_map)
    holiday_list.flags.ignore_permissions = True
    holiday_list.save(ignore_permissions=True)
    frappe.db.commit()

    return {
        "holiday_list": holiday_list.name,
        "holiday_list_name": holiday_list.holiday_list_name,
        "country_code": country_code,
        "year": year,
        "count": len(holiday_map),
    }


@frappe.whitelist()
def manual_sync(country_code: str = "KE", year: int | None = None) -> dict[str, Any]:
    if year is None:
        year = date.today().year
    return _upsert_holiday_list(country_code, int(year))


@frappe.whitelist()
def manual_sync_many(country_codes: list[str] | None = None, year: int | None = None) -> list[dict[str, Any]]:
    if year is None:
        year = date.today().year
    codes = country_codes or _get_configured_country_codes()
    return [_upsert_holiday_list(code, int(year)) for code in codes]


def scheduled_sync() -> list[dict[str, Any]]:
    today = date.today()
    if today.month not in (11, 12):
        return []

    # Pre-create current and next year during year-end period.
    years = [today.year, today.year + 1]
    results: list[dict[str, Any]] = []
    for year in years:
        results.extend(manual_sync_many(year=year))
    return results
