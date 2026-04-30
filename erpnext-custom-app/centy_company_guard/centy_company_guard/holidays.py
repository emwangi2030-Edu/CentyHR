import frappe
import holidays
from frappe.utils import getdate, today
from datetime import date

SUPPORTED_COUNTRIES = {
    "KE": "Kenya",
    "UG": "Uganda",
    "TZ": "Tanzania",
    "RW": "Rwanda",
}


def get_or_create_holiday_list(country_code, country_name, year):
    """Idempotent: returns existing list or creates a fresh one."""
    list_name = f"{country_name} {year}"

    if frappe.db.exists("Holiday List", list_name):
        return frappe.get_doc("Holiday List", list_name)

    doc = frappe.new_doc("Holiday List")
    doc.holiday_list_name = list_name
    doc.from_date = date(year, 1, 1)
    doc.to_date = date(year, 12, 31)
    doc.country = country_code
    doc.insert(ignore_permissions=True)
    return doc


def sync_country_holidays(country_code, year):
    """
    Pulls public holidays from python-holidays and merges them into
    the Holiday List for that country/year. Manual entries are preserved.
    """
    if country_code not in SUPPORTED_COUNTRIES:
        frappe.throw(f"Country {country_code} not supported")

    country_name = SUPPORTED_COUNTRIES[country_code]
    library_holidays = holidays.country_holidays(country_code, years=year)

    holiday_list = get_or_create_holiday_list(country_code, country_name, year)

    existing_by_date = {
        getdate(row.holiday_date): row for row in holiday_list.holidays
    }

    added, updated, kept_manual = 0, 0, 0
    auto_dates_seen = set()

    for holiday_date, holiday_name in sorted(library_holidays.items()):
        auto_dates_seen.add(holiday_date)
        existing = existing_by_date.get(holiday_date)

        if existing is None:
            holiday_list.append("holidays", {
                "holiday_date": holiday_date,
                "description": holiday_name,
                "holiday_source": "Auto",
            })
            added += 1
        elif existing.holiday_source == "Manual":
            kept_manual += 1
        elif existing.description != holiday_name:
            existing.description = holiday_name
            updated += 1

    removed = 0
    for row in list(holiday_list.holidays):
        if row.holiday_source == "Auto" and getdate(row.holiday_date) not in auto_dates_seen:
            holiday_list.remove(row)
            removed += 1

    holiday_list.save(ignore_permissions=True)
    frappe.db.commit()

    log = (
        f"[{country_name} {year}] +{added} added, "
        f"~{updated} updated, -{removed} removed, "
        f"{kept_manual} manual preserved"
    )
    frappe.logger("centyhr").info(log)
    return log


def annual_sync_all_countries():
    """
    Scheduler entry point. Runs daily but only acts Nov 15 – Dec 31
    to populate next year's holidays. Idempotent — safe to re-run.
    """
    today_dt = getdate(today())

    # Only run in the seeding window: Nov 15 through end of December
    if not (today_dt.month == 12 or (today_dt.month == 11 and today_dt.day >= 15)):
        return

    next_year = today_dt.year + 1
    current_year = today_dt.year

    results = []
    for code in SUPPORTED_COUNTRIES:
        try:
            results.append(sync_country_holidays(code, current_year))
            results.append(sync_country_holidays(code, next_year))
        except Exception as e:
            frappe.log_error(
                f"Holiday sync failed for {code}: {e}",
                "CentyHR Holiday Sync"
            )

    return results


@frappe.whitelist()
def manual_sync(country_code, year):
    """Exposed to the UI button so HR admins can trigger a sync on demand."""
    frappe.only_for(["HR Manager", "System Manager"])
    return sync_country_holidays(country_code, int(year))


@frappe.whitelist()
def add_adhoc_holiday(country_code, year, holiday_date, description):
    """
    Adds a presidential proclamation / gazette holiday as a Manual entry.
    Survives all future auto-syncs.
    """
    frappe.only_for(["HR Manager", "System Manager"])

    country_name = SUPPORTED_COUNTRIES[country_code]
    holiday_list = get_or_create_holiday_list(country_code, country_name, int(year))

    for row in holiday_list.holidays:
        if getdate(row.holiday_date) == getdate(holiday_date):
            row.description = description
            row.holiday_source = "Manual"
            holiday_list.save(ignore_permissions=True)
            return f"Updated existing entry for {holiday_date}"

    holiday_list.append("holidays", {
        "holiday_date": getdate(holiday_date),
        "description": description,
        "holiday_source": "Manual",
    })
    holiday_list.save(ignore_permissions=True)
    return f"Added {description} on {holiday_date}"
