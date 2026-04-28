"""One-time setup: Custom Field on Company for Centy performance methodology (BSC vs OKR)."""

import frappe


def after_install():
    _ensure_company_performance_methodology_field()


def _ensure_company_performance_methodology_field():
    fieldname = "centy_performance_methodology"
    if frappe.db.exists("Custom Field", {"dt": "Company", "fieldname": fieldname}):
        return

    doc = frappe.get_doc(
        {
            "doctype": "Custom Field",
            "dt": "Company",
            "fieldname": fieldname,
            "label": "Centy performance methodology",
            "fieldtype": "Select",
            "options": "bsc\nokr",
            "default": "bsc",
            "insert_after": "abbr",
            "description": "Drives People hub + employee portal: Balanced Scorecard vs OKR-first UX.",
        }
    )
    doc.insert()
    frappe.db.commit()
