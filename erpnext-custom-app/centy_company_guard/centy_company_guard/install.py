"""One-time setup: Custom Field on Company for Centy performance methodology (BSC vs OKR)."""

import frappe


def after_install():
    _ensure_company_performance_methodology_field()
    _ensure_employee_disability_exemption_certificate_field()
    _ensure_employee_p10_car_benefit_fields()


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


def _ensure_employee_disability_exemption_certificate_field():
    """KRA P10 Section C — exemption certificate no. for persons with disability."""
    fieldname = "custom_exemption_certificate_number"
    if frappe.db.exists("Custom Field", {"dt": "Employee", "fieldname": fieldname}):
        return

    doc = frappe.get_doc(
        {
            "doctype": "Custom Field",
            "dt": "Employee",
            "fieldname": fieldname,
            "label": "Disability exemption certificate no.",
            "fieldtype": "Data",
            "insert_after": "tax_id",
            "description": "KRA P10 Section C — persons with disability (leave blank if not applicable).",
        }
    )
    doc.insert()
    frappe.db.commit()
    frappe.clear_cache(doctype="Employee")


def _ensure_employee_p10_car_benefit_fields():
    """KRA P10 Sheet D — employer car benefit computation (one company car row per employee)."""
    specs = [
        {
            "fieldname": "custom_p10_car_section_ref",
            "label": "P10 car benefit — section ref",
            "fieldtype": "Data",
            "insert_after": "custom_exemption_certificate_number",
            "default": "B",
            "description": "Reference to main P10 employee section (usually B).",
        },
        {
            "fieldname": "custom_car_registration",
            "label": "Company car — registration no.",
            "fieldtype": "Data",
            "insert_after": "custom_p10_car_section_ref",
        },
        {
            "fieldname": "custom_car_make",
            "label": "Company car — make",
            "fieldtype": "Data",
            "insert_after": "custom_car_registration",
        },
        {
            "fieldname": "custom_car_body_type",
            "label": "Company car — body type",
            "fieldtype": "Select",
            "options": "Salon / Hatch / Estate\nPick-up / Panel Van\nLand Rover / Cruiser\nOther",
            "insert_after": "custom_car_make",
        },
        {
            "fieldname": "custom_car_cc_rating",
            "label": "Company car — CC rating",
            "fieldtype": "Int",
            "insert_after": "custom_car_body_type",
        },
        {
            "fieldname": "custom_car_cost_type",
            "label": "Company car — cost type",
            "fieldtype": "Select",
            "options": "Owned\nHiring / Leasing",
            "insert_after": "custom_car_cc_rating",
        },
        {
            "fieldname": "custom_car_owned_cost",
            "label": "Company car — owned cost (KES)",
            "fieldtype": "Currency",
            "insert_after": "custom_car_cost_type",
            "description": "Initial / capital cost for 2% monthly rule.",
        },
        {
            "fieldname": "custom_car_hire_monthly",
            "label": "Company car — hire/lease per month (KES)",
            "fieldtype": "Currency",
            "insert_after": "custom_car_owned_cost",
        },
        {
            "fieldname": "custom_car_benefit_override",
            "label": "Company car — benefit override (KES/mo)",
            "fieldtype": "Currency",
            "insert_after": "custom_car_hire_monthly",
            "description": "If set, P10 uses this instead of computed value.",
        },
    ]
    for spec in specs:
        fieldname = spec["fieldname"]
        if frappe.db.exists("Custom Field", {"dt": "Employee", "fieldname": fieldname}):
            continue
        doc = frappe.get_doc({"doctype": "Custom Field", "dt": "Employee", **spec})
        doc.insert()
    frappe.db.commit()
    frappe.clear_cache(doctype="Employee")
