# Copyright: Centy HR — idempotent ERPNext Custom Field installer (two-stage approvals).
#
# Run on the **Frappe bench host** (not on the BFF). Examples:
#
#   cd /path/to/frappe-bench
#   bench --site YOUR_SITE console
#
# Then in the console:
#
#   exec(open("/path/to/install_two_stage_erpnext_fields.py").read())
#   install_two_stage_erpnext_fields()
#   frappe.db.commit()
#
# Or one line:
#   bench --site YOUR_SITE console -c "exec(open('.../install_two_stage_erpnext_fields.py').read()); install_two_stage_erpnext_fields(); import frappe; frappe.db.commit()"
#
# Requires: Administrator / site context (bench console provides it).

DEFAULT_FIELD = "custom_centy_first_approver_done"


def install_two_stage_erpnext_fields(field_name: str = DEFAULT_FIELD) -> None:
    import frappe
    from frappe.custom.doctype.custom_field.custom_field import create_custom_fields

    fn = (field_name or DEFAULT_FIELD).strip()
    if not fn.startswith("custom_"):
        frappe.throw("Field name must be a custom field (start with custom_)")

    create_custom_fields(
        {
            "Leave Application": [
                {
                    "fieldname": fn,
                    "label": "Centy — First approver done",
                    "fieldtype": "Check",
                    "insert_after": "status",
                    "allow_on_submit": 1,
                    "default": "0",
                }
            ],
            "Expense Claim": [
                {
                    "fieldname": fn,
                    "label": "Centy — First approver done",
                    "fieldtype": "Check",
                    "insert_after": "approval_status",
                    "allow_on_submit": 1,
                    "default": "0",
                }
            ],
        },
        update=True,
    )
    frappe.clear_cache(doctype="Leave Application")
    frappe.clear_cache(doctype="Expense Claim")


if __name__ == "__main__":
    raise SystemExit(
        "Run inside bench console — see docstring at top of install_two_stage_erpnext_fields.py"
    )
