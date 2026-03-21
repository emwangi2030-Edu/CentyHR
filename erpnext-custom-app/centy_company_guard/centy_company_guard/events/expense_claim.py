import frappe
from frappe import _


def validate(doc, method=None):
    """Tenant = Company: keep Expense Claim aligned with Employee and approver."""
    if not doc.get("employee"):
        return

    emp_company = frappe.db.get_value("Employee", doc.employee, "company")
    if not emp_company:
        frappe.throw(_("Employee {0} must have a Company set.").format(doc.employee))

    if doc.get("company") and doc.company != emp_company:
        frappe.throw(
            _("Expense Claim Company ({0}) must match Employee's Company ({1}).").format(
                doc.company, emp_company
            )
        )

    if not doc.get("company"):
        doc.company = emp_company

    approver = doc.get("expense_approver")
    if not approver:
        return

    approver_company = frappe.db.sql(
        """
        SELECT company FROM `tabEmployee`
        WHERE user_id = %s AND status = 'Active'
        LIMIT 1
        """,
        (approver,),
    )
    if not approver_company:
        return

    ac = approver_company[0][0]
    if ac and ac != emp_company:
        frappe.throw(
            _("Expense Approver must belong to the same Company as the employee ({0}).").format(
                emp_company
            )
        )
