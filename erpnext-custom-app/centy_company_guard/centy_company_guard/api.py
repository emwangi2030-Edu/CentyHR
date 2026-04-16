import frappe
from frappe import _


@frappe.whitelist()
def create_shift_assignment(employee, shift_type, start_date, end_date=None):
    """
    Create a Shift Assignment while tolerating a stale/invalid department link on the
    employee record.  The standard API endpoint raises LinkValidationError when the
    employee's `department` value no longer matches any Department document (e.g. the
    department was renamed or deleted after the employee was created).  This method
    validates the fields we care about and inserts the document with link-validation
    bypassed so a bad department value does not block shift scheduling.
    """
    # Validate the fields that actually matter for shift assignment.
    if not frappe.db.exists("Employee", employee):
        frappe.throw(_("Employee {0} not found.").format(employee))
    if not frappe.db.exists("Shift Type", shift_type):
        frappe.throw(_("Shift Type {0} not found.").format(shift_type))

    doc = frappe.get_doc(
        {
            "doctype": "Shift Assignment",
            "employee": employee,
            "shift_type": shift_type,
            "start_date": start_date,
        }
    )
    if end_date:
        doc.end_date = end_date

    # ignore_links skips ERPNext's link-field validation so a stale department
    # value on the employee record does not prevent the assignment from saving.
    doc.flags.ignore_links = True
    doc.insert()
    frappe.db.commit()
    return doc.as_dict()
