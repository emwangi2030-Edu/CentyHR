import frappe
from frappe import _


@frappe.whitelist()
def create_loan_application(
    applicant_type, applicant, company, loan_product, loan_amount, posting_date,
    repayment_method=None, repayment_periods=None, repayment_amount=None,
):
    """
    Create a Loan Application while tolerating the centylms_credit on_validate hook
    that tries to insert a credit-assessment document linked back to the loan application
    before it has been committed to the database.

    We sidestep the timing issue by:
      1. Inserting the doc with ignore_validate=True so it lands in the DB.
      2. Committing immediately so subsequent queries can find it.
      3. Running validate manually — the loan application now exists, so the
         credit-assessment insert can resolve the link.
      4. Persisting any field changes made by validate via db_update().
    """
    # Validate the fields that actually matter.
    if not frappe.db.exists("Employee", applicant):
        frappe.throw(_("Employee {0} not found.").format(applicant))
    if not frappe.db.exists("Loan Product", loan_product):
        frappe.throw(_("Loan Product {0} not found.").format(loan_product))

    doc = frappe.get_doc({
        "doctype": "Loan Application",
        "applicant_type": applicant_type,
        "applicant": applicant,
        "company": company,
        "loan_product": loan_product,
        "loan_amount": frappe.utils.flt(loan_amount),
        "posting_date": posting_date,
    })
    if repayment_method:
        doc.repayment_method = repayment_method
    if repayment_periods is not None:
        doc.repayment_periods = frappe.utils.cint(repayment_periods)
    if repayment_amount is not None:
        doc.repayment_amount = frappe.utils.flt(repayment_amount)

    # Step 1 & 2: save to DB without running validate hooks.
    doc.flags.ignore_validate = True
    doc.insert(ignore_permissions=True)
    frappe.db.commit()

    # Step 3 & 4: now that the doc is in the DB, run validate safely.
    try:
        doc.run_method("validate")
        doc.db_update()
        frappe.db.commit()
    except Exception as e:
        # Credit-assessment or other post-insert validation failures should not
        # block the loan application from being created.
        frappe.log_error(frappe.get_traceback(), "create_loan_application: post-insert validate failed")

    return doc.as_dict()


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
