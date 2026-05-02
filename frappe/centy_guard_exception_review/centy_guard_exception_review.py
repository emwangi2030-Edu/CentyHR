import frappe
from frappe import _
from frappe.model.document import Document


class CentyGuardExceptionReview(Document):
	"""Pay Hub ↔ ERP exception review row; upsert keyed by payhub_review_id."""


def validate(doc, method=None):
	if doc.client_site:
		site_company = frappe.db.get_value("Client Site", doc.client_site, "company")
		if site_company and site_company != doc.company:
			frappe.throw(_("Client Site belongs to a different company."))
	if doc.site_assignment:
		sa_company = frappe.db.get_value("Site Assignment", doc.site_assignment, "company")
		if sa_company and sa_company != doc.company:
			frappe.throw(_("Site Assignment belongs to a different company."))
