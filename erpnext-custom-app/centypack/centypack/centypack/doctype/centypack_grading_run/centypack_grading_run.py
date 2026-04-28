import frappe
from frappe import _
from frappe.model.document import Document


class CentyPackGradingRun(Document):
	def validate(self):
		if not self.get("lines"):
			frappe.throw(_("Add at least one grading line."))
