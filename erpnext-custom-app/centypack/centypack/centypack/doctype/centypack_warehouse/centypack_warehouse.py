import frappe
from frappe import _
from frappe.model.document import Document


class CentyPackWarehouse(Document):
	def validate(self):
		existing = frappe.db.get_value(
			"CentyPack Warehouse",
			{"company": self.company, "warehouse": self.warehouse},
			"name",
		)
		if existing and existing != self.name:
			frappe.throw(_("This warehouse is already mapped for this company."))
