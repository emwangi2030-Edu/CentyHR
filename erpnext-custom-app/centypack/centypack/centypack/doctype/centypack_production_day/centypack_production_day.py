import frappe
from frappe import _
from frappe.model.document import Document

from centypack.production_rollup import fill_production_day_totals


class CentyPackProductionDay(Document):
	def validate(self):
		names = frappe.get_all(
			"CentyPack Production Day",
			filters={"company": self.company, "log_date": self.log_date},
			pluck="name",
		)
		others = [n for n in names if n != self.name]
		if others:
			frappe.throw(_("A production day already exists for this company and date."))
		if self.get("auto_rollup"):
			fill_production_day_totals(self)
