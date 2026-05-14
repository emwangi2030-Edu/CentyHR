import frappe
from frappe import _
from frappe.model.document import Document
from frappe.utils import cint

from centypack.batch_trace import compute_packed_weight_kg
from centypack.pack_bridge import cancel_pack_session, submit_pack_session


class CentyPackPackSession(Document):
	def validate(self):
		if self.get("items"):
			for row in self.items:
				if row.get("batch_no"):
					packed = compute_packed_weight_kg(row)
					rejected = float(row.get("rejected_weight_kg") or 0)
					returned = float(row.get("returned_to_stock_weight_kg") or 0)
					if packed < 0 or rejected < 0 or returned < 0:
						frappe.throw(_("Packed, rejected, and returned weights must be non-negative on line {0}.").format(row.idx))
				if row.item_code and cint(frappe.db.get_value("Item", row.item_code, "has_serial_no")):
					if not (row.serial_prefix or "").strip():
						frappe.throw(
							_("Set **Serial Prefix** on line {0} for serialized item {1}.").format(
								row.idx, row.item_code
							)
						)

	def before_submit(self):
		if not self.get("items"):
			frappe.throw(_("Add at least one pack line before submitting."))

	def on_submit(self):
		se_name = submit_pack_session(self)
		self.db_set("stock_entry", se_name, update_modified=False)

	def on_cancel(self):
		cancel_pack_session(self)
		self.db_set("stock_entry", None, update_modified=False)
