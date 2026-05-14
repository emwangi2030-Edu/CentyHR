import frappe
from frappe import _
from frappe.model.document import Document

from centypack.stock_bridge import cancel_gdn_stock_documents, create_gdn_stock_documents


class CentyPackGDN(Document):
	def validate(self):
		self._validate_routing()

	def before_submit(self):
		self._validate_items()
		self._validate_routing()

	def on_submit(self):
		if self.delivery_note or self.stock_entry:
			return
		dn, se = create_gdn_stock_documents(self)
		updates = {}
		if dn:
			updates["delivery_note"] = dn
		if se:
			updates["stock_entry"] = se
		if updates:
			self.db_set(updates, update_modified=False)

	def on_cancel(self):
		cancel_gdn_stock_documents(self)
		self.db_set({"delivery_note": None, "stock_entry": None}, update_modified=False)

	def _validate_items(self):
		if not self.get("items"):
			frappe.throw(_("At least one item line is required."))
		for row in self.items:
			if not row.item_code or not row.qty or row.qty <= 0:
				frappe.throw(_("Each line needs Item and a positive Qty."))
			if not row.uom:
				frappe.throw(_("UOM is required on line {0}.").format(row.idx))

	def _validate_routing(self):
		has_cust = bool(self.customer)
		has_from = bool(self.from_warehouse)
		has_to = bool(self.to_warehouse)
		if has_cust and has_from and not has_to:
			return
		if has_from and has_to and not has_cust:
			return
		frappe.throw(
			_(
				"Use **Customer + From Warehouse** for outbound delivery, or **From + To Warehouse** (no Customer) for internal transfer."
			)
		)
