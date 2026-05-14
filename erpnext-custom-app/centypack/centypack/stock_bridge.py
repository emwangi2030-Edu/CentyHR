"""Create / cancel ERPNext stock documents for CentyPack GDN."""

import frappe
from frappe import _


def create_gdn_stock_documents(doc) -> tuple[str | None, str | None]:
	"""Return (delivery_note_name, stock_entry_name). Exactly one is set."""
	if doc.delivery_note or doc.stock_entry:
		return (doc.delivery_note or None, doc.stock_entry or None)

	if not doc.get("items"):
		frappe.throw(_("Add at least one line item before submitting."))

	if doc.customer and doc.from_warehouse:
		dn_name = _create_delivery_note(doc)
		return dn_name, None

	if doc.from_warehouse and doc.to_warehouse:
		if doc.from_warehouse == doc.to_warehouse:
			frappe.throw(_("From and To warehouse must differ for a transfer."))
		se_name = _create_material_transfer(doc)
		return None, se_name

	frappe.throw(
		_("Set either **Customer + From Warehouse** (outbound delivery) or **From + To Warehouse** (internal transfer).")
	)


def cancel_gdn_stock_documents(doc):
	if doc.delivery_note and frappe.db.exists("Delivery Note", doc.delivery_note):
		dn = frappe.get_doc("Delivery Note", doc.delivery_note)
		if dn.docstatus == 1:
			dn.flags.ignore_permissions = True
			dn.cancel()
	if doc.stock_entry and frappe.db.exists("Stock Entry", doc.stock_entry):
		se = frappe.get_doc("Stock Entry", doc.stock_entry)
		if se.docstatus == 1:
			se.flags.ignore_permissions = True
			se.cancel()


def _create_delivery_note(doc) -> str:
	naming_series = frappe.db.get_single_value("Selling Settings", "dn_naming_series") or "DN-.######"
	dn = frappe.get_doc(
		{
			"doctype": "Delivery Note",
			"naming_series": naming_series,
			"company": doc.company,
			"customer": doc.customer,
			"posting_date": doc.posting_date,
			"set_warehouse": doc.from_warehouse,
		}
	)
	if hasattr(dn, "set_missing_values"):
		dn.set_missing_values()
	for row in doc.items:
		dn.append(
			"items",
			{
				"item_code": row.item_code,
				"qty": row.qty,
				"uom": row.uom,
				"rate": 0,
				"warehouse": doc.from_warehouse,
				"batch_no": row.get("batch_no") or None,
			},
		)
	dn.flags.ignore_permissions = True
	dn.insert()
	dn.submit()
	return dn.name


def _create_material_transfer(doc) -> str:
	items = []
	for row in doc.items:
		item_code = row.item_code
		uom = row.uom or frappe.db.get_value("Item", item_code, "stock_uom")
		line = {
			"item_code": item_code,
			"qty": row.qty,
			"uom": uom,
			"s_warehouse": doc.from_warehouse,
			"t_warehouse": doc.to_warehouse,
			"stock_uom": uom,
			"conversion_factor": 1,
			"transfer_qty": row.qty,
		}
		if row.get("batch_no"):
			line["batch_no"] = row.batch_no
		items.append(line)

	se = frappe.get_doc(
		{
			"doctype": "Stock Entry",
			"stock_entry_type": "Material Transfer",
			"company": doc.company,
			"from_warehouse": doc.from_warehouse,
			"to_warehouse": doc.to_warehouse,
			"posting_date": doc.posting_date,
			"items": items,
		}
	)
	se.flags.ignore_permissions = True
	se.insert()
	se.submit()
	return se.name
