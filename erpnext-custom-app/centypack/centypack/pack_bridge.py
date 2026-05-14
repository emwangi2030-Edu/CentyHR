"""Pack session -> Material Receipt, serial numbers, trace + QR payload."""

import json

import frappe
from frappe import _
from frappe.utils import cint

from centypack.batch_trace import apply_pack_line_to_batch_control


def submit_pack_session(doc):
	if doc.stock_entry and frappe.db.exists("Stock Entry", doc.stock_entry):
		return doc.stock_entry

	if not doc.get("items"):
		frappe.throw(_("Add at least one pack line before submitting."))

	base_url = ""
	if frappe.db.has_column("Company", "centypack_trace_public_base_url"):
		base_url = (frappe.db.get_value("Company", doc.company, "centypack_trace_public_base_url") or "").rstrip("/")

	se_items = []
	line_meta = []

	for row in doc.items:
		apply_pack_line_to_batch_control(doc.company, row, sign=1)

		if not row.item_code or not row.qty or row.qty <= 0:
			frappe.throw(_("Each line needs Item and a positive carton Qty."))

		uom = row.uom or frappe.db.get_value("Item", row.item_code, "stock_uom")
		has_serial = cint(frappe.db.get_value("Item", row.item_code, "has_serial_no"))
		prefix = (row.serial_prefix or "CP").strip() or "CP"
		n_cartons = int(row.qty)
		if n_cartons != row.qty:
			frappe.throw(_("Carton Qty must be a whole number for serial generation."))

		serials = []
		if has_serial:
			for i in range(n_cartons):
				serials.append(f"{prefix}-{doc.name}-{row.name or row.idx}-{i + 1}")

		token = frappe.generate_hash(length=12)
		trace_url = f"{base_url}/t/{token}" if base_url else f"/trace?t={token}"

		payload = {
			"v": 1,
			"t": token,
			"item": row.item_code,
			"company": doc.company,
			"session": doc.name,
			"line": row.idx,
			"batch_no": row.get("batch_no") or None,
			"packed_by": doc.get("packed_by") or None,
		}
		qr_payload = json.dumps(payload, separators=(",", ":"))

		line_meta.append(
			{
				"name": row.name,
				"trace_token": token,
				"trace_url": trace_url,
				"qr_payload": qr_payload,
				"serials": serials,
				"item_code": row.item_code,
				"qty": row.qty,
				"uom": uom,
				"batch_no": row.get("batch_no") or None,
			}
		)

		entry = {
			"item_code": row.item_code,
			"qty": row.qty,
			"uom": uom,
			"stock_uom": uom,
			"conversion_factor": 1,
			"t_warehouse": doc.warehouse,
		}
		if line_meta[-1]["batch_no"]:
			entry["batch_no"] = line_meta[-1]["batch_no"]
		if serials:
			entry["serial_no"] = "\n".join(serials)
		se_items.append(entry)

	se = frappe.get_doc(
		{
			"doctype": "Stock Entry",
			"stock_entry_type": "Material Receipt",
			"company": doc.company,
			"to_warehouse": doc.warehouse,
			"posting_date": doc.posting_date,
			"items": se_items,
			"remarks": _("CentyPack Pack Session {0}").format(doc.name),
		}
	)
	se.flags.ignore_permissions = True
	se.insert()
	se.submit()

	for meta in line_meta:
		if meta.get("name"):
			frappe.db.set_value(
				"CentyPack Pack Item",
				meta["name"],
				{
					"trace_token": meta["trace_token"],
					"trace_url": meta["trace_url"],
					"qr_payload": meta["qr_payload"],
				},
				update_modified=False,
			)

	return se.name


def cancel_pack_session(doc):
	for row in (doc.get("items") or []):
		apply_pack_line_to_batch_control(doc.company, row, sign=-1)

	if doc.stock_entry and frappe.db.exists("Stock Entry", doc.stock_entry):
		se = frappe.get_doc("Stock Entry", doc.stock_entry)
		if se.docstatus == 1:
			se.flags.ignore_permissions = True
			se.cancel()
