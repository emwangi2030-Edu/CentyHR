"""CSV → CentyPack masters (bench / execute). Use UTF-8 CSV with header row."""

import csv
import io
import frappe
from frappe import _
from frappe.utils import cint, flt


def _read_rows(text):
	text = (text or "").lstrip("\ufeff")
	if not text.strip():
		return []
	reader = csv.DictReader(io.StringIO(text))
	return [dict((k.strip(), (v or "").strip()) for k, v in list(row.items()) if k) for row in reader]


def _norm_key(key: str) -> str:
	return (key or "").strip().lower().replace(" ", "_").replace("-", "_")


def _pick(row: dict, *aliases: str) -> str:
	for a in aliases:
		v = row.get(a) or row.get(_norm_key(a))
		if v is not None and str(v).strip() != "":
			return str(v).strip()
	return ""


def import_farmers_from_text(text, update=True):
	"""CSV columns: farmer_code (or code), farmer_name, farmer_type, phone, mpesa_number, globalgap_number, globalgap_expiry, active (0/1)."""
	rows = _read_rows(text)
	created, updated, errors = 0, 0, []
	for i, raw in enumerate(rows, start=2):
		row = {_norm_key(k): v for k, v in raw.items()}
		code = _pick(row, "farmer_code", "code")
		name = _pick(row, "farmer_name", "name")
		ftype = _pick(row, "farmer_type", "type") or "Contract"
		if not code or not name:
			errors.append(_("Row {0}: farmer_code and farmer_name are required.").format(i))
			continue
		if ftype not in ("Own", "Contract", "Spot"):
			errors.append(_("Row {0}: farmer_type must be Own, Contract, or Spot.").format(i))
			continue
		doc_dict = {
			"doctype": "Farmer",
			"farmer_code": code,
			"farmer_name": name,
			"farmer_type": ftype,
			"phone": _pick(row, "phone"),
			"mpesa_number": _pick(row, "mpesa_number", "mpesa"),
			"globalgap_number": _pick(row, "globalgap_number", "globalgap"),
			"globalgap_expiry": _pick(row, "globalgap_expiry") or None,
			"active": cint(_pick(row, "active") or 1),
		}
		try:
			if frappe.db.exists("Farmer", code):
				if not update:
					errors.append(_("Row {0}: Farmer {1} exists (skipped).").format(i, code))
					continue
				doc = frappe.get_doc("Farmer", code)
				doc.update({k: v for k, v in doc_dict.items() if k != "doctype" and k != "farmer_code"})
				doc.save(ignore_permissions=True)
				updated += 1
			else:
				frappe.get_doc(doc_dict).insert(ignore_permissions=True)
				created += 1
		except Exception as e:
			errors.append(_("Row {0}: {1}").format(i, str(e)))
	frappe.db.commit()
	return {"created": created, "updated": updated, "errors": errors}


def import_farms_from_text(text, update=True):
	"""CSV: farm_code, farm_name, farmer (link code), county, location_note, size_ha, active."""
	rows = _read_rows(text)
	created, updated, errors = 0, 0, []
	for i, raw in enumerate(rows, start=2):
		row = {_norm_key(k): v for k, v in raw.items()}
		code = _pick(row, "farm_code", "code")
		name = _pick(row, "farm_name", "name")
		farmer = _pick(row, "farmer", "farmer_code")
		if not code or not name or not farmer:
			errors.append(_("Row {0}: farm_code, farm_name, and farmer are required.").format(i))
			continue
		if not frappe.db.exists("Farmer", farmer):
			errors.append(_("Row {0}: Farmer {1} does not exist.").format(i, farmer))
			continue
		doc_dict = {
			"doctype": "Farm",
			"farm_code": code,
			"farm_name": name,
			"farmer": farmer,
			"county": _pick(row, "county"),
			"location_note": _pick(row, "location_note", "location"),
			"size_ha": flt(_pick(row, "size_ha") or 0) or None,
			"active": cint(_pick(row, "active") or 1),
		}
		try:
			if frappe.db.exists("Farm", code):
				if not update:
					errors.append(_("Row {0}: Farm {1} exists (skipped).").format(i, code))
					continue
				doc = frappe.get_doc("Farm", code)
				doc.update({k: v for k, v in doc_dict.items() if k not in ("doctype", "farm_code")})
				doc.save(ignore_permissions=True)
				updated += 1
			else:
				frappe.get_doc(doc_dict).insert(ignore_permissions=True)
				created += 1
		except Exception as e:
			errors.append(_("Row {0}: {1}").format(i, str(e)))
	frappe.db.commit()
	return {"created": created, "updated": updated, "errors": errors}


def import_blocks_from_text(text, update=True):
	"""CSV: block_code, block_name, farm, crop, variety, size_ha, planting_date, expected_first_harvest, active. Farmer is fetched from farm."""
	rows = _read_rows(text)
	created, updated, errors = 0, 0, []
	for i, raw in enumerate(rows, start=2):
		row = {_norm_key(k): v for k, v in raw.items()}
		code = _pick(row, "block_code", "code")
		name = _pick(row, "block_name", "name")
		farm = _pick(row, "farm", "farm_code")
		crop = _pick(row, "crop", "crop_name")
		variety = _pick(row, "variety", "variety_name")
		if not code or not name or not farm or not crop or not variety:
			errors.append(_("Row {0}: block_code, block_name, farm, crop, variety are required.").format(i))
			continue
		if not frappe.db.exists("Farm", farm):
			errors.append(_("Row {0}: Farm {1} does not exist.").format(i, farm))
			continue
		if not frappe.db.exists("Crop", crop):
			errors.append(_("Row {0}: Crop {1} does not exist.").format(i, crop))
			continue
		if not frappe.db.exists("Variety", variety):
			errors.append(_("Row {0}: Variety {1} does not exist.").format(i, variety))
			continue
		farmer = frappe.db.get_value("Farm", farm, "farmer")
		doc_dict = {
			"doctype": "Block",
			"block_code": code,
			"block_name": name,
			"farm": farm,
			"farmer": farmer,
			"crop": crop,
			"variety": variety,
			"size_ha": flt(_pick(row, "size_ha") or 0) or None,
			"planting_date": _pick(row, "planting_date") or None,
			"expected_first_harvest": _pick(row, "expected_first_harvest", "first_harvest") or None,
			"active": cint(_pick(row, "active") or 1),
		}
		try:
			if frappe.db.exists("Block", code):
				if not update:
					errors.append(_("Row {0}: Block {1} exists (skipped).").format(i, code))
					continue
				doc = frappe.get_doc("Block", code)
				doc.update(
					{
						k: v
						for k, v in doc_dict.items()
						if k not in ("doctype", "block_code", "farmer")
					}
				)
				doc.farmer = farmer
				doc.save(ignore_permissions=True)
				updated += 1
			else:
				frappe.get_doc(doc_dict).insert(ignore_permissions=True)
				created += 1
		except Exception as e:
			errors.append(_("Row {0}: {1}").format(i, str(e)))
	frappe.db.commit()
	return {"created": created, "updated": updated, "errors": errors}


def import_farmers_from_file(path, update=True):
	with open(path, newline="", encoding="utf-8") as fh:
		return import_farmers_from_text(fh.read(), update=update)


def import_farms_from_file(path, update=True):
	with open(path, newline="", encoding="utf-8") as fh:
		return import_farms_from_text(fh.read(), update=update)


def import_blocks_from_file(path, update=True):
	with open(path, newline="", encoding="utf-8") as fh:
		return import_blocks_from_text(fh.read(), update=update)
