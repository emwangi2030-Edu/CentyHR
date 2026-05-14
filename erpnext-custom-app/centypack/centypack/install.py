import frappe

from centypack.permissions import CENTYPACK_GATED_DOCTYPES


def before_install():
	# Bench keeps a stale in-memory app_modules map; clear_cache alone does not reset it.
	# Without a rebuild, sync_for("centypack") can skip all DocTypes and after_install fails.
	frappe.clear_cache()
	frappe.setup_module_map(include_all_apps=True)


def after_install():
	ensure_roles()
	ensure_company_custom_fields()
	append_poc_permissions()
	seed_masters_if_empty()
	seed_grading_masters_if_empty()


def after_migrate():
	ensure_company_custom_fields()
	append_poc_permissions()
	seed_grading_masters_if_empty()


def ensure_roles():
	for role_name in ("CentyPack POC Admin", "CentyPack POC User"):
		if frappe.db.exists("Role", role_name):
			continue
		doc = frappe.get_doc(
			{
				"doctype": "Role",
				"role_name": role_name,
				"desk_access": 1,
			}
		)
		doc.insert(ignore_permissions=True)
	frappe.db.commit()


def append_poc_permissions():
	user_row = {
		"role": "CentyPack POC User",
		"read": 1,
		"export": 1,
		"print": 1,
		"email": 1,
		"report": 1,
	}
	was_patch = bool(getattr(frappe.flags, "in_patch", False))
	frappe.flags.in_patch = True
	try:
		for dt in CENTYPACK_GATED_DOCTYPES:
			doc = frappe.get_doc("DocType", dt)
			is_submittable = bool(frappe.db.get_value("DocType", dt, "is_submittable"))
			admin_row = {
				"role": "CentyPack POC Admin",
				"read": 1,
				"write": 1,
				"create": 1,
				"delete": 1,
				"export": 1,
				"print": 1,
				"email": 1,
				"report": 1,
				"share": 1,
				"submit": 1 if is_submittable else 0,
				"cancel": 1 if is_submittable else 0,
			}
			changed = False
			for role_row in (admin_row, user_row):
				role = role_row["role"]
				found = None
				for p in doc.permissions:
					if getattr(p, "role", None) == role:
						found = p
						break
				if found is None:
					doc.append("permissions", role_row)
					changed = True
				elif role == "CentyPack POC Admin":
					for key, val in admin_row.items():
						if key == "role":
							continue
						if int(getattr(found, key, 0) or 0) != int(val or 0):
							setattr(found, key, val)
							changed = True
			if changed:
				doc.save(ignore_permissions=True)
	finally:
		frappe.flags.in_patch = was_patch
	frappe.db.commit()


def ensure_company_custom_fields():
	"""Mirror Hub flags on ERPNext Company (Custom Field)."""
	meta = frappe.get_meta("Company")
	existing = {df.fieldname for df in meta.fields}
	insert_after = "abbr" if "abbr" in existing else None
	if not insert_after:
		insert_after = meta.fields[-1].fieldname if meta.fields else "company_name"
	specs = [
		{
			"fieldname": "centypack_disabled",
			"label": "CentyPack Disabled",
			"fieldtype": "Check",
			"default": "0",
			"description": "When set, CentyPack DocTypes are hidden for non–System Manager users (mirrors Pay Hub kill switch).",
		},
		{
			"fieldname": "centyhq_business_id",
			"label": "CentyHQ Business ID",
			"fieldtype": "Data",
			"description": "Optional correlation key to the Pay Hub business row.",
		},
		{
			"fieldname": "centypack_hub_industry",
			"label": "CentyPack Hub Industry Slug",
			"fieldtype": "Data",
			"description": "When set to a value other than agriculture (case-insensitive), CentyPack is blocked for non–System Manager users.",
		},
		{
			"fieldname": "centypack_trace_public_base_url",
			"label": "CentyPack Trace Public Base URL",
			"fieldtype": "Data",
			"description": "Optional HTTPS origin for consumer trace links (e.g. https://trace.example.com). If empty, Pack Session stores a relative /trace?t=… URL.",
		},
	]
	for spec in specs:
		fn = spec["fieldname"]
		if fn in existing:
			continue
		row = {
			"doctype": "Custom Field",
			"dt": "Company",
			"fieldname": fn,
			"label": spec["label"],
			"fieldtype": spec["fieldtype"],
			"insert_after": insert_after,
		}
		if spec.get("default") is not None:
			row["default"] = spec["default"]
		if spec.get("description"):
			row["description"] = spec["description"]
		frappe.get_doc(row).insert(ignore_permissions=True)
		insert_after = fn
		existing.add(fn)
	frappe.db.commit()
	frappe.clear_cache(doctype="Company")


def seed_grading_masters_if_empty():
	if frappe.db.exists("CentyPack Grade", {"grade_code": "CLASS-A"}):
		return
	for row in (
		{"grade_code": "CLASS-A", "label": "Class A", "sort_order": 10},
		{"grade_code": "CLASS-B", "label": "Class B", "sort_order": 20},
		{"grade_code": "CLASS-C", "label": "Class C", "sort_order": 30},
	):
		frappe.get_doc({"doctype": "CentyPack Grade", **row, "active": 1}).insert(ignore_permissions=True)
	if not frappe.db.exists("CentyPack Defect Type", {"defect_code": "BRUISING"}):
		frappe.get_doc(
			{
				"doctype": "CentyPack Defect Type",
				"defect_code": "BRUISING",
				"label": "Bruising",
				"active": 1,
			}
		).insert(ignore_permissions=True)
	frappe.db.commit()


def seed_masters_if_empty():
	if frappe.db.exists("Crop", "CHILI"):
		return

	crop = frappe.get_doc(
		{
			"doctype": "Crop",
			"crop_name": "CHILI",
			"shelf_life_days": 14,
			"active": 1,
		}
	)
	crop.insert(ignore_permissions=True)

	variety = frappe.get_doc(
		{
			"doctype": "Variety",
			"variety_name": "Birds Eye",
			"crop": "CHILI",
			"maturity_days": 90,
			"active": 1,
		}
	)
	variety.insert(ignore_permissions=True)

	variety_j = frappe.get_doc(
		{
			"doctype": "Variety",
			"variety_name": "Jalapeño",
			"crop": "CHILI",
			"maturity_days": 95,
			"active": 1,
		}
	)
	variety_j.insert(ignore_permissions=True)

	carton = frappe.get_doc(
		{
			"doctype": "Carton Type",
			"carton_code": "EXP-5KG",
			"carton_name": "Export 5kg carton",
			"carton_weight_kg": 5.0,
			"units_per_carton": 1,
			"active": 1,
		}
	)
	carton.insert(ignore_permissions=True)

	wc = frappe.get_doc(
		{
			"doctype": "Worker Category",
			"category_name": "Packer",
			"is_piece_rate": 1,
			"active": 1,
		}
	)
	wc.insert(ignore_permissions=True)

	farmer = frappe.get_doc(
		{
			"doctype": "Farmer",
			"farmer_code": "FRM-SAMPLE-001",
			"farmer_name": "Sample Contract Farmer",
			"farmer_type": "Contract",
			"phone": "+254712000001",
			"mpesa_number": "+254712000001",
			"globalgap_number": "GG-POC-001",
			"active": 1,
		}
	)
	farmer.insert(ignore_permissions=True)

	farm = frappe.get_doc(
		{
			"doctype": "Farm",
			"farm_code": "FARM-SAMPLE-001",
			"farm_name": "Sample Farm Kiambu",
			"farmer": "FRM-SAMPLE-001",
			"county": "Kiambu",
			"location_note": "-1.1743, 36.9378",
			"size_ha": 2.5,
			"active": 1,
		}
	)
	farm.insert(ignore_permissions=True)

	today = frappe.utils.today()
	block = frappe.get_doc(
		{
			"doctype": "Block",
			"block_code": "BLK-SAMPLE-001",
			"block_name": "Block A North",
			"farm": "FARM-SAMPLE-001",
			"farmer": "FRM-SAMPLE-001",
			"crop": "CHILI",
			"variety": "Birds Eye",
			"size_ha": 0.8,
			"planting_date": frappe.utils.add_days(today, -120),
			"expected_first_harvest": frappe.utils.add_days(today, -30),
			"active": 1,
		}
	)
	block.insert(ignore_permissions=True)

	frappe.db.commit()
