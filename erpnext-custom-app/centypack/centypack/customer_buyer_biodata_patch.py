import frappe
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields
from frappe.custom.doctype.property_setter.property_setter import make_property_setter


def execute():
    custom_fields = {
        "Customer": [
            {
                "fieldname": "buyer_bio_section",
                "label": "Buyer Bio Data",
                "fieldtype": "Section Break",
                "insert_after": "customer_name",
            },
            {
                "fieldname": "buyer_address",
                "label": "Address",
                "fieldtype": "Small Text",
                "insert_after": "buyer_bio_section",
            },
            {
                "fieldname": "buyer_county",
                "label": "County",
                "fieldtype": "Data",
                "insert_after": "buyer_address",
            },
            {
                "fieldname": "buyer_currency",
                "label": "Currency",
                "fieldtype": "Select",
                "options": "EUR\nGBP",
                "reqd": 1,
                "insert_after": "buyer_county",
            },
            {
                "fieldname": "buyer_shipping_terms",
                "label": "Shipping Terms (CIF/FOB)",
                "fieldtype": "Select",
                "options": "CIF\nFOB",
                "reqd": 1,
                "insert_after": "buyer_currency",
            },
            {
                "fieldname": "buyer_unique_id",
                "label": "Customer Unique ID / Code Name",
                "fieldtype": "Data",
                "reqd": 1,
                "insert_after": "buyer_shipping_terms",
                "unique": 1,
            },
            {
                "fieldname": "buyer_certification_section",
                "label": "Certifications",
                "fieldtype": "Section Break",
                "insert_after": "buyer_unique_id",
            },
            {
                "fieldname": "cert_global_gap",
                "label": "Global GaP",
                "fieldtype": "Check",
                "insert_after": "buyer_certification_section",
            },
            {
                "fieldname": "cert_grasp",
                "label": "G.R.A.S.P",
                "fieldtype": "Check",
                "insert_after": "cert_global_gap",
            },
            {
                "fieldname": "cert_smeta",
                "label": "S.M.E.T.A",
                "fieldtype": "Check",
                "insert_after": "cert_grasp",
            },
            {
                "fieldname": "cert_brc",
                "label": "B.R.C",
                "fieldtype": "Check",
                "insert_after": "cert_smeta",
            },
        ]
    }

    create_custom_fields(custom_fields, update=True)

    make_property_setter("Customer", "customer_name", "label", "Name", "Data", for_doctype=False)

    frappe.clear_cache(doctype="Customer")
    frappe.clear_cache()
    frappe.db.commit()
    return "OK: Customer/Buyer biodata patch applied"

def verify():
    keys = [
        "customer_name","buyer_address","buyer_county","buyer_currency","buyer_shipping_terms","buyer_unique_id","cert_global_gap","cert_grasp","cert_smeta","cert_brc"
    ]
    meta = frappe.get_meta("Customer")
    out = {}
    for k in keys:
        f = meta.get_field(k)
        out[k] = None if not f else {
            "label": f.label,
            "fieldtype": f.fieldtype,
            "reqd": f.reqd,
            "options": f.options,
        }
    return out

def upgrade_certifications_to_child_table():
    child_dt = "Buyer Certification Row"

    if not frappe.db.exists("DocType", child_dt):
        doc = frappe.get_doc({
            "doctype": "DocType",
            "name": child_dt,
            "module": "CentyPack",
            "custom": 1,
            "istable": 1,
            "editable_grid": 1,
            "track_changes": 0,
            "fields": [
                {
                    "fieldname": "certification",
                    "label": "Certification",
                    "fieldtype": "Select",
                    "options": "Global GaP\nG.R.A.S.P\nS.M.E.T.A\nB.R.C",
                    "reqd": 1,
                    "in_list_view": 1,
                },
                {
                    "fieldname": "certificate_number",
                    "label": "Certificate Number",
                    "fieldtype": "Data",
                },
                {
                    "fieldname": "valid_until",
                    "label": "Valid Until",
                    "fieldtype": "Date",
                },
                {
                    "fieldname": "notes",
                    "label": "Notes",
                    "fieldtype": "Small Text",
                },
            ],
            "permissions": [{"role": "System Manager", "read": 1, "write": 1, "create": 1, "delete": 1}],
        })
        doc.insert(ignore_permissions=True)

    custom_fields = {
        "Customer": [
            {
                "fieldname": "buyer_certifications_table",
                "label": "Certifications",
                "fieldtype": "Table",
                "options": child_dt,
                "insert_after": "buyer_unique_id",
            }
        ]
    }
    create_custom_fields(custom_fields, update=True)

    # Backfill table rows from existing checkbox flags once.
    legacy_map = {
        "cert_global_gap": "Global GaP",
        "cert_grasp": "G.R.A.S.P",
        "cert_smeta": "S.M.E.T.A",
        "cert_brc": "B.R.C",
    }

    customers = frappe.get_all(
        "Customer",
        fields=["name", *legacy_map.keys()],
        limit_page_length=0,
    )

    for c in customers:
        if frappe.db.count(child_dt, {"parent": c["name"], "parenttype": "Customer", "parentfield": "buyer_certifications_table"}) > 0:
            continue
        rows = []
        for key, label in legacy_map.items():
            if int(c.get(key) or 0) == 1:
                rows.append({"certification": label})
        if not rows:
            continue
        customer = frappe.get_doc("Customer", c["name"])
        for r in rows:
            customer.append("buyer_certifications_table", r)
        customer.save(ignore_permissions=True)

    frappe.clear_cache(doctype="Customer")
    frappe.clear_cache()
    frappe.db.commit()
    return "OK: Buyer certifications upgraded to child table"

def verify_upgrade():
    customer_meta = frappe.get_meta("Customer")
    child_meta = frappe.get_meta("Buyer Certification Row")
    f = customer_meta.get_field("buyer_certifications_table")
    return {
        "buyer_certifications_table": None if not f else {
            "label": f.label,
            "fieldtype": f.fieldtype,
            "options": f.options,
        },
        "child_doctype": {
            "name": child_meta.name,
            "istable": child_meta.istable,
            "fields": [
                {"fieldname": df.fieldname, "fieldtype": df.fieldtype, "options": df.options}
                for df in child_meta.fields
            ],
        },
        "rows_count": frappe.db.count("Buyer Certification Row"),
    }

def cleanup_hide_legacy_cert_checkboxes():
    legacy = ["cert_global_gap", "cert_grasp", "cert_smeta", "cert_brc", "buyer_certification_section"]
    from frappe.custom.doctype.property_setter.property_setter import make_property_setter

    for f in legacy:
        make_property_setter("Customer", f, "hidden", "1", "Check", for_doctype=False)
        make_property_setter("Customer", f, "in_list_view", "0", "Check", for_doctype=False)

    frappe.clear_cache(doctype="Customer")
    frappe.clear_cache()
    frappe.db.commit()
    return "OK: Legacy certification checkboxes hidden"

def verify_cleanup():
    meta = frappe.get_meta("Customer")
    keys = ["buyer_certifications_table", "cert_global_gap", "cert_grasp", "cert_smeta", "cert_brc", "buyer_certification_section"]
    out = {}
    for k in keys:
        f = meta.get_field(k)
        out[k] = None if not f else {
            "fieldtype": f.fieldtype,
            "hidden": f.hidden,
            "in_list_view": f.in_list_view,
            "options": f.options,
        }
    return out
