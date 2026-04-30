"""
End-to-end smoke tests scoped to Company **Upeo Tech** and user **edwin@upeo.co.ke**.

Run on the bench host::

    bench --site erp.tarakilishicloud.com execute centypack.e2e_upeo.run_all

Optional kwargs::

    hub_business_id — UUID on Pay Hub `businesses.id` for Upeo Tech (defaults to staging POC value).

The script:
- Sets Edwin's default **Company** to Upeo Tech and ensures **CentyPack POC Admin** role.
- Aligns **Company** custom fields for Hub mirror testing.
- Ensures stock for **SKU001** in **Stores - UPEO**, then submits a **CentyPack GDN** (internal transfer).
- Creates **CentyPack Grading Run**, **CentyPack Pack Session** (MR on submit), and **CentyPack Production Day** with auto roll-up.
- Verifies **edwin@upeo.co.ke** passes CentyPack gate and can read **CentyPack GDN**.
"""

import frappe
from frappe.utils import today

from centypack.permissions import centypack_gate_allowed

COMPANY = "Upeo Tech"
USER = "edwin@upeo.co.ke"
WH_STORES = "Stores - UPEO"
WH_FINISHED = "Finished Goods - UPEO"
ITEM = "SKU001"
TAG = "E2E-UPEO-TECH"

DEFAULT_HUB_BUSINESS_ID = "f0a93809-2e86-4d94-aa3c-c60ebb34263d"


def _cleanup_tagged():
	for dt in ("CentyPack Production Day", "CentyPack Pack Session", "CentyPack Grading Run", "CentyPack GDN"):
		names = frappe.get_all(
			dt,
			filters={"company": COMPANY, "remarks": ("like", f"%{TAG}%")},
			pluck="name",
		)
		for name in names:
			try:
				doc = frappe.get_doc(dt, name)
				if getattr(doc, "docstatus", 0) == 1:
					doc.flags.ignore_permissions = True
					doc.cancel()
				frappe.delete_doc(dt, name, force=True, ignore_permissions=True)
			except Exception:
				frappe.db.rollback()


def configure_test_user(hub_business_id=None):
	hub_business_id = (hub_business_id or DEFAULT_HUB_BUSINESS_ID).strip()
	frappe.defaults.set_user_default("Company", COMPANY, user=USER)
	u = frappe.get_doc("User", USER)
	if not any(getattr(r, "role", None) == "CentyPack POC Admin" for r in (u.roles or [])):
		u.append("roles", {"role": "CentyPack POC Admin"})
		u.save(ignore_permissions=True)
	frappe.db.set_value(
		"Company",
		COMPANY,
		{
			"centyhq_business_id": hub_business_id,
			"centypack_disabled": 0,
			"centypack_hub_industry": "agriculture",
		},
		update_modified=False,
	)
	frappe.db.commit()
	frappe.clear_cache()
	return {"user": USER, "company": COMPANY, "centyhq_business_id": hub_business_id}


def _ensure_stock(qty=40.0):
	frappe.set_user("Administrator")
	uom = frappe.db.get_value("Item", ITEM, "stock_uom") or "Nos"
	rows = frappe.db.sql(
		"SELECT COALESCE(actual_qty,0) FROM `tabBin` WHERE item_code=%s AND warehouse=%s LIMIT 1",
		(ITEM, WH_STORES),
	)
	if rows and float(rows[0][0] or 0) >= 5:
		return {"stock": float(rows[0][0]), "skipped_mr": True}
	se = frappe.get_doc(
		{
			"doctype": "Stock Entry",
			"stock_entry_type": "Material Receipt",
			"company": COMPANY,
			"to_warehouse": WH_STORES,
			"posting_date": today(),
			"remarks": f"{TAG} seed stock",
			"items": [
				{
					"item_code": ITEM,
					"qty": qty,
					"uom": uom,
					"stock_uom": uom,
					"conversion_factor": 1,
					"t_warehouse": WH_STORES,
				}
			],
		}
	)
	se.flags.ignore_permissions = True
	se.insert()
	se.submit()
	return {"stock": qty, "stock_entry": se.name, "skipped_mr": False}


def _gdn_transfer(qty=3.0):
	frappe.set_user("Administrator")
	uom = frappe.db.get_value("Item", ITEM, "stock_uom") or "Nos"
	g = frappe.get_doc(
		{
			"doctype": "CentyPack GDN",
			"company": COMPANY,
			"posting_date": today(),
			"from_warehouse": WH_STORES,
			"to_warehouse": WH_FINISHED,
			"remarks": f"{TAG} internal transfer",
			"items": [{"item_code": ITEM, "qty": qty, "uom": uom}],
		}
	)
	g.flags.ignore_permissions = True
	g.insert()
	g.submit()
	return g.name


def _grading_run():
	frappe.set_user("Administrator")
	if not frappe.db.exists("Block", "BLK-SAMPLE-001"):
		frappe.throw("Missing seed Block BLK-SAMPLE-001 — run centypack.install.seed_masters_if_empty first.")
	gr = frappe.get_doc(
		{
			"doctype": "CentyPack Grading Run",
			"company": COMPANY,
			"posting_date": today(),
			"block": "BLK-SAMPLE-001",
			"remarks": f"{TAG} grading",
			"lines": [
				{
					"grade": "CLASS-A",
					"defect_type": "BRUISING",
					"quantity_kg": 7.5,
					"notes": TAG,
				}
			],
		}
	)
	gr.flags.ignore_permissions = True
	gr.insert()
	return gr.name


def _pack_session():
	frappe.set_user("Administrator")
	uom = frappe.db.get_value("Item", ITEM, "stock_uom") or "Nos"
	ps = frappe.get_doc(
		{
			"doctype": "CentyPack Pack Session",
			"company": COMPANY,
			"posting_date": today(),
			"warehouse": WH_STORES,
			"remarks": f"{TAG} pack",
			"items": [{"item_code": ITEM, "qty": 2, "uom": uom}],
		}
	)
	ps.flags.ignore_permissions = True
	ps.insert()
	ps.submit()
	return ps.name


def _production_day_rollup():
	frappe.set_user("Administrator")
	ld = today()
	existing = frappe.db.get_value(
		"CentyPack Production Day",
		{"company": COMPANY, "log_date": ld, "remarks": ("like", f"%{TAG}%")},
		"name",
	)
	if existing:
		frappe.delete_doc("CentyPack Production Day", existing, force=True, ignore_permissions=True)
	pd = frappe.get_doc(
		{
			"doctype": "CentyPack Production Day",
			"company": COMPANY,
			"log_date": ld,
			"auto_rollup": 1,
			"remarks": f"{TAG} rollup {ld}",
		}
	)
	pd.flags.ignore_permissions = True
	pd.insert()
	return pd.name


def _assert_edwin_gate():
	frappe.set_user(USER)
	co = frappe.db.get_value(
		"DefaultValue",
		{"parent": USER, "defkey": "Company"},
		"defvalue",
	)
	return {
		"default_company": co,
		"centypack_gate_allowed": centypack_gate_allowed(USER),
		"has_perm_gdn_read": frappe.has_permission("CentyPack GDN", "read"),
	}


def run_all(hub_business_id=None):
	out = {"configure": configure_test_user(hub_business_id)}
	_cleanup_tagged()
	out["stock"] = _ensure_stock()
	out["gdn"] = _gdn_transfer()
	out["grading_run"] = _grading_run()
	out["pack_session"] = _pack_session()
	out["production_day"] = _production_day_rollup()
	pd = frappe.get_doc("CentyPack Production Day", out["production_day"])
	out["rollup_totals"] = {
		"kg_graded": pd.kg_graded,
		"cartons_packed": pd.cartons_packed,
		"gdn_count": pd.gdn_count,
		"pack_sessions_count": pd.pack_sessions_count,
	}
	out["edwin_checks"] = _assert_edwin_gate()
	frappe.db.commit()
	return out
