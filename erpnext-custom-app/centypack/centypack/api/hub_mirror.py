"""Pay Hub → ERP Company mirror (centyhq_business_id, flags, industry slug for gate)."""

import frappe
from frappe import _
from frappe.utils import cint


def _expect_token(hub_token):
	expected = (frappe.conf.get("centypack_hub_mirror_token") or "").strip()
	if not expected:
		frappe.throw(_("CentyPack hub mirror is not configured (missing centypack_hub_mirror_token in site_config)."))
	if (hub_token or "").strip() != expected:
		frappe.throw(_("Invalid hub_token."), frappe.AuthenticationError)


def _truthy(v):
	if v is True:
		return True
	if v is False or v is None:
		return False
	s = str(v).strip().lower()
	return s in ("1", "true", "yes", "on")


def _resolve_company(business_id, business_name):
	"""Return Company name (PK)."""
	bid = (business_id or "").strip()
	if not bid:
		frappe.throw(_("business_id is required."))

	if frappe.db.has_column("Company", "centyhq_business_id"):
		row = frappe.db.get_value("Company", {"centyhq_business_id": bid}, "name")
		if row:
			return row

	if business_name:
		bn = str(business_name).strip()
		if bn and frappe.db.exists("Company", bn):
			return bn

	frappe.throw(
		_("No ERP Company matched. Set **CentyHQ Business ID** on the Company to {0} or ensure a Company named **{1}** exists.").format(
			bid,
			business_name or "-",
		)
	)


@frappe.whitelist(allow_guest=True, methods=["POST"])
def apply_from_hub(
	hub_token=None,
	business_id=None,
	business_name=None,
	industry_slug=None,
	centypack_disabled=None,
	centypack_beta_enabled=None,
):
	"""
	Pay Hub calls this after CentyPack flags / org change.

	site_config.json::

	    "centypack_hub_mirror_token": "<long random secret>"

	Hub .env::

	    CENTYPACK_ERP_MIRROR_TOKEN=<same secret>
	"""
	_expect_token(hub_token)

	company = _resolve_company(business_id, business_name)

	disabled = 1 if _truthy(centypack_disabled) else 0
	beta = 1 if _truthy(centypack_beta_enabled) else 0
	ind = (industry_slug or "").strip().lower() or None

	if beta:
		hub_ind = "agriculture"
	else:
		hub_ind = ind

	updates = {"centypack_disabled": disabled}
	if frappe.db.has_column("Company", "centyhq_business_id"):
		updates["centyhq_business_id"] = str(business_id).strip()
	if frappe.db.has_column("Company", "centypack_hub_industry"):
		updates["centypack_hub_industry"] = hub_ind or None

	frappe.db.set_value("Company", company, updates)
	frappe.clear_cache(doctype="Company")

	return {"ok": True, "company": company, "centypack_disabled": disabled, "centypack_hub_industry": hub_ind}


@frappe.whitelist(allow_guest=True, methods=["POST"])
def ping(hub_token=None):
	_expect_token(hub_token)
	return {"ok": True, "message": "centypack hub mirror token accepted"}
