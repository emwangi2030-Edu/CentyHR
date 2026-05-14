"""CentyPack Company gate (mirrors Hub) + list/doc permission hooks."""

import frappe
from frappe import _

# DocTypes guarded by Company custom fields + optional hub industry slug.
CENTYPACK_GATED_DOCTYPES = (
	"Crop",
	"Variety",
	"Carton Type",
	"Worker Category",
	"Farmer",
	"Farm",
	"Block",
	"CentyPack Warehouse",
	"CentyPack GDN",
	"CentyPack GDN Item",
	"CentyPack Grade",
	"CentyPack Defect Type",
	"CentyPack Grading Line",
	"CentyPack Grading Run",
	"CentyPack Pack Item",
	"CentyPack Pack Session",
	"CentyPack Batch Control",
	"CentyPack Production Day",
)


def _privileged(user: str | None) -> bool:
	user = user or frappe.session.user
	if not user or user == "Administrator":
		return True
	return "System Manager" in frappe.get_roles(user)


def _default_company(user: str | None) -> str | None:
	user = user or frappe.session.user
	c = frappe.defaults.get_user_default("Company", user=user)
	if c:
		return c
	try:
		return frappe.db.get_single_value("Global Defaults", "default_company")
	except Exception:
		return None


def centypack_gate_allowed(user: str | None = None) -> bool:
	"""Allow CentyPack when default company is not disabled and industry slug allows (if set)."""
	if _privileged(user):
		return True
	user = user or frappe.session.user
	company = _default_company(user)
	if not company:
		return False
	if not frappe.db.has_column("Company", "centypack_disabled"):
		return True
	row = frappe.db.get_value(
		"Company",
		company,
		["centypack_disabled", "centypack_hub_industry"],
		as_dict=True,
	)
	if not row:
		return False
	if row.get("centypack_disabled"):
		return False
	ind = (row.get("centypack_hub_industry") or "").strip().lower()
	if ind and ind != "agriculture":
		return False
	return True


def centypack_doc_has_permission(doc=None, ptype=None, user=None, debug=False):
	"""Controller hook: deny doc access when gate is closed (non–System Manager)."""
	if not doc:
		return None
	if doc.doctype not in CENTYPACK_GATED_DOCTYPES:
		return None
	if _privileged(user):
		return None
	if not centypack_gate_allowed(user):
		return False
	return None


def centypack_permission_query_conditions(user, doctype=None, **kwargs):
	"""Hide all rows in list/report when gate is closed."""
	if _privileged(user):
		return None
	if doctype and doctype not in CENTYPACK_GATED_DOCTYPES:
		return None
	if not centypack_gate_allowed(user):
		return "(1=0)"
	return None


def centypack_document_gate(doc, method=None):
	"""Block writes when gate is closed (covers create/update/delete paths)."""
	if not doc or doc.doctype not in CENTYPACK_GATED_DOCTYPES:
		return
	if getattr(frappe.flags, "in_install", None) or getattr(frappe.flags, "in_migrate", False):
		return
	if getattr(frappe.flags, "in_patch", None):
		return
	user = frappe.session.user
	if _privileged(user):
		return
	if not centypack_gate_allowed(user):
		frappe.throw(
			_("CentyPack is not available for this company (disabled or industry not allowed)."),
			frappe.PermissionError,
		)


def _doc_gate_events():
	return {
		"validate": "centypack.permissions.centypack_document_gate",
		"on_trash": "centypack.permissions.centypack_document_gate",
	}


def build_doc_events() -> dict:
	return {dt: _doc_gate_events() for dt in CENTYPACK_GATED_DOCTYPES}
