"""Roll CentyPack Grading / GDN / Pack into Production Day totals."""

import frappe
from frappe.utils import flt


def fill_production_day_totals(doc):
	"""Mutate `doc` in-place from DB aggregates for company + log_date."""
	company = doc.company
	d = doc.log_date
	if not company or not d:
		return

	kg = frappe.db.sql(
		"""
		SELECT COALESCE(SUM(l.quantity_kg), 0)
		FROM `tabCentyPack Grading Line` l
		INNER JOIN `tabCentyPack Grading Run` r ON r.name = l.parent AND l.parenttype = %s
		WHERE r.company = %s AND r.posting_date = %s
		""",
		("CentyPack Grading Run", company, d),
	)[0][0]

	gdn_n = frappe.db.sql(
		"""
		SELECT COUNT(*) FROM `tabCentyPack GDN`
		WHERE company = %s AND posting_date = %s AND docstatus = 1
		""",
		(company, d),
	)[0][0]

	pack_n = frappe.db.sql(
		"""
		SELECT COUNT(*) FROM `tabCentyPack Pack Session`
		WHERE company = %s AND posting_date = %s AND docstatus = 1
		""",
		(company, d),
	)[0][0]

	cartons = frappe.db.sql(
		"""
		SELECT COALESCE(SUM(pi.qty), 0)
		FROM `tabCentyPack Pack Session` ps
		INNER JOIN `tabCentyPack Pack Item` pi ON pi.parent = ps.name AND pi.parenttype = %s
		WHERE ps.company = %s AND ps.posting_date = %s AND ps.docstatus = 1
		""",
		("CentyPack Pack Session", company, d),
	)[0][0]

	doc.kg_graded = flt(kg)
	doc.gdn_count = int(gdn_n or 0)
	doc.pack_sessions_count = int(pack_n or 0)
	doc.cartons_packed = flt(cartons)
