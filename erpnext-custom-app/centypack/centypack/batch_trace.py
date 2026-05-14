import frappe
from frappe import _


def _flt(v):
    return float(v or 0)


def resolve_batch_control(batch_no, company):
    name = frappe.db.get_value(
        "CentyPack Batch Control",
        {"batch_no": batch_no, "company": company},
        "name",
    )
    if not name:
        frappe.throw(
            _("Batch {0} has no CentyPack Batch Control record for company {1}.").format(batch_no, company)
        )
    return frappe.get_doc("CentyPack Batch Control", name)


def compute_packed_weight_kg(row):
    if row.get("packed_weight_kg") not in (None, ""):
        return _flt(row.get("packed_weight_kg"))

    carton_type = row.get("carton_type")
    if carton_type:
        carton_kg = frappe.db.get_value("Carton Type", carton_type, "carton_weight_kg")
        if carton_kg is not None:
            return _flt(row.get("qty")) * _flt(carton_kg)

    frappe.throw(
        _("Set Packed Weight Kg or provide Carton Type with carton weight on batch line {0}.").format(
            row.get("idx")
        )
    )


def apply_pack_line_to_batch_control(company, row, sign=1):
    batch_no = row.get("batch_no")
    if not batch_no:
        return

    packed_kg = compute_packed_weight_kg(row)
    rejected_kg = _flt(row.get("rejected_weight_kg"))
    returned_kg = _flt(row.get("returned_to_stock_weight_kg"))

    if packed_kg < 0 or rejected_kg < 0 or returned_kg < 0:
        frappe.throw(_("Packed, rejected, and returned weights must be non-negative."))

    ctrl = resolve_batch_control(batch_no=batch_no, company=company)

    next_packed = _flt(ctrl.packed_kg) + (sign * packed_kg)
    next_rejected = _flt(ctrl.rejected_kg) + (sign * rejected_kg)
    next_returned = _flt(ctrl.returned_to_stock_kg) + (sign * returned_kg)

    if min(next_packed, next_rejected, next_returned) < -0.0001:
        frappe.throw(
            _("Batch {0}: reversal would make totals negative.").format(batch_no)
        )

    packhouse_kg = _flt(ctrl.packhouse_weight_kg)

    # Track operational classifications while preventing over-allocation from intake mass.
    if (next_packed + next_rejected + next_returned) - packhouse_kg > 0.0001:
        frappe.throw(
            _("Batch {0} exceeds packhouse kg. Packed + Rejected + Returned cannot exceed {1}.").format(
                batch_no, packhouse_kg
            )
        )

    available = packhouse_kg - next_packed - next_rejected
    if available < -0.0001:
        frappe.throw(
            _("Batch {0} over-consumed. Packed + Rejected cannot exceed packhouse kg.").format(batch_no)
        )

    ctrl.db_set(
        {
            "packed_kg": max(0, next_packed),
            "rejected_kg": max(0, next_rejected),
            "returned_to_stock_kg": max(0, next_returned),
            "available_kg": max(0, available),
        },
        update_modified=False,
    )
