import frappe
from frappe import _
from frappe.model.document import Document


class CentyPackBatchControl(Document):
    def autoname(self):
        if self.batch_no:
            self.name = self.batch_no

    def validate(self):
        self._validate_weights()
        self._refresh_derived_fields()

    def _validate_weights(self):
        for f in (
            "farm_weight_kg",
            "packhouse_weight_kg",
            "allowable_variance_kg",
            "packed_kg",
            "rejected_kg",
            "returned_to_stock_kg",
        ):
            val = float(getattr(self, f) or 0)
            if val < 0:
                frappe.throw(_("{0} must be non-negative.").format(self.meta.get_label(f)))

        if not self.batch_no:
            frappe.throw(_("Batch No is required."))

        if not self.company:
            frappe.throw(_("Company is required."))

    def _refresh_derived_fields(self):
        farm = float(self.farm_weight_kg or 0)
        packhouse = float(self.packhouse_weight_kg or 0)
        allowable = float(self.allowable_variance_kg or 20)

        variance = packhouse - farm
        self.variance_kg = variance

        status = "Within Tolerance" if abs(variance) <= allowable else "Out of Tolerance"
        self.variance_status = status

        if abs(variance) > allowable:
            frappe.throw(
                _("Batch variance {0}kg exceeds allowable +/-{1}kg.").format(round(variance, 3), allowable)
            )

        packed = float(self.packed_kg or 0)
        rejected = float(self.rejected_kg or 0)
        returned = float(self.returned_to_stock_kg or 0)

        if (packed + rejected + returned) > packhouse + 1e-6:
            frappe.throw(
                _("Packed + Rejected + Returned cannot exceed packhouse weight ({0}kg).").format(packhouse)
            )

        available = packhouse - packed - rejected
        if available < -1e-6:
            frappe.throw(_("Packed + Rejected cannot exceed packhouse weight."))

        self.available_kg = max(0, available)
