import frappe
from frappe.model.document import Document


class CentyPackProductDailyPrice(Document):
    def validate(self):
        duplicate = frappe.db.exists(
            "CentyPack Product Daily Price",
            {
                "name": ["!=", self.name],
                "product": self.product,
                "price_date": self.price_date,
                "active": 1,
            },
        )
        if duplicate:
            frappe.throw("An active buying rate already exists for this product on this date.")
