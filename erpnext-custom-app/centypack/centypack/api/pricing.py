import frappe


def resolve_effective_buying_rate(product, price_date=None):
    """Resolve effective buying rate as daily override first, else product default minimum."""
    product = (product or "").strip()
    if not product:
        frappe.throw("product is required")

    rate_date = frappe.utils.getdate(price_date) if price_date else frappe.utils.getdate()

    override = frappe.db.get_value(
        "CentyPack Product Daily Price",
        {"product": product, "price_date": rate_date, "active": 1},
        ["buying_rate", "name"],
        as_dict=True,
    )

    default_min = frappe.db.get_value("Crop", product, "default_min_buying_rate")
    if default_min is None:
        frappe.throw(f"Default minimum buying rate is not set for product {product}.")

    if override and override.get("buying_rate") is not None:
        return {
            "product": product,
            "rate_date": str(rate_date),
            "effective_buying_rate": float(override.get("buying_rate")),
            "source": "daily_override",
            "override_record": override.get("name"),
            "default_min_buying_rate": float(default_min),
        }

    return {
        "product": product,
        "rate_date": str(rate_date),
        "effective_buying_rate": float(default_min),
        "source": "default_minimum",
        "override_record": None,
        "default_min_buying_rate": float(default_min),
    }


@frappe.whitelist()
def get_effective_buying_rate(product, price_date=None):
    return resolve_effective_buying_rate(product=product, price_date=price_date)
