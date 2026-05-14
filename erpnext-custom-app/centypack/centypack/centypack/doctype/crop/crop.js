frappe.ui.form.on("Crop", {
  refresh(frm) {
    frm.trigger("refresh_effective_rate");
  },
  effective_rate_date(frm) {
    frm.trigger("refresh_effective_rate");
  },
  refresh_effective_rate(frm) {
    if (!frm.doc.name || frm.is_new()) {
      frm.set_value("effective_buying_rate", null);
      frm.set_value("effective_rate_source", null);
      return;
    }

    frappe.call({
      method: "centypack.api.pricing.get_effective_buying_rate",
      args: {
        product: frm.doc.name,
        price_date: frm.doc.effective_rate_date || frappe.datetime.get_today(),
      },
      callback: (r) => {
        const d = r?.message;
        if (!d) return;
        frm.set_value("effective_buying_rate", d.effective_buying_rate ?? null);
        frm.set_value("effective_rate_source", d.source || null);
      },
    });
  },
});
