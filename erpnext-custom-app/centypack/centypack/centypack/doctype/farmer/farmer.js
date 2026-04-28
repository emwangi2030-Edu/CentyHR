frappe.ui.form.on("Farmer", {
  date_of_birth(frm) {
    if (!frm.doc.date_of_birth) {
      frm.set_value("age_years", null);
      return;
    }

    const dob = frappe.datetime.str_to_obj(frm.doc.date_of_birth);
    const today = frappe.datetime.str_to_obj(frappe.datetime.get_today());
    if (!dob || !today) return;

    let age = today.getFullYear() - dob.getFullYear();
    const monthDiff = today.getMonth() - dob.getMonth();
    const dayDiff = today.getDate() - dob.getDate();
    if (monthDiff < 0 || (monthDiff === 0 && dayDiff < 0)) {
      age -= 1;
    }

    frm.set_value("age_years", age >= 0 ? age : null);
  },

  onload(frm) {
    if (frm.doc.date_of_birth && !frm.doc.age_years) {
      frm.trigger("date_of_birth");
    }
  },
});
