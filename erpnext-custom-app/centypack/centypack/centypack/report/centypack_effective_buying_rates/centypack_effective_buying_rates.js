frappe.query_reports["CentyPack Effective Buying Rates"] = {
  filters: [
    {
      fieldname: "price_date",
      label: __("Price Date"),
      fieldtype: "Date",
      default: frappe.datetime.get_today(),
      reqd: 1,
    },
  ],
};
