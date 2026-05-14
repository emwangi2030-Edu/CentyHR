frappe.query_reports["CentyPack Batch Traceability"] = {
  filters: [
    {
      fieldname: "batch_no",
      label: __("Batch"),
      fieldtype: "Link",
      options: "Batch",
    },
    {
      fieldname: "customer",
      label: __("Customer"),
      fieldtype: "Link",
      options: "Customer",
    },
    {
      fieldname: "from_date",
      label: __("From Date"),
      fieldtype: "Date",
    },
    {
      fieldname: "to_date",
      label: __("To Date"),
      fieldtype: "Date",
    },
  ],
};
