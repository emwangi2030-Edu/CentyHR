frappe.query_reports["CentyPack Batch Exceptions"] = {
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
      fieldname: "rejection_threshold_pct",
      label: __("Rejection Threshold %"),
      fieldtype: "Float",
      default: 5,
      reqd: 1,
    },
  ],
};
