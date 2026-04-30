from . import __version__

app_name = "centy_company_guard"
app_title = "Centy Company Guard"
app_publisher = "Centy Capital"
app_description = "Expense Claim: Company must match Employee and Expense Approver (tenant boundary)."
app_email = "dev@centycapital.co.ke"
app_license = "MIT"
app_version = __version__

doc_events = {
    "Expense Claim": {
        "validate": "centy_company_guard.events.expense_claim.validate",
    },
}

scheduler_events = {
    "daily": [
        "centy_company_guard.holidays.annual_sync_all_countries",
    ],
}

after_install = "centy_company_guard.install.after_install"
