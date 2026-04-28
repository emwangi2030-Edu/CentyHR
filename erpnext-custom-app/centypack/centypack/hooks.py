from . import __version__ as app_version

import centypack.api.hub_mirror  # noqa: F401 — register @frappe.whitelist hub mirror API
import centypack.api.pricing  # noqa: F401 — register @frappe.whitelist pricing API
from centypack.permissions import CENTYPACK_GATED_DOCTYPES, build_doc_events

app_name = "centypack"
app_title = "CentyPack"
app_publisher = "CentyHQ"
app_description = "Fresh produce packhouse POC"
app_email = "info@centyhq.com"
app_license = "MIT"

required_apps = ["frappe", "erpnext"]

before_install = "centypack.install.before_install"
after_install = "centypack.install.after_install"
after_migrate = "centypack.install.after_migrate"

_HP = "centypack.permissions.centypack_doc_has_permission"
_PQ = "centypack.permissions.centypack_permission_query_conditions"

has_permission = {dt: _HP for dt in CENTYPACK_GATED_DOCTYPES}
permission_query_conditions = {dt: _PQ for dt in CENTYPACK_GATED_DOCTYPES}
doc_events = build_doc_events()
