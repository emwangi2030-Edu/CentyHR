#!/usr/bin/env bash
# Phase B: install centy_company_guard on Docker ERPNext (run on the ERP host as root).
set -euo pipefail
BENCH_CONTAINER="${BENCH_CONTAINER:-erpnext-backend-1}"
SITE="${SITE:-erp.tarakilishicloud.com}"
SRC="${1:-/opt/centy-hr-integration/erpnext-custom-app/centy_company_guard}"

if [[ ! -d "$SRC/centy_company_guard" ]]; then
  echo "Missing app at $SRC — copy centy_company_guard there first."
  exit 1
fi

docker cp "$SRC" "$BENCH_CONTAINER:/home/frappe/frappe-bench/apps/centy_company_guard"
docker exec -u root "$BENCH_CONTAINER" bash -lc \
  "chown -R frappe:frappe /home/frappe/frappe-bench/apps/centy_company_guard"
# Frappe resolves apps via sites/apps.txt (see frappe.get_all_apps). Append only — never overwrite.
docker exec "$BENCH_CONTAINER" bash -lc "
  set -e
  BENCH=/home/frappe/frappe-bench
  SITES_APPS=\"\$BENCH/sites/apps.txt\"
  cd \"\$BENCH\" &&
  ./env/bin/pip install -e apps/centy_company_guard &&
  test -f \"\$SITES_APPS\" &&
  grep -qxF 'centy_company_guard' \"\$SITES_APPS\" || sed -i '\$a centy_company_guard' \"\$SITES_APPS\" &&
  bench --site '$SITE' install-app centy_company_guard --force 2>/dev/null || bench --site '$SITE' install-app centy_company_guard
"
docker exec "$BENCH_CONTAINER" bash -lc "cd /home/frappe/frappe-bench && bench restart"
# bench restart alone can leave old Gunicorn workers; recycle the container so imports match the new venv.
docker restart "$BENCH_CONTAINER"
echo "Waiting for backend to listen…"
for i in $(seq 1 30); do
  if docker exec "$BENCH_CONTAINER" bash -lc "curl -sf -o /dev/null -H \"Host: $SITE\" http://127.0.0.1:8000/api/method/ping" 2>/dev/null; then
    break
  fi
  sleep 1
done
echo "Installed centy_company_guard on $SITE"
