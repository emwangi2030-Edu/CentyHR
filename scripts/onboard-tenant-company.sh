#!/usr/bin/env bash
# CentyHR — onboard a new ERPNext Company AND Pay Hub business for a tenant.
#
# Creates (idempotent):
#   ERP side:
#     1. Currency enabled (if disabled)
#     2. Company (country, default_currency, abbr) with chart of accounts
#     3. User Permission: user can access this Company in ERPNext
#     4. Employee row for the user inside the new Company (skipped silently
#        if the user already has an Employee elsewhere - ERPNext enforces
#        one Employee per user globally)
#   Pay Hub side (when --payhub-pm2 <name> is given OR DATABASE_URL is set):
#     5. businesses row (Tanzanian-style defaults; kyc_status=approved; sandbox)
#     6. user_business_memberships row (role=admin, status=active) so the
#        business appears in the user's tenant switcher
#
# Never echoes secrets. Frappe API keys come from BFF .env. Pay Hub DB URL is
# read from a pm2 process env (recommended) or DATABASE_URL.
#
# Usage:
#   onboard-tenant-company.sh \
#     --company "Upeo TZ Demo" --abbr UTZD \
#     --country Tanzania --currency TZS \
#     --user edwin@upeo.co.ke --employee-name "Edwin Njuguna" \
#     --payhub-pm2 payhub-staging
#
# Optional env:
#   ENV_FILE        path to BFF .env (default: /opt/centy-hr-integration/bff/.env)
#   ERP_BASE_URL, HR_ERP_API_KEY, HR_ERP_API_SECRET   override .env
#   DATABASE_URL    Pay Hub Postgres URL (overrides --payhub-pm2)
#   DRY_RUN=1       print intended actions only
set -euo pipefail

ENV_FILE="${ENV_FILE:-/opt/centy-hr-integration/bff/.env}"

usage() {
  sed -n '2,30p' "$0" >&2
  exit 1
}

COMPANY=""; ABBR=""; COUNTRY=""; CURRENCY=""; USER_EMAIL=""; EMP_DISPLAY_NAME=""
PAYHUB_PM2=""; PAYHUB_BUSINESS_ROLE="admin"; PAYHUB_SKIP="0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --company) COMPANY="$2"; shift 2 ;;
    --abbr) ABBR="$2"; shift 2 ;;
    --country) COUNTRY="$2"; shift 2 ;;
    --currency) CURRENCY="$2"; shift 2 ;;
    --user) USER_EMAIL="$2"; shift 2 ;;
    --employee-name) EMP_DISPLAY_NAME="$2"; shift 2 ;;
    --payhub-pm2) PAYHUB_PM2="$2"; shift 2 ;;
    --payhub-role) PAYHUB_BUSINESS_ROLE="$2"; shift 2 ;;
    --skip-payhub) PAYHUB_SKIP="1"; shift ;;
    -h|--help) usage ;;
    *) echo "Unknown arg: $1" >&2; usage ;;
  esac
done

for v in COMPANY ABBR COUNTRY CURRENCY USER_EMAIL; do
  if [[ -z "${!v}" ]]; then echo "Missing --${v,,}" >&2; usage; fi
done

if [[ -z "${ERP_BASE_URL:-}" || -z "${HR_ERP_API_KEY:-}" || -z "${HR_ERP_API_SECRET:-}" ]]; then
  [[ -r "$ENV_FILE" ]] || { echo "Cannot read env file: $ENV_FILE" >&2; exit 1; }
  ERP_BASE_URL="${ERP_BASE_URL:-$(grep -E '^ERP_BASE_URL=' "$ENV_FILE" | tail -1 | cut -d= -f2-)}"
  HR_ERP_API_KEY="${HR_ERP_API_KEY:-$(grep -E '^HR_ERP_API_KEY=|^ERP_API_KEY=' "$ENV_FILE" | tail -1 | cut -d= -f2-)}"
  HR_ERP_API_SECRET="${HR_ERP_API_SECRET:-$(grep -E '^HR_ERP_API_SECRET=|^ERP_API_SECRET=' "$ENV_FILE" | tail -1 | cut -d= -f2-)}"
fi
[[ -n "${ERP_BASE_URL:-}" && -n "${HR_ERP_API_KEY:-}" && -n "${HR_ERP_API_SECRET:-}" ]] \
  || { echo "Missing ERP_BASE_URL or Frappe API credentials." >&2; exit 1; }

AUTH="Authorization: token ${HR_ERP_API_KEY}:${HR_ERP_API_SECRET}"

say(){ printf '\n>>> %s\n' "$*"; }
jq_pick(){ python3 -c "import sys,json; d=json.load(sys.stdin); v=(d.get('message') or {}); print(v.get(sys.argv[1],'') if isinstance(v,dict) else '')" "$1"; }
jq_msg_name(){ python3 -c "import sys,json; d=json.load(sys.stdin); v=(d.get('message') or {}); print((v.get('name') or '').strip())"; }
api_get_value(){
  local doctype="$1" filters="$2" fieldname="${3:-name}"
  curl -sS -H "$AUTH" --get "$ERP_BASE_URL/api/method/frappe.client.get_value" \
    --data-urlencode "doctype=$doctype" \
    --data-urlencode "filters=$filters" \
    --data-urlencode "fieldname=$fieldname"
}
api_insert(){
  local payload="$1"
  curl -sS -H "$AUTH" -H "Content-Type: application/json" \
    -X POST "$ERP_BASE_URL/api/method/frappe.client.insert" -d "$payload"
}
api_set_value(){
  local doctype="$1" name="$2" fieldname="$3" value="$4"
  curl -sS -H "$AUTH" -H "Content-Type: application/json" \
    -X POST "$ERP_BASE_URL/api/method/frappe.client.set_value" \
    -d "{\"doctype\":\"$doctype\",\"name\":\"$name\",\"fieldname\":\"$fieldname\",\"value\":$value}"
}

EMP_DISPLAY_NAME="${EMP_DISPLAY_NAME:-${USER_EMAIL%@*}}"
EMP_FIRST="${EMP_DISPLAY_NAME%% *}"
EMP_LAST="${EMP_DISPLAY_NAME##* }"
[[ "$EMP_FIRST" == "$EMP_LAST" ]] && EMP_LAST=""

say "Target: $COMPANY ($ABBR) — $COUNTRY/$CURRENCY for $USER_EMAIL ($EMP_DISPLAY_NAME)"
say "ERP: $ERP_BASE_URL"
[[ "${DRY_RUN:-0}" == "1" ]] && { echo "DRY_RUN=1, exiting."; exit 0; }

say "1) Ensure currency $CURRENCY is enabled"
CUR_NAME=$(api_get_value "Currency" "{\"name\":\"$CURRENCY\"}" "name" | jq_msg_name)
if [[ -z "$CUR_NAME" ]]; then
  echo "Currency $CURRENCY not found in ERP. Aborting (add the Currency master first)."; exit 2
fi
CUR_ENABLED=$(api_get_value "Currency" "{\"name\":\"$CURRENCY\"}" "enabled" | python3 -c "import sys,json; d=json.load(sys.stdin); v=(d.get('message') or {}); print(v.get('enabled',0))")
if [[ "$CUR_ENABLED" != "1" ]]; then
  echo "Enabling Currency $CURRENCY"
  api_set_value "Currency" "$CURRENCY" "enabled" "1" >/dev/null
else
  echo "Currency $CURRENCY already enabled"
fi

say "2) Ensure Company $COMPANY"
COMPANY_DOC=$(api_get_value "Company" "{\"company_name\":\"$COMPANY\"}" "name" | jq_msg_name)
if [[ -n "$COMPANY_DOC" ]]; then
  echo "Company already exists: $COMPANY_DOC"
else
  payload=$(python3 -c "import json,sys; print(json.dumps({'doc':{'doctype':'Company','company_name':sys.argv[1],'abbr':sys.argv[2],'country':sys.argv[3],'default_currency':sys.argv[4]}}))" "$COMPANY" "$ABBR" "$COUNTRY" "$CURRENCY")
  resp=$(api_insert "$payload")
  echo "$resp" | python3 -m json.tool
  COMPANY_DOC=$(echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); m=d.get('message',{}); print((m.get('name') if isinstance(m,dict) else m) or '')")
  [[ -n "$COMPANY_DOC" ]] || { echo "Company create failed" >&2; exit 3; }
fi

say "3) Ensure User Permission ($USER_EMAIL -> Company:$COMPANY_DOC)"
PERM_NAME=$(api_get_value "User Permission" "{\"user\":\"$USER_EMAIL\",\"allow\":\"Company\",\"for_value\":\"$COMPANY_DOC\"}" "name" | jq_msg_name)
if [[ -n "$PERM_NAME" ]]; then
  echo "User Permission exists: $PERM_NAME"
else
  payload=$(python3 -c "import json,sys; print(json.dumps({'doc':{'doctype':'User Permission','user':sys.argv[1],'allow':'Company','for_value':sys.argv[2],'is_default':0,'apply_to_all_doctypes':1}}))" "$USER_EMAIL" "$COMPANY_DOC")
  api_insert "$payload" | python3 -m json.tool
fi

say "4) Ensure Employee row for $USER_EMAIL inside $COMPANY_DOC"
EMP_NAME=$(api_get_value "Employee" "{\"user_id\":\"$USER_EMAIL\",\"company\":\"$COMPANY_DOC\"}" "name" | jq_msg_name)
if [[ -n "$EMP_NAME" ]]; then
  echo "Employee already exists in $COMPANY_DOC: $EMP_NAME"
else
  TODAY=$(date +%F)
  payload=$(python3 - "$EMP_DISPLAY_NAME" "$EMP_FIRST" "$EMP_LAST" "$USER_EMAIL" "$COMPANY_DOC" "$TODAY" <<'PY'
import json,sys
name, first, last, user, company, today = sys.argv[1:7]
doc = {
  "doctype":"Employee",
  "employee_name": name,
  "first_name": first or name,
  "last_name": last or None,
  "user_id": user,
  "company": company,
  "status": "Active",
  "gender": "Male",
  "date_of_birth": "1990-01-15",
  "date_of_joining": today,
}
print(json.dumps({"doc": {k:v for k,v in doc.items() if v is not None}}))
PY
)
  resp=$(api_insert "$payload")
  if echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); raise SystemExit(0 if 'exception' in d else 1)"; then
    echo "(skipping employee: $(echo "$resp" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('exception','error'))"))"
  else
    echo "$resp" | python3 -m json.tool
  fi
fi

if [[ "$PAYHUB_SKIP" == "1" ]]; then
  say "Skipping Pay Hub seeding (--skip-payhub)."
  echo "Done. ERP side ready for '$COMPANY_DOC'."
  exit 0
fi

# --- Pay Hub side ---
if [[ -z "${DATABASE_URL:-}" && -n "$PAYHUB_PM2" ]]; then
  DATABASE_URL=$(pm2 jlist 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for p in data:
  if p.get('name') == sys.argv[1]:
    env = (p.get('pm2_env') or {}).get('env') or {}
    print(env.get('DATABASE_URL') or env.get('DATABASE_URL_RUNTIME') or '')
    break
" "$PAYHUB_PM2")
fi
if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "No Pay Hub DATABASE_URL available; skipping Pay Hub seed." >&2
  echo "Done. ERP side ready for '$COMPANY_DOC'."
  exit 0
fi
# Normalize Node 'pg' sslmode values that psql does not accept.
DATABASE_URL=$(python3 -c "
import sys, urllib.parse as u
url = sys.argv[1]
parts = u.urlparse(url)
q = dict(u.parse_qsl(parts.query, keep_blank_values=True))
m = (q.get('sslmode') or '').lower()
if m in ('no-verify','noverify'):
  q['sslmode'] = 'require'
parts = parts._replace(query=u.urlencode(q))
print(u.urlunparse(parts))
" "$DATABASE_URL")
export PGOPTIONS='--client-min-messages=warning'
export PGCONNECT_TIMEOUT=10

say "5) Ensure Pay Hub businesses row for '$COMPANY' (owner=$USER_EMAIL)"
PAYHUB_USER_ID=$(psql "$DATABASE_URL" -At -c "SELECT id FROM users WHERE email = '${USER_EMAIL//\'/}' LIMIT 1;")
if [[ -z "$PAYHUB_USER_ID" ]]; then
  echo "Pay Hub user not found for $USER_EMAIL — cannot link tenant." >&2
  echo "Done. ERP side ready; Pay Hub side incomplete." >&2
  exit 4
fi
echo "Pay Hub user id: $PAYHUB_USER_ID"

uuid_re='[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}'
BIZ_ID=$(psql "$DATABASE_URL" -qAtX -c "SELECT id FROM businesses WHERE business_name = '${COMPANY//\'/}' LIMIT 1;" 2>/dev/null | grep -Eo "$uuid_re" | head -1 || true)
if [[ -n "$BIZ_ID" ]]; then
  echo "Pay Hub business already exists: $BIZ_ID"
else
  NEW_BIZ_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
  psql "$DATABASE_URL" -qAtX -c "
    INSERT INTO businesses (id, user_id, business_name, country, kyc_status, otp_method, is_sandbox)
    VALUES ('$NEW_BIZ_ID', '$PAYHUB_USER_ID', '${COMPANY//\'/}', '${COUNTRY//\'/}', 'approved', 'email', true);" >/dev/null
  BIZ_ID="$NEW_BIZ_ID"
  echo "Created Pay Hub business: $BIZ_ID"
fi

say "6) Ensure user_business_memberships ($USER_EMAIL -> $COMPANY, role=$PAYHUB_BUSINESS_ROLE)"
NEW_MEM_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
psql "$DATABASE_URL" -qAtX -c "
  INSERT INTO user_business_memberships (id, user_id, business_id, role, membership_status)
  VALUES ('$NEW_MEM_ID', '$PAYHUB_USER_ID', '$BIZ_ID', '${PAYHUB_BUSINESS_ROLE//\'/}', 'active')
  ON CONFLICT (user_id, business_id) DO UPDATE SET
    role = EXCLUDED.role,
    membership_status = 'active';" >/dev/null
psql "$DATABASE_URL" -qAtX -c "
  SELECT id, role, membership_status FROM user_business_memberships
  WHERE user_id = '$PAYHUB_USER_ID' AND business_id = '$BIZ_ID';"

say "Done. $USER_EMAIL can now switch to '$COMPANY' on staging (Pay Hub + ERP)."
