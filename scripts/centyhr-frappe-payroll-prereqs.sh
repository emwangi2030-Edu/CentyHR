#!/usr/bin/env bash
# CentyHR — Ubuntu 22.04 prerequisites for Frappe v15 + HRMS + Kenya payroll stack.
# Run as root on a NEW VPS (do not run on a Zimbra/mail server).
# Usage: sudo bash scripts/centyhr-frappe-payroll-prereqs.sh
set -euo pipefail

if [[ "$(id -u)" -ne 0 ]]; then
  echo "Run as root: sudo $0" >&2
  exit 1
fi

. /etc/os-release
if [[ "${VERSION_ID:-}" != "22.04" ]]; then
  echo "Warning: This script targets Ubuntu 22.04 LTS. Detected: ${PRETTY_NAME:-unknown}" >&2
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get upgrade -y
apt-get install -y \
  git python3-dev python3-setuptools python3-pip python3-venv \
  mariadb-server mariadb-client redis-server \
  nginx curl software-properties-common libffi-dev libssl-dev \
  xvfb libfontconfig fontconfig libxrender1

echo "Installing wkhtmltopdf 0.12.6 (jammy amd64)…"
WK_DEB="/tmp/wkhtmltox_0.12.6.1-2.jammy_amd64.deb"
if [[ ! -f "$WK_DEB" ]]; then
  wget -qO "$WK_DEB" \
    "https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6.1-2/wkhtmltox_0.12.6.1-2.jammy_amd64.deb"
fi
dpkg -i "$WK_DEB" || apt-get --fix-broken install -y

if ! id frappe &>/dev/null; then
  echo "Creating user frappe (add to sudo as needed)…"
  adduser --disabled-password --gecos "" frappe || true
  usermod -aG sudo frappe
fi

echo ""
echo "== Next steps (manual) =="
echo "1) mysql_secure_installation"
echo "2) Add charset under [mysqld] in MariaDB config (see docs/CENTYHR_BENCH_INSTALL_RUNBOOK.md)"
echo "3) sudo pip3 install frappe-bench"
echo "4) su - frappe — then nvm install 18, yarn global, bench init (see runbook)"
echo ""
