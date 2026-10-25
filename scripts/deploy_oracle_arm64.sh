#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -m)" != "aarch64" ]]; then
  echo "warning: intended for Oracle Ampere ARM64 (aarch64); found $(uname -m)" >&2
fi
if [[ ${EUID} -ne 0 ]]; then
  echo "run with sudo" >&2
  exit 2
fi

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
id ume >/dev/null 2>&1 || useradd --system --create-home --home-dir /opt/ume-data-supplier --shell /usr/sbin/nologin ume
install -d -o ume -g ume /opt/ume-data-supplier/data
rsync -a --delete --exclude .git --exclude .venv --exclude .env --exclude data/ "$SOURCE_DIR/" /opt/ume-data-supplier/
chown -R ume:ume /opt/ume-data-supplier
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y python3-venv python3-pip rsync
sudo -u ume python3 -m venv /opt/ume-data-supplier/.venv
sudo -u ume /opt/ume-data-supplier/.venv/bin/pip install --upgrade pip
sudo -u ume /opt/ume-data-supplier/.venv/bin/pip install /opt/ume-data-supplier
if [[ ! -f /etc/ume-data-supplier.env ]]; then
  install -m 600 -o root -g root /opt/ume-data-supplier/.env.example /etc/ume-data-supplier.env
  echo "Created /etc/ume-data-supplier.env; edit it before starting the service."
  exit 3
fi
install -m 644 /opt/ume-data-supplier/deploy/ume-data-supplier.service /etc/systemd/system/ume-data-supplier.service
systemctl daemon-reload
systemctl enable --now ume-data-supplier
systemctl --no-pager --full status ume-data-supplier
