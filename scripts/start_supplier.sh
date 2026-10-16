#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
fi
. .venv/bin/activate
python -m pip install -e .
exec uvicorn supplier.app:app --host "${SUPPLIER_HOST:-0.0.0.0}" --port "${SUPPLIER_PORT:-8090}"

