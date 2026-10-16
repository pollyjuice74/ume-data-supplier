#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

cloudflared_version="2026.10.0"
tool_dir="${XDG_CACHE_HOME:-${HOME}/.cache}/ume-data-supplier"
cloudflared_bin="$tool_dir/cloudflared"
machine="$(uname -m)"
case "$machine" in
  x86_64|amd64) asset="cloudflared-linux-amd64"; checksum="d33ff2d14475178d2012c2c56beba87389ac5ded27649519f198a7d3134a99db" ;;
  aarch64|arm64) asset="cloudflared-linux-arm64"; checksum="e6422b9d4f72d3194bc5a38676f13667c06666523217b842a877d72a80b5ac08" ;;
  *) echo "Unsupported architecture: $machine" >&2; exit 1 ;;
esac
if [[ ! -x "$cloudflared_bin" ]]; then
  mkdir -p "$tool_dir"
  curl --fail --location --proto '=https' --tlsv1.2 "https://github.com/cloudflare/cloudflared/releases/download/${cloudflared_version}/${asset}" -o "$cloudflared_bin.download"
  echo "$checksum  $cloudflared_bin.download" | sha256sum --check --status
  mv "$cloudflared_bin.download" "$cloudflared_bin"
  chmod 0755 "$cloudflared_bin"
fi

log_file="$(mktemp)"
supplier_pid=""
tunnel_pid=""
cleanup() {
  [[ -n "$supplier_pid" ]] && kill "$supplier_pid" 2>/dev/null || true
  [[ -n "$tunnel_pid" ]] && kill "$tunnel_pid" 2>/dev/null || true
  rm -f "$log_file"
}
trap cleanup EXIT INT TERM

"$cloudflared_bin" tunnel --no-autoupdate --url "http://127.0.0.1:${SUPPLIER_PORT:-8090}" >"$log_file" 2>&1 &
tunnel_pid=$!
public_url=""
for _ in $(seq 1 60); do
  public_url="$(sed -nE 's#.*(https://[a-z0-9-]+\.trycloudflare\.com).*#\1#p' "$log_file" | head -1)"
  [[ -n "$public_url" ]] && break
  kill -0 "$tunnel_pid" 2>/dev/null || { cat "$log_file" >&2; exit 1; }
  sleep 1
done
[[ -n "$public_url" ]] || { cat "$log_file" >&2; echo "Tunnel URL was not assigned" >&2; exit 1; }

export SUPPLIER_PUBLIC_URL="$public_url"
export SUPPLIER_CORS_ORIGINS="${SUPPLIER_CORS_ORIGINS:-https://kpm-intake-web.onrender.com,http://localhost:3000,http://127.0.0.1:3000}"
echo "Supplier URL: $public_url"
echo "Paste this exact HTTPS URL into Sell Data: https://kpm-intake-web.onrender.com/sell"
./scripts/start_supplier.sh &
supplier_pid=$!
wait "$supplier_pid"
