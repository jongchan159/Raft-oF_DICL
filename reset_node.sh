#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

ID="${1:-}"
VER="${2:-old}"
MODE="${3:-destination}"
TRANSPORT_MODE="${4:-rdma}"

if ! is_valid_id "$ID"; then
    echo "usage: $0 <$(valid_ids_str | tr -d ' ' | tr ',' '|')> [destination|leader] [tcp|tcp-ib|rdma]" >&2
    exit 1
fi

if [[ ! "$VER" =~ ^(old|pw)$ ]]; then
    echo "usage: $0 <old|pw>" >&2
    exit 1
fi


case "$MODE" in
    destination|leader) ;;
    *) echo "invalid mode: $MODE" >&2; exit 1 ;;
esac

case "$TRANSPORT_MODE" in
    tcp|tcp-ib|rdma) ;;
    *) echo "invalid transport: $TRANSPORT_MODE" >&2; exit 1 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=== stop node${ID} ==="
./stop_node.sh || true

echo
echo "=== clean node${ID} ==="
./clean_node.sh "$ID"

sleep 3.5

echo
echo "=== restart node${ID} ver=${VER} mode=${MODE} transport=${TRANSPORT_MODE} ==="
exec ./start_node.sh "$ID" "$VER" "$MODE" "$TRANSPORT_MODE"