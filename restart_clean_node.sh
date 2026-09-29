#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <node-id>"
    exit 1
fi

ID="$1"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== stop node ==="
"$SCRIPT_DIR/stop_node.sh"

echo
echo "=== clean metadata ==="
"$SCRIPT_DIR/clean_node.sh" "$ID"

echo
echo "=== start node ==="
"$SCRIPT_DIR/start_node.sh" "$ID"