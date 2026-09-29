#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <node-id>"
    exit 1
fi

ID="$1"

case "$ID" in
    4|5|6|7) ;;
    *)
        echo "invalid node id: $ID"
        exit 1
        ;;
esac

META_DIR="$(metadata_dir_for_id "$ID")"

echo "========================================"
echo "Clean Raft node"
echo "========================================"
echo "id       : $ID"
echo "metadata : $META_DIR"
echo "========================================"

if pgrep -f '[r]aft_node' >/dev/null; then
    echo "ERROR: raft_node is still running."
    echo "Run ./stop_node.sh first."
    exit 1
fi

if ! mountpoint -q /mnt/raftvol; then
    echo "ERROR: /mnt/raftvol is not mounted."
    exit 1
fi

# 안전장치
case "$META_DIR" in
    /mnt/raftvol/node4|/mnt/raftvol/node5|/mnt/raftvol/node6|/mnt/raftvol/node7)
        ;;
    *)
        echo "ERROR: refusing to clean unexpected path: $META_DIR"
        exit 1
        ;;
esac

echo "Removing contents of:"
echo "  $META_DIR"

sudo mkdir -p "$META_DIR"

sudo find "$META_DIR" \
    -mindepth 1 \
    -maxdepth 1 \
    -exec rm -rf -- {} +

echo "clean complete."

echo
echo "directory:"
sudo ls -la "$META_DIR"