#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <node-id>"
    exit 1
fi

ID="$1"

is_valid_id "$ID" || {
    echo "invalid node id: $ID"
    echo "valid ids: $(valid_ids_str)"
    exit 1
}

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
if [[ "$META_DIR" != "/mnt/raftvol/node$ID" ]]; then
    echo "ERROR: refusing to clean unexpected path: $META_DIR"
    exit 1
fi

echo "Removing contents of:"
echo "  $META_DIR"

sudo mkdir -p "$META_DIR"

# 링 파일(raft-<ID>.ring)은 지우지 않는다: 다시 만들면 precondition 으로 파일 전체를
# 다시 써야 해서 시작이 오래 걸린다. 헤더는 노드가 시작할 때 0 으로 되돌린다.
# 링 파일까지 지우려면 RECREATE_RING=1 (예: ring 배치를 새로 받고 싶을 때, 정확성 검증).
KEEP_ARGS=()
if [[ "${RECREATE_RING:-0}" != "1" ]]; then
    KEEP_ARGS=( ! -name "raft-${ID}.ring" )
    echo "keeping ring file: raft-${ID}.ring  (RECREATE_RING=1 to delete)"
else
    echo "RECREATE_RING=1: deleting ring file too"
fi

sudo find "$META_DIR" \
    -mindepth 1 \
    -maxdepth 1 \
    ${KEEP_ARGS[@]+"${KEEP_ARGS[@]}"} \
    -exec rm -rf -- {} +

echo "clean complete."

echo
echo "directory:"
sudo ls -la "$META_DIR"