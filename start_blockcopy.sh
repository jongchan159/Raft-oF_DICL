#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAFT_ROOT="$SCRIPT_DIR"
source "$SCRIPT_DIR/env.sh"
build_bc_devices

TRANSPORT="${1:-tcp}"

case "$TRANSPORT" in
    rdma|tcp|tcp-ib) ;;
    *)
        echo "usage: $0 [rdma|tcp|tcp-ib]"
        exit 1
        ;;
esac

LOG="$RAFT_ROOT/raft_blockcopy_server_${TRANSPORT}.log"

cd "$RAFT_ROOT"

echo "========================================"
echo "Starting BlockCopy server"
echo "========================================"
echo "transport   : $TRANSPORT"
echo "listen      : 0.0.0.0:5050"
echo "nodes       : $NODES"
echo "devices     : $BC_DEVICES   (NODES 순서 = cluster_index 순서)"
echo "workers     : 8"
echo "log         : $LOG"
echo "========================================"

IFS=',' read -r -a BC_DEV_ARR <<< "$BC_DEVICES"
for dev in "${BC_DEV_ARR[@]}"; do
    if [[ ! -b "$dev" ]]; then
        echo "ERROR: block device not found: $dev"
        exit 1
    fi
done

if pgrep -f '[r]aft_blockcopy_server' >/dev/null; then
    echo "ERROR: raft_blockcopy_server already running:"
    pgrep -af '[r]aft_blockcopy_server'
    exit 1
fi

sudo stdbuf -oL -eL ./build/raft_blockcopy_server \
    -addr 0.0.0.0:5050 \
    -devices "$BC_DEVICES" \
    -copy-workers 8 \
    -transport "$TRANSPORT" \
    > "$LOG" 2>&1 &

PID=$!

sleep 1

if ! ps -p "$PID" >/dev/null 2>&1; then
    echo "ERROR: blockcopy server failed to start"
    tail -100 "$LOG"
    exit 1
fi

echo
echo "blockcopy server started"
echo "pid: $PID"

echo
pgrep -af '[r]aft_blockcopy_server' || true

echo
tail -30 "$LOG"