#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

if [[ $# -lt 1 || $# -gt 4 ]]; then
    echo "usage: $0 <node-id> [destination|leader] [rdma|tcp-ib|tcp]"
    echo
    echo "examples:"
    echo "  $0 5"
    echo "  $0 5 destination rdma"
    echo "  $0 5 destination tcp"
    echo "  $0 5 leader rdma"
    echo "  $0 5 leader tcp"
    exit 1
fi

ID="$1"
VER="${2:-}"
MODE="${3:-destination}"
TRANSPORT_MODE="${4:-tcp}"

case "$ID" in
    4|5|6|7) ;;
    *)
        echo "invalid node id: $ID"
        echo "valid ids: 4, 5, 6, 7"
        exit 1
        ;;
esac

case "$VER" in
    old|pw) ;;
    *)
        echo "invalid ver: $VER"
        exit 1
        ;;
esac

case "$MODE" in
    destination|leader) ;;
    *)
        echo "invalid mode: $MODE"
        echo "valid modes: destination, leader"
        exit 1
        ;;
esac


case "$TRANSPORT_MODE" in
    rdma)
        TRANSPORT="rdma"
        select_transport_env rdma
        ;;
    tcp-ib)
        TRANSPORT="tcp"
        select_transport_env tcp-ib
        ;;
    tcp)
        TRANSPORT="tcp"
        select_transport_env tcp
        ;;
esac

select_transport_env "$TRANSPORT"

META_DIR="$(metadata_dir_for_id "$ID")"
DEVICE="$(device_for_id "$ID")"

LOG="$RAFT_ROOT/raft_node_${ID}_${MODE}_${TRANSPORT_MODE}.log"

cd "$RAFT_ROOT"

echo "========================================"
echo "Starting Raft node"
echo "========================================"
echo "id          : $ID"
echo "mode        : $MODE"
echo "transport   : $TRANSPORT"
echo "metadata    : $META_DIR"
echo "device      : $DEVICE"
echo "cluster     : $CLUSTER"
echo "ring-pages  : $RING_PAGES"
echo "heartbeat   : $HEARTBEAT_MS ms"
echo "ae-batch    : $AE_BATCH"
echo "log         : $LOG"
echo "========================================"

if [[ ! -b "$DEVICE" ]]; then
    echo "ERROR: block device does not exist: $DEVICE"
    exit 1
fi

if ! mountpoint -q /mnt/raftvol; then
    echo "ERROR: /mnt/raftvol is not mounted"
    exit 1
fi

sudo mkdir -p "$META_DIR"

if pgrep -f '[r]aft_node' >/dev/null; then
    echo "ERROR: raft_node already running:"
    pgrep -af '[r]aft_node'
    exit 1
fi

sudo stdbuf -oL -eL ./build/raft_node_"$VER"_ae \
    -id "$ID" \
    -cluster "$CLUSTER" \
    -metadata-dir "$META_DIR" \
    -transport "$TRANSPORT" \
    -mode "$MODE" \
    -ring-pages "$RING_PAGES" \
    -heartbeat-ms "$HEARTBEAT_MS" \
    -ae-batch "$AE_BATCH" \
    -profile \
    > "$LOG" 2>&1 &

PID=$!

sleep 1

if ! ps -p "$PID" >/dev/null 2>&1; then
    echo "ERROR: raft_node exited during startup"
    tail -100 "$LOG"
    exit 1
fi

echo
echo "raft_node started"
echo "pid: $PID"

echo
pgrep -af '[r]aft_node' || true

echo
tail -30 "$LOG"