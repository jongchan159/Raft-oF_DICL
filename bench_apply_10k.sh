#!/usr/bin/env bash
set -euo pipefail

PAYLOAD="${1:-4096}"
VER="${2:-}"
TRANSPORT_MODE="${3:-tcp}"
WARMUP="${4:-1000}"
COUNT="${5:-10000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
cd "$RAFT_ROOT"

case "$TRANSPORT_MODE" in
    tcp)
        select_transport_env tcp
        CLIENT_TRANSPORT="tcp"
        ;;
    tcp-ib)
        select_transport_env tcp-ib
        CLIENT_TRANSPORT="tcp"
        ;;
    rdma)
        select_transport_env rdma
        CLIENT_TRANSPORT="rdma"
        ;;
    *)
        echo "usage: $0 [payload_bytes] [tcp|tcp-ib|rdma] [warmup] [count]" >&2
        exit 1
        ;;
esac

RESULT_DIR="$RAFT_ROOT/results/${VER}_ae_breakdown_0929"
mkdir -p "$RESULT_DIR"
TS="$(date +%Y%m%d_%H%M%S)"
OUT="$RESULT_DIR/${PAYLOAD}B_${TS}_${TRANSPORT_MODE}.log"

echo "========================================" | tee "$OUT"
echo "Raft-oF plain apply internal latency"     | tee -a "$OUT"
echo "========================================" | tee -a "$OUT"
echo "transport       : $TRANSPORT_MODE"       | tee -a "$OUT"
echo "payload_bytes   : $PAYLOAD"              | tee -a "$OUT"
echo "warmup_count    : $WARMUP"               | tee -a "$OUT"
echo "request_count   : $COUNT"                | tee -a "$OUT"
echo "batch           : 1"                     | tee -a "$OUT"
echo "raft_addrs      : $ADDRS"                | tee -a "$OUT"
echo                                              | tee -a "$OUT"

sleep 1

echo "[pre-check]" | tee -a "$OUT"
./build/raft_client \
    -addrs "$ADDRS" \
    -op commit-index \
    -transport "$CLIENT_TRANSPORT" 2>&1 | tee -a "$OUT"

echo | tee -a "$OUT"
echo "[warmup]" | tee -a "$OUT"
./build/raft_client \
    -addrs "$ADDRS" \
    -op apply \
    -n "$WARMUP" \
    -size "$PAYLOAD" \
    -batch 1 \
    -transport "$CLIENT_TRANSPORT" \
    >/dev/null

echo "[measurement]" | tee -a "$OUT"
./build/raft_client \
    -addrs "$ADDRS" \
    -op apply-timed \
    -n "$COUNT" \
    -size "$PAYLOAD" \
    -batch 1 \
    -transport "$CLIENT_TRANSPORT" \
    2>&1 | tee -a "$OUT"

echo | tee -a "$OUT"
echo "[post-check]" | tee -a "$OUT"
./build/raft_client \
    -addrs "$ADDRS" \
    -op commit-index \
    -transport "$CLIENT_TRANSPORT" 2>&1 | tee -a "$OUT"

echo
echo "saved: $OUT"
