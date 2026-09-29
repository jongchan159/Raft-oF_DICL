#!/usr/bin/env bash
set -euo pipefail

source ./env.sh
cd "$RAFT_ROOT"

PAYLOAD="${1:-4064}"
WARMUP="${2:-5000}"
SAMPLES="${3:-10000}"

echo "========================================"
echo "Raft-oF single-client latency experiment"
echo "========================================"
echo "payload : $PAYLOAD"
echo "warmup  : $WARMUP"
echo "samples : $SAMPLES"
echo

echo "[1/4] pre-check"
./build/raft_client \
    -addrs "$ADDRS" \
    -op commit-index \
    -transport "$TRANSPORT"

echo
echo "[2/4] warm-up"

./build/raft_client \
    -addrs "$ADDRS" \
    -op apply \
    -n "$WARMUP" \
    -size "$PAYLOAD" \
    -batch 10 \
    -transport "$TRANSPORT"

echo
echo "[3/4] measurement"

mkdir -p results
TS="$(date +%Y%m%d_%H%M%S)"
OUT="results/latency_${PAYLOAD}B_${TS}.log"

./build/raft_client \
    -addrs "$ADDRS" \
    -op apply-timed \
    -n "$SAMPLES" \
    -size "$PAYLOAD" \
    -batch 1 \
    -transport "$TRANSPORT" \
    | tee "$OUT"

echo
echo "[4/4] post-check"

{
    echo
    echo "===== commit-index ====="
    ./build/raft_client \
        -addrs "$ADDRS" \
        -op commit-index \
        -transport "$TRANSPORT"

    echo
    echo "===== ae-stats ====="
    ./build/raft_client \
        -addrs "$ADDRS" \
        -op ae-stats \
        -transport "$TRANSPORT"
} | tee -a "$OUT"

echo
echo "result: $OUT"