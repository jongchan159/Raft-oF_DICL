#!/usr/bin/env bash
set -euo pipefail

source ./env.sh
cd "$RAFT_ROOT"

PAYLOAD="${1:-4064}"
COUNT="${2:-5000}"

echo "[warmup] payload=$PAYLOAD count=$COUNT"

./build/raft_client \
    -addrs "$ADDRS" \
    -op apply \
    -n "$COUNT" \
    -size "$PAYLOAD" \
    -batch 10 \
    -transport "$TRANSPORT"

echo
echo "[warmup] done -- discard this result"