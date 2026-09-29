#!/usr/bin/env bash
set -euo pipefail

source ./env.sh
cd "$RAFT_ROOT"

echo "========== apply 200 =========="

./build/raft_client \
    -addrs "$ADDRS" \
    -op apply \
    -n 200 \
    -size 512 \
    -batch 10 \
    -transport "$TRANSPORT"

echo
echo "========== commit index =========="

./build/raft_client \
    -addrs "$ADDRS" \
    -op commit-index \
    -transport "$TRANSPORT"

echo
echo "========== AE stats =========="

./build/raft_client \
    -addrs "$ADDRS" \
    -op ae-stats \
    -transport "$TRANSPORT"