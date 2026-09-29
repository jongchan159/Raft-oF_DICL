#!/usr/bin/env bash
set -euo pipefail

source ./env.sh
cd "$RAFT_ROOT"

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