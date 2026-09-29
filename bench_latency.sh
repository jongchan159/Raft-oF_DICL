#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

cd "$RAFT_ROOT"

if [[ $# -lt 2 || $# -gt 5 ]]; then
    echo "usage: $0 <mode> <transport> [payload] [count] [batch]"
    echo
    echo "examples:"
    echo "  $0 destination rdma 4064 10000 1"
    echo "  $0 destination tcp  4064 10000 1"
    echo "  $0 leader      rdma 4064 10000 1"
    echo "  $0 leader      tcp  4064 10000 1"
    exit 1
fi

MODE="$1"
TRANSPORT="$2"
PAYLOAD="${3:-4064}"
COUNT="${4:-10000}"
BATCH="${5:-1}"

case "$MODE" in
    destination|leader) ;;
    *)
        echo "invalid mode: $MODE"
        exit 1
        ;;
esac

case "$TRANSPORT" in
    rdma|tcp|tcp-ib) ;;
    *)
        echo "invalid transport: $TRANSPORT"
        exit 1
        ;;
esac

select_transport_env "$TRANSPORT"

RESULT_DIR="$RAFT_ROOT/results"
mkdir -p "$RESULT_DIR"

TS="$(date +%Y%m%d_%H%M%S)"
HOST="$(hostname)"

OUT="$RESULT_DIR/apply_timed_${MODE}_${TRANSPORT}_${PAYLOAD}B_n${COUNT}_b${BATCH}_${HOST}_${TS}.log"

echo "========================================" | tee "$OUT"
echo "Raft-oF apply-timed benchmark"            | tee -a "$OUT"
echo "========================================" | tee -a "$OUT"

TMP_OUT="$(mktemp)"
trap 'rm -f "$TMP_OUT"' EXIT

./build/raft_client \
    -addrs "$ADDRS" \
    -op apply \
    -n "$COUNT" \
    -size "$PAYLOAD" \
    -batch "$BATCH" \
    -transport "$TRANSPORT" \
    > "$TMP_OUT" 2>&1

#
# 전체 raw output은 결과 파일에 보존
#
cat "$TMP_OUT" >> "$OUT"

echo
echo "===== RESULT ====="

#
# count가 작은 경우 전체 표시
# 큰 경우 summary만 표시
#
if (( COUNT <= 1000 )); then
    cat "$TMP_OUT"
else
    sed -n '/apply-timed summary/,$p' "$TMP_OUT"
fi

echo                                   | tee -a "$OUT"
echo "===== POST-RUN COMMIT INDEX ====="  | tee -a "$OUT"

./build/raft_client \
    -addrs "$ADDRS" \
    -op commit-index \
    -transport "$TRANSPORT" \
    2>&1 | tee -a "$OUT"

echo                                   | tee -a "$OUT"
echo "===== POST-RUN AE STATS ====="      | tee -a "$OUT"

./build/raft_client \
    -addrs "$ADDRS" \
    -op ae-stats \
    -transport "$TRANSPORT" \
    2>&1 | tee -a "$OUT"

echo                                   | tee -a "$OUT"
echo "========================================" | tee -a "$OUT"
echo "BENCH INFO"                            | tee -a "$OUT"
echo "========================================" | tee -a "$OUT"

echo "timestamp       : $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$OUT"
echo "client_host     : $HOST"                         | tee -a "$OUT"

echo "mode            : $MODE"                         | tee -a "$OUT"
echo "transport       : $TRANSPORT"                    | tee -a "$OUT"

if [[ "$TRANSPORT" == "rdma" ]]; then
    echo "network_path    : InfiniBand / 10.0.0.x"       | tee -a "$OUT"
else
    echo "network_path    : Ethernet / 115.145.173.x"    | tee -a "$OUT"
fi

echo                                                    | tee -a "$OUT"

echo "operation       : apply-timed"                   | tee -a "$OUT"
echo "payload_bytes   : $PAYLOAD"                      | tee -a "$OUT"
echo "count           : $COUNT"                        | tee -a "$OUT"
echo "batch           : $BATCH"                        | tee -a "$OUT"

echo                                                    | tee -a "$OUT"

echo "ring_pages      : $RING_PAGES"                   | tee -a "$OUT"
echo "heartbeat_ms    : $HEARTBEAT_MS"                 | tee -a "$OUT"
echo "ae_batch        : $AE_BATCH"                     | tee -a "$OUT"
echo "copy_workers    : $COPY_WORKERS"                 | tee -a "$OUT"

echo                                                    | tee -a "$OUT"

echo "raft_addrs      : $ADDRS"                        | tee -a "$OUT"
echo "cluster         : $CLUSTER"                      | tee -a "$OUT"

echo                                                    | tee -a "$OUT"

echo "profiling       : enabled"                       | tee -a "$OUT"

echo "========================================" | tee -a "$OUT"

echo
echo "saved:"
echo "  $OUT"