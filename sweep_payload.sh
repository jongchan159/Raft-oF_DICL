#!/usr/bin/env bash
set -euo pipefail

#
# usage:
#   ./sweep_payload.sh <destination|leader> <rdma|tcp-ipoib|tcp>
#
# examples:
#   ./sweep_payload.sh destination rdma
#   ./sweep_payload.sh destination tcp-ib
#   ./sweep_payload.sh leader rdma
#

if [[ $# -ne 2 ]]; then
    echo "usage: $0 <destination|leader> <rdma|tcp-ib|tcp>"
    exit 1
fi

MODE="$1"
TRANSPORT_MODE="$2"

case "$MODE" in
    destination|leader) ;;
    *)
        echo "invalid mode: $MODE"
        exit 1
        ;;
esac

case "$TRANSPORT_MODE" in
    rdma|tcp-ib|tcp) ;;
    *)
        echo "invalid transport mode: $TRANSPORT_MODE"
        exit 1
        ;;
esac

#
# 환경
#
RAFT_ROOT="/home/ryudb00/raftof_jongc"

NODE5="eternity5"
NODE6="eternity6"
NODE7="eternity7"
STORAGE="eternitystorage"

#
# payload sweep
#
PAYLOADS=(
    512
    992
    2016
    4064
    8160
    16352
    32736
    65504
    131040
)

WARMUP_COUNT=5000
WARMUP_BATCH=10

BENCH_COUNT=10000
BENCH_BATCH=1

echo "================================================"
echo "Raft-oF payload sweep"
echo "================================================"
echo "mode          : $MODE"
echo "transport     : $TRANSPORT_MODE"
echo "warmup count  : $WARMUP_COUNT"
echo "warmup batch  : $WARMUP_BATCH"
echo "bench count   : $BENCH_COUNT"
echo "bench batch   : $BENCH_BATCH"
echo "payloads      : ${PAYLOADS[*]}"
echo "================================================"

#
# remote helper
#
remote() {
    local host="$1"
    shift
    ssh "$host" "cd '$RAFT_ROOT' && $*"
}

#
# cluster stop
#
stop_cluster() {
    echo
    echo "===== STOP CLUSTER ====="

    remote "$NODE5" "./stop_node.sh" || true
    remote "$NODE6" "./stop_node.sh" || true
    remote "$NODE7" "./stop_node.sh" || true

    remote "$STORAGE" "./stop_blockcopy.sh" || true
}

#
# metadata clean
#
clean_cluster() {
    echo
    echo "===== CLEAN METADATA ====="

    remote "$NODE5" "./clean_node.sh 5"
    remote "$NODE6" "./clean_node.sh 6"
    remote "$NODE7" "./clean_node.sh 7"
}

#
# start cluster
#
start_cluster() {
    echo
    echo "===== START BLOCKCOPY ====="

    remote "$STORAGE" "./start_blockcopy.sh '$TRANSPORT_MODE'"

    sleep 1

    echo
    echo "===== START RAFT NODES ====="

    remote "$NODE5" "./start_node.sh 5 '$MODE' '$TRANSPORT_MODE'"
    remote "$NODE6" "./start_node.sh 6 '$MODE' '$TRANSPORT_MODE'"
    remote "$NODE7" "./start_node.sh 7 '$MODE' '$TRANSPORT_MODE'"

    #
    # election 안정화
    #
    sleep 2
}

#
# warm-up
#
warmup() {
    local payload="$1"

    echo
    echo "===== WARM-UP: payload=${payload} ====="

    cd "$RAFT_ROOT"

    source ./env.sh
    select_transport_env "$TRANSPORT_MODE"

    ./build/raft_client \
        -addrs "$ADDRS" \
        -op apply \
        -n "$WARMUP_COUNT" \
        -size "$payload" \
        -batch "$WARMUP_BATCH" \
        -transport "$TRANSPORT"
}

#
# benchmark
#
benchmark() {
    local payload="$1"

    echo
    echo "===== BENCH: payload=${payload} ====="

    cd "$RAFT_ROOT"

    ./bench_latency.sh \
        "$MODE" \
        "$TRANSPORT_MODE" \
        "$payload" \
        "$BENCH_COUNT" \
        "$BENCH_BATCH"
}

#
# payload별:
#
# clean start
# → warm-up
# → benchmark
# → stop
#
for payload in "${PAYLOADS[@]}"; do

    echo
    echo
    echo "################################################"
    echo "# PAYLOAD = ${payload} bytes"
    echo "################################################"

    stop_cluster

    clean_cluster

    start_cluster

    warmup "$payload"

    benchmark "$payload"

    stop_cluster

    echo
    echo "===== payload ${payload} COMPLETE ====="
done

echo
echo "================================================"
echo "ALL PAYLOAD TESTS COMPLETE"
echo "================================================"