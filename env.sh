#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export RAFT_ROOT="$SCRIPT_DIR"

#
# IPoIB addresses
# - RDMA transport에서도 사용
# - TCP-over-IPoIB에서도 사용
#
export CLUSTER_IPOIB="4@10.0.0.4:6000@/dev/nvme1n1@10.0.0.91:5050,5@10.0.0.5:6000@/dev/nvme2n1@10.0.0.91:5050,6@10.0.0.6:6000@/dev/nvme3n1@10.0.0.91:5050"

export ADDRS_IPOIB="10.0.0.4:6000,10.0.0.5:6000,10.0.0.6:6000"

#
# Ethernet addresses
# - Normal TCP/Ethernet에서 사용
#
export CLUSTER_ETH="4@115.145.173.123:6000@/dev/nvme1n1@115.145.173.243:5050,5@115.145.173.124:6000@/dev/nvme2n1@115.145.173.243:5050,6@115.145.173.125:6000@/dev/nvme3n1@115.145.173.243:5050"

export ADDRS_ETH="115.145.173.123:6000,115.145.173.124:6000,115.145.173.125:6000"
#
# Common benchmark settings
#
export RING_PAGES=4096
# export RING_PAGES=1048576
export HEARTBEAT_MS=300
export AE_BATCH=64
export COPY_WORKERS=8

metadata_dir_for_id() {
    case "$1" in
        4) echo "/mnt/raftvol/node4" ;;
        5) echo "/mnt/raftvol/node5" ;;
        6) echo "/mnt/raftvol/node6" ;;
        7) echo "/mnt/raftvol/node7" ;;
        *)
            echo "unknown node id: $1" >&2
            return 1
            ;;
    esac
}

device_for_id() {
    case "$1" in
        4) echo "/dev/nvme1n1" ;;
        5) echo "/dev/nvme2n1" ;;
        6) echo "/dev/nvme3n1" ;;
        7) echo "/dev/nvme1n1" ;;
        *)
            echo "unknown node id: $1" >&2
            return 1
            ;;
    esac
}

#
# transport mode 선택
#
# rdma
#   → -transport rdma
#   → 10.0.0.x IPoIB addresses
#
# tcp-ipoib
#   → -transport tcp
#   → 10.0.0.x IPoIB addresses
#
# tcp
#   → -transport tcp
#   → 115.145.173.x Ethernet addresses
#
select_transport_env() {
    local mode="$1"

    case "$mode" in
        rdma)
            export TRANSPORT="rdma"
            export CLUSTER="$CLUSTER_IPOIB"
            export ADDRS="$ADDRS_IPOIB"
            ;;

        tcp-ipoib)
            export TRANSPORT="tcp"
            export CLUSTER="$CLUSTER_IPOIB"
            export ADDRS="$ADDRS_IPOIB"
            ;;

        tcp)
            export TRANSPORT="tcp"
            export CLUSTER="$CLUSTER_ETH"
            export ADDRS="$ADDRS_ETH"
            ;;

        *)
            echo "invalid transport mode: $mode" >&2
            echo "valid modes: rdma, tcp-ipoib, tcp" >&2
            return 1
            ;;
    esac
}