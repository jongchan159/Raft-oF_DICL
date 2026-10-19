#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export RAFT_ROOT="$SCRIPT_DIR"

#
# 노드 풀 (고정) + 실행 시 선택 (NODES / TOPO)
#
# 풀 형식: id|ipoib_addr|eth_addr|compute_dev|storage_dev
#   ipoib_addr / eth_addr : raft RPC 주소 (host:port)
#   compute_dev           : 컴퓨트 노드에서 본 볼륨 디바이스 (-cluster 의 device_path)
#   storage_dev           : 스토리지 서버 로컬 디바이스 (raft_blockcopy_server -devices)
# TODO 값이 있는 노드를 NODES 에 넣으면 build_cluster 가 에러로 막는다.
#
# !! 모든 raft 노드, blockcopy 서버, 클라이언트가 **같은 NODES** 를 써야 한다.
#    멤버 수가 quorum 을 정하고, NODES 순서가 cluster_index 와 blockcopy 의
#    -devices 인덱스를 정한다 (순서가 다르면 엉뚱한 볼륨에 쓴다).
#
POOL=(
    "1|10.0.0.1:6000|115.145.173.242:6000|/dev/nvme1n1|/dev/nvme7n1"
    "2|10.0.0.2:6000|115.145.173.244:6000|/dev/nvme1n1|/dev/nvme8n1"      # IPoIB 케이블 비정상 -> tcp(Ethernet)만
    "3|10.0.0.3:6000|115.145.173.245:6000|/dev/nvme1n1|/dev/nvme0n1"      # IPoIB 케이블 비정상 -> tcp(Ethernet)만
    "4|10.0.0.4:6000|115.145.173.123:6000|/dev/nvme1n1|/dev/nvme1n1"
    "5|10.0.0.5:6000|115.145.173.124:6000|/dev/nvme1n1|/dev/nvme3n1"
    "6|10.0.0.6:6000|115.145.173.125:6000|/dev/nvme1n1|/dev/nvme5n1"
)
# 출처: 사용자 제공 인벤토리 (eternityN = raft id N, raft 포트 6000).
#   storage_dev = 스토리지 서버(115.145.173.243)에서 본 "Remote Storage" 디바이스.
#   compute_dev = 각 컴퓨트 노드에서 자기 볼륨이 보이는 디바이스 ("Local" = 컴퓨트 관점).
#     -cluster 의 device_path 로 들어가며 노드는 자기 항목만 쓴다(raft_node_main.cpp).
#     5,6 은 예전 env.sh 에 nvme2n1/nvme3n1 이었으나 현재 인벤토리 기준 nvme1n1 로 변경.
# 2,3 번 중 하나만 쓴다: TOPO=5 의 기본은 "1 2 4 5 6", 3번을 쓰려면
#   TOPO_5_NODES="1 3 4 5 6" 으로 덮어쓴다.
# DOWN 은 해당 전송 모드(ipoib: rdma/tcp-ipoib)를 쓸 수 없다는 표시다.

# 실행 시 선택. 우선순위: NODES > TOPO > 기본(3노드)
#   NODES="1 2 4 5 6" ./start_node.sh ...      직접 지정 (순서 = cluster 순서)
#   TOPO=5 ./start_node.sh ...                 프리셋 (1 2 4 5 6)
TOPO_3_NODES="${TOPO_3_NODES:-4 5 6}"
TOPO_5_NODES="${TOPO_5_NODES:-1 2 4 5 6}"
if [[ -z "${NODES:-}" ]]; then
    case "${TOPO:-3}" in
        3) NODES="$TOPO_3_NODES" ;;
        5) NODES="$TOPO_5_NODES" ;;
        *) echo "invalid TOPO: ${TOPO} (3|5)" >&2; return 1 2>/dev/null || exit 1 ;;
    esac
fi
export NODES

# 호스트명 규칙: eternity<ID> (sweep_payload.sh 의 기존 NODE5=eternity5 와 동일).
host_for_id() { echo "eternity$1"; }

# _pool_field <id> <col>   col: 1=ipoib 2=eth 3=compute_dev 4=storage_dev
_pool_field() {
    local e f
    for e in "${POOL[@]}"; do
        e="${e%%#*}"                       # 줄 끝 주석 제거
        IFS='|' read -r -a f <<< "$e"
        if [[ "${f[0]}" == "$1" ]]; then echo "${f[$2]}"; return 0; fi
    done
    return 1
}

is_valid_id() {                            # 풀이 아니라 **이번 NODES** 기준
    local x
    for x in $NODES; do [[ "$x" == "$1" ]] && return 0; done
    return 1
}

valid_ids_str() { echo "${NODES// /, }"; }

# build_cluster <ipoib|eth>  ->  CLUSTER ADDRS (NODES 순서)
build_cluster() {
    local kind="$1" col id addr dev sh
    case "$kind" in
        ipoib) col=1; sh="10.0.0.91:5050" ;;
        eth)   col=2; sh="115.145.173.243:5050" ;;
        *) echo "build_cluster: kind must be ipoib|eth" >&2; return 1 ;;
    esac
    CLUSTER=""; ADDRS=""
    for id in $NODES; do
        addr="$(_pool_field "$id" "$col")"  || { echo "node $id: 풀에 없음" >&2; return 1; }
        dev="$(_pool_field "$id" 3)"
        if [[ "$addr" == DOWN* ]]; then
            echo "node $id: 이 전송 모드($kind)의 링크가 비정상(DOWN). tcp(Ethernet)로만 쓸 수 있다 (env.sh POOL)" >&2
            return 1
        fi
        if [[ "$addr" == TODO* || "$dev" == TODO* ]]; then
            echo "node $id: 풀의 TODO 값(addr=$addr dev=$dev)을 채우기 전에는 NODES 에 넣을 수 없다 (env.sh POOL)" >&2
            return 1
        fi
        CLUSTER+="${CLUSTER:+,}${id}@${addr}@${dev}@${sh}"
        ADDRS+="${ADDRS:+,}${addr}"
    done
    export CLUSTER ADDRS
}

# build_bc_devices -> BC_DEVICES (NODES 순서, 스토리지 서버의 raft_blockcopy_server -devices 용)
build_bc_devices() {
    local id sdev
    BC_DEVICES=""
    for id in $NODES; do
        sdev="$(_pool_field "$id" 4)" || { echo "node $id: 풀에 없음" >&2; return 1; }
        if [[ -z "$sdev" || "$sdev" == TODO* ]]; then
            echo "node $id: storage_dev 가 풀에 없음 (env.sh POOL, ./nvmeof_ctl.sh map 으로 확인)" >&2
            return 1
        fi
        BC_DEVICES+="${BC_DEVICES:+,}${sdev}"
    done
    export BC_DEVICES
}

#
# Common benchmark settings 
#
# export RING_PAGES=4096
export RING_PAGES=524288
export RING_PRECONDITION="${RING_PRECONDITION:-1}"
export HEARTBEAT_MS=300
export AE_BATCH=64
export COPY_WORKERS=8
export MAX_GC_REQ=32
export MAX_GC_BYTES=4194304

metadata_dir_for_id() {
    if ! is_valid_id "$1"; then
        echo "unknown node id: $1 (NODES=$NODES)" >&2
        return 1
    fi
    echo "/mnt/raftvol/node$1"
}

device_for_id() {
    local d
    is_valid_id "$1" || { echo "unknown node id: $1 (NODES=$NODES)" >&2; return 1; }
    d="$(_pool_field "$1" 3)"
    if [[ -z "$d" || "$d" == TODO* ]]; then
        echo "node $1: compute_dev 가 풀에 없음 (env.sh POOL)" >&2
        return 1
    fi
    echo "$d"
}

#
# 하위 호환: 예전 env.sh 가 source 시점에 export 하던 상수들
#
# IPoIB addresses
# - RDMA transport에서도 사용
# - TCP-over-IPoIB에서도 사용
#   -> CLUSTER_IPOIB / ADDRS_IPOIB
# Ethernet addresses
# - Normal TCP/Ethernet에서 사용
#   -> CLUSTER_ETH / ADDRS_ETH
#
# 이제 정적 문자열이 아니라 현재 NODES 로 풀에서 생성한다.
# DOWN/TODO 때문에 만들 수 없는 쪽(예: NODES 에 2번이 있는 IPoIB)은 정의하지 않는다.
# CLUSTER / ADDRS 는 원본처럼 select_transport_env 를 부를 때만 설정된다.
#
_export_compat_constants() {
    local had_c=0 had_a=0 sc="" sa=""
    if [[ -n "${CLUSTER+x}" ]]; then had_c=1; sc="$CLUSTER"; fi
    if [[ -n "${ADDRS+x}" ]];   then had_a=1; sa="$ADDRS";   fi

    if build_cluster ipoib 2>/dev/null; then
        export CLUSTER_IPOIB="$CLUSTER" ADDRS_IPOIB="$ADDRS"
    fi
    if build_cluster eth 2>/dev/null; then
        export CLUSTER_ETH="$CLUSTER" ADDRS_ETH="$ADDRS"
    fi

    if [[ $had_c -eq 1 ]]; then export CLUSTER="$sc"; else unset CLUSTER; fi
    if [[ $had_a -eq 1 ]]; then export ADDRS="$sa";   else unset ADDRS;   fi
}
_export_compat_constants

#
# transport mode 선택
#
# rdma
#   → -transport rdma
#   → 10.0.0.x IPoIB addresses
#
# tcp-ib
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
            build_cluster ipoib || return 1
            ;;

        tcp-ib)
            export TRANSPORT="tcp"
            build_cluster ipoib || return 1
            ;;

        tcp)
            export TRANSPORT="tcp"
            build_cluster eth || return 1
            ;;

        *)
            echo "invalid transport mode: $mode" >&2
            echo "valid modes: rdma, tcp-ib, tcp" >&2
            return 1
            ;;
    esac
}