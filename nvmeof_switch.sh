#!/usr/bin/env bash
#
# nvmeof_switch.sh -- NVMe-oF initiator 전송(tcp|rdma) 전환 / 상태 확인
#
# 이 호스트(initiator)의 볼륨 하나를 끊고 다른 전송으로 다시 붙인 뒤
# /mnt/raftvol 에 다시 마운트한다. 노드마다 따로 실행해야 한다.
#
#   sudo ./nvmeof_switch.sh status
#   sudo ./nvmeof_switch.sh tcp  [-n NQN] [-i QUEUES] [-d EXPECT_DEV] [--dry-run] [--force]
#   sudo ./nvmeof_switch.sh rdma [-n NQN] [-i QUEUES] [-d EXPECT_DEV] [--dry-run] [--force]
#
#   -n NQN         대상 subsystem NQN. 생략하면 자동 결정 (순서):
#                    1) /mnt/raftvol 에 마운트된 장치의 subsystem
#                    2) 연결된 fabrics 컨트롤러가 하나뿐일 때 그것
#                    3) 이전에 성공한 값 (STATE_FILE, 기본 /var/lib/nvmeof_switch.nqn)
#                    4) 호스트명 끝 숫자 N -> nodeN (target discovery 에 있을 때만, 휴리스틱)
#   -i QUEUES      nvme connect -i 값 (A/B 비교 시 고정할 것)
#   -d EXPECT_DEV  재연결 후 마운트된 장치가 이 값과 다르면 경고
#   --dry-run      실행할 명령만 출력
#   --force        이미 목표 전송이어도 재연결
#
# 환경변수: MNT(/mnt/raftvol) TCP_ADDR RDMA_ADDR PORT(4420) QUEUES STATE_FILE
#
# 사전 조건: 이 볼륨을 쓰는 프로세스(raft_node, braft, hashicorp 등)를 먼저 멈출 것.
#           /mnt/raftvol 이 busy 면 umount 전에 중단한다.

set -euo pipefail

MNT="${MNT:-/mnt/raftvol}"
TCP_ADDR="${TCP_ADDR:-115.145.173.243}"
RDMA_ADDR="${RDMA_ADDR:-10.0.0.91}"
PORT="${PORT:-4420}"
QUEUES="${QUEUES:-}"
STATE_FILE="${STATE_FILE:-/var/lib/nvmeof_switch.nqn}"
NQN_SRC=""

NQN=""
EXPECT_DEV=""
DRY=0
FORCE=0

die() { echo "ERROR: $*" >&2; exit 1; }

usage() { sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }

run() {
    echo "+ $*"
    if [[ $DRY -eq 0 ]]; then "$@"; fi
}

# fabrics(tcp/rdma) 컨트롤러 목록: ctrl transport address subsysnqn queues
fabric_ctrls() {
    local c t
    for c in /sys/class/nvme/nvme*; do
        [[ -r "$c/transport" ]] || continue
        t="$(<"$c/transport")"
        [[ "$t" == tcp || "$t" == rdma ]] || continue
        printf '%s %s %s %s %s\n' "$(basename "$c")" "$t" \
            "$(<"$c/address")" "$(<"$c/subsysnqn")" \
            "$(cat "$c/queue_count" 2>/dev/null || echo -)"
    done
}

# subsysnqn 에 속한 namespace 블록 장치 이름들
find_ns() {
    local nqn="$1" sub d
    for sub in /sys/class/nvme-subsystem/*; do
        [[ -r "$sub/subsysnqn" ]] || continue
        [[ "$(<"$sub/subsysnqn")" == "$nqn" ]] || continue
        for d in "$sub"/nvme*n*; do
            [[ -e "$d" ]] && basename "$d"
        done
    done
}

status() {
    printf '%-7s %-5s %-6s %-12s %s\n' CTRL TRANS QUEUES NQN ADDRESS
    fabric_ctrls | while read -r ctrl t addr nqn q; do
        printf '%-7s %-5s %-6s %-12s %s\n' "$ctrl" "$t" "$q" "$nqn" "$addr"
    done
    local src
    src="$(findmnt -no SOURCE "$MNT" 2>/dev/null || true)"
    echo "mount $MNT : ${src:-<not mounted>}"
}

# 장치(블록 디바이스 이름) -> 그 장치가 속한 subsysnqn
nqn_of_dev() {
    local dev="$1" sub d
    for sub in /sys/class/nvme-subsystem/*; do
        [[ -r "$sub/subsysnqn" ]] || continue
        for d in "$sub"/nvme*n*; do
            if [[ -e "$d" && "$(basename "$d")" == "$dev" ]]; then
                cat "$sub/subsysnqn"
                return 0
            fi
        done
    done
    return 1
}

# target 이 노출 중인 subsystem NQN 목록 (tcp 먼저, 안 되면 rdma)
discover_nqns() {
    {
        nvme discover -t tcp  -a "$TCP_ADDR"  -s "$PORT" 2>/dev/null ||
        nvme discover -t rdma -a "$RDMA_ADDR" -s "$PORT" 2>/dev/null || true
    } | awk '/^subnqn:/ {print $2}' | grep -v '^nqn\.2014-08\.org\.nvmexpress\.discovery$' || true
}

# NQN 결정 순서: -n > /mnt/raftvol 장치 > fabrics 컨트롤러가 하나 > 저장값 > 호스트명 규칙
pick_nqn() {
    local src base nqns count host num cand
    if [[ -n "$NQN" ]]; then NQN_SRC="-n 인자"; return 0; fi

    src="$(findmnt -no SOURCE "$MNT" 2>/dev/null || true)"
    if [[ -n "$src" ]]; then
        base="$(basename "$src")"; base="${base%p[0-9]*}"
        if cand="$(nqn_of_dev "$base")" && [[ -n "$cand" ]]; then
            NQN="$cand"; NQN_SRC="$MNT 에 마운트된 장치($base)"; return 0
        fi
    fi

    nqns="$(fabric_ctrls | awk '{print $4}' | sort -u)"
    count="$(printf '%s\n' "$nqns" | grep -c . || true)"
    if [[ "$count" -eq 1 ]]; then
        NQN="$nqns"; NQN_SRC="연결된 fabrics 컨트롤러가 하나"; return 0
    fi

    # 연결이 없거나 모호: 이전에 성공한 값
    if [[ -r "$STATE_FILE" ]] && cand="$(head -n1 "$STATE_FILE")" && [[ -n "$cand" ]]; then
        NQN="$cand"; NQN_SRC="저장값($STATE_FILE)"; return 0
    fi

    # 마지막 수단: 호스트명 끝 숫자 -> nodeN, 단 target discovery 에 있을 때만 채택 (휴리스틱)
    host="${NVMEOF_HOST:-$(hostname -s)}"
    if [[ "$host" =~ ([0-9]+)$ ]]; then
        num="${BASH_REMATCH[1]}"
        if discover_nqns | grep -qx "node${num}"; then
            NQN="node${num}"; NQN_SRC="호스트명 규칙($host -> node${num}, 휴리스틱)"; return 0
        fi
    fi

    echo "NQN 을 자동으로 정하지 못했다. target 이 노출 중인 NQN:" >&2
    discover_nqns | sed 's/^/  /' >&2
    die "-n NQN 으로 한 번 지정하라 (성공하면 다음부터 저장값을 쓴다)."
}

switch_to() {
    local target="$1" addr cur src base uuid i newsrc

    case "$target" in
        tcp)  addr="$TCP_ADDR" ;;
        rdma) addr="$RDMA_ADDR" ;;
    esac

    [[ $EUID -eq 0 ]] || die "root 로 실행하라 (sudo)."
    command -v nvme >/dev/null || die "nvme-cli 가 없다."

    pick_nqn
    echo "target=$target addr=$addr:$PORT nqn=$NQN queues=${QUEUES:-<default>}"
    echo "NQN 결정 근거: $NQN_SRC"

    cur="$(fabric_ctrls | awk -v n="$NQN" '$4==n {print $2}' | sort -u | tr '\n' ' ')"
    echo "current transport(s) for $NQN: ${cur:-<none>}"
    if [[ "$cur" == "$target " && $FORCE -eq 0 ]]; then
        echo "이미 $target 이다 (--force 로 재연결)."
        status
        return 0
    fi

    # 사전 점검(읽기 전용): 목표 전송으로 이 NQN 이 실제로 보이는지 먼저 확인한다.
    # 안 보이면 끊은 뒤 못 붙는 사고가 나므로 umount/disconnect 전에 중단한다.
    local seen
    seen="$(nvme discover -t "$target" -a "$addr" -s "$PORT" 2>/dev/null |
            awk '/^subnqn:/ {print $2}' || true)"
    if ! printf '%s\n' "$seen" | grep -qx "$NQN"; then
        echo "target $addr:$PORT ($target) 에서 보이는 NQN:" >&2
        printf '%s\n' "$seen" | grep -v '^$' | sed 's/^/  /' >&2
        die "$NQN 이 $target 으로 노출되지 않는다. 아무것도 끊지 않았다."
    fi
    echo "preflight OK: $NQN 이 $target($addr:$PORT) 에서 보인다."

    src="$(findmnt -no SOURCE "$MNT" 2>/dev/null || true)"
    uuid=""
    if [[ -n "$src" ]]; then
        # 마운트된 장치가 이 NQN 의 것인지 확인한다 (엉뚱한 볼륨을 끊지 않기 위해)
        base="$(basename "$src")"
        base="${base%p[0-9]*}"
        if ! find_ns "$NQN" | grep -qx "$base"; then
            die "$MNT 의 장치($src)가 NQN=$NQN 의 namespace 가 아니다. -n 을 확인하라."
        fi
        uuid="$(blkid -s UUID -o value "$src" 2>/dev/null || true)"
        echo "mount source=$src uuid=${uuid:-<none>}"

        if command -v fuser >/dev/null && fuser -m "$MNT" >/dev/null 2>&1; then
            echo "$MNT 을 사용 중인 프로세스:" >&2
            fuser -vm "$MNT" >&2 || true
            die "먼저 노드 프로세스를 멈춰라."
        fi
        run umount "$MNT"
    else
        echo "$MNT 는 마운트돼 있지 않다 (재연결만 한다)."
    fi

    run nvme disconnect -n "$NQN"
    run modprobe nvme-fabrics
    run modprobe "nvme-$target"

    local cmd=(nvme connect -t "$target" -a "$addr" -s "$PORT" -n "$NQN")
    [[ -n "$QUEUES" ]] && cmd+=(-i "$QUEUES")
    run "${cmd[@]}"

    if [[ $DRY -eq 1 ]]; then
        echo "(dry-run) 장치 대기, 마운트, 검증은 생략"
        return 0
    fi

    # 새 namespace 장치가 나타날 때까지 대기
    newsrc=""
    for i in $(seq 1 40); do
        newsrc="$(find_ns "$NQN" | head -n1 || true)"
        if [[ -n "$newsrc" && -b "/dev/$newsrc" ]]; then break; fi
        sleep 0.5
    done
    [[ -n "$newsrc" ]] || die "연결 후 $NQN 의 장치를 찾지 못했다."
    echo "new device: /dev/$newsrc"

    if [[ -n "$src" ]]; then
        if [[ -n "$uuid" ]]; then
            run mount -U "$uuid" "$MNT"
        else
            run mount "/dev/$newsrc" "$MNT"
        fi
    fi

    printf '%s\n' "$NQN" > "$STATE_FILE" 2>/dev/null || true

    echo
    echo "=== 검증 ==="
    status
    if [[ -n "$EXPECT_DEV" ]]; then
        local now
        now="$(findmnt -no SOURCE "$MNT" 2>/dev/null || true)"
        if [[ "$now" != "$EXPECT_DEV" ]]; then
            echo "WARNING: 마운트 장치($now) 가 기대값($EXPECT_DEV)과 다르다." >&2
            echo "         env.sh 의 DEVICE / 클러스터 spec 의 device_path 를 확인하라." >&2
        fi
    fi
}

[[ $# -ge 1 ]] || usage
CMD="$1"; shift

while [[ $# -gt 0 ]]; do
    case "$1" in
        -n) NQN="${2:?-n 값 필요}"; shift 2 ;;
        -i) QUEUES="${2:?-i 값 필요}"; shift 2 ;;
        -d) EXPECT_DEV="${2:?-d 값 필요}"; shift 2 ;;
        --dry-run) DRY=1; shift ;;
        --force) FORCE=1; shift ;;
        *) usage ;;
    esac
done

case "$CMD" in
    status)    status ;;
    tcp|rdma)  switch_to "$CMD" ;;
    *)         usage ;;
esac