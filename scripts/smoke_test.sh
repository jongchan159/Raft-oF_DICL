#!/usr/bin/env bash
#
# smoke_test.sh -- 3노드 + 3스토리지노드 로컬 e2e 테스트
#
# 블록 디바이스도 root 권한도 없이 PBA 블록복사 경로 전체를 돌린다.
# 방법: raft_node를 -identity-pba로 띄워 "논리 오프셋 == 물리 오프셋"으로
# 두고, 스토리지 노드(-devices)에 세 노드의 링 파일을 클러스터 인덱스
# 순서대로 그대로 넘긴다. 그러면
#   follower의 스토리지 노드가 pread(leader 링 파일, PBA) ->
#   pwrite(자기 링 파일, PBA)
# 를 수행하므로, 실서버에서 NVMe-oF 볼륨 사이에 일어나는 일과 같은
# 코드 경로를 탄다 (전송만 로컬 파일 I/O).
#
# 실서버(실제 NVMe-oF 디바이스)에서는 -identity-pba를 빼고 -devices에
# 실제 블록 디바이스를 넣어야 한다. 그때는 링 메타데이터 파일이 그
# 디바이스 위 파일시스템에 있어야 하고, FIEMAP으로 PBA가 해석된다.
#
# 검증 항목:
#   1. 리더가 선출된다
#   2. 클라이언트 Apply가 성공하고 커밋까지 완료된다
#   3. 세 노드의 commit_index가 모두 따라온다 (복제 프로토콜)
#   4. 리더 상태머신의 count/hash가 기대값과 맞는다
#   5. 세 노드의 링 파일 내용이 헤더(512B) 이후 바이트 단위로 같다
#      (PBA 블록복사가 실제로 올바른 바이트를 옮겼다는 증거)
#   6. 리더가 매 루프마다 no-op을 쌓지 않는다 (main_loop의 상태 분기:
#      become_leader는 candidate 상태에서만 호출돼야 한다)
#
# 사용법:
#   ./scripts/smoke_test.sh [WORKDIR] [N_COMMANDS]
#
# 주의: WORKDIR은 O_DIRECT를 지원하는 로컬 파일시스템이어야 한다
#       (ext4/xfs OK, NFS는 안 됨). 기본값이 /tmp인 이유.

set -uo pipefail

WORKDIR="${1:-/tmp/raftof_smoke}"
NCMD="${2:-200}"
CMD_SIZE="${CMD_SIZE:-512}"
BATCH="${BATCH:-10}"
RING_PAGES="${RING_PAGES:-4096}"        # 4096 * 4096B = 16MiB per node
HEARTBEAT_MS="${HEARTBEAT_MS:-100}"
BIN="${BIN:-$(cd "$(dirname "$0")/.." && pwd)/build}"

RAFT_PORTS=(6001 6002 6003)
STOR_PORTS=(5051 5052 5053)
IDS=(1 2 3)

PIDS=()
FAILURES=0

log()  { printf '\n=== %s\n' "$*"; }
ok()   { printf '  [ok]   %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; FAILURES=$((FAILURES+1)); }

cleanup() {
    for p in "${PIDS[@]:-}"; do
        [[ -n "$p" ]] && kill "$p" 2>/dev/null
    done
    sleep 0.5
    for p in "${PIDS[@]:-}"; do
        [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null
    done
    wait 2>/dev/null
}
trap cleanup EXIT

for b in raft_node raft_client raft_blockcopy_server; do
    if [[ ! -x "$BIN/$b" ]]; then
        echo "missing $BIN/$b -- run ./build.sh all first" >&2
        exit 1
    fi
done

log "setup: $WORKDIR (ring=${RING_PAGES} pages = $((RING_PAGES*4096)) bytes/node)"
rm -rf "$WORKDIR"
mkdir -p "$WORKDIR"

# 링 파일을 미리 만들어 둔다. 스토리지 노드가 시작 시 -devices를 열기
# 때문에, raft_node가 만들기를 기다리면 순서 의존이 생긴다.
# (raft_node의 init_storage도 같은 크기로 fallocate하며, 이미 크기가
#  충분하면 그대로 쓴다.)
RING_BYTES=$((RING_PAGES*4096))
DEVICES=""
for i in 0 1 2; do
    id=${IDS[$i]}
    mkdir -p "$WORKDIR/n$id"
    ring="$WORKDIR/n$id/raft-$id.ring"
    fallocate -l "$RING_BYTES" "$ring" || { echo "fallocate failed on $ring"; exit 1; }
    DEVICES+="${DEVICES:+,}$ring"
done
echo "  devices (cluster-index order): $DEVICES"

# -cluster 스펙: id@raft_addr@device_path@storage_host
# device_path를 비워 두면 raft_node가 자기 링 파일을 볼륨으로 쓴다.
CLUSTER=""
for i in 0 1 2; do
    CLUSTER+="${CLUSTER:+,}${IDS[$i]}@127.0.0.1:${RAFT_PORTS[$i]}@@127.0.0.1:${STOR_PORTS[$i]}"
done
echo "  cluster: $CLUSTER"

log "starting 3 storage nodes (raft_blockcopy_server)"
for i in 0 1 2; do
    "$BIN/raft_blockcopy_server" \
        -addr "0.0.0.0:${STOR_PORTS[$i]}" \
        -devices "$DEVICES" \
        -copy-workers 4 \
        > "$WORKDIR/stor$((i+1)).log" 2>&1 &
    PIDS+=($!)
done
sleep 0.5
for i in 0 1 2; do
    if ! kill -0 "${PIDS[$i]}" 2>/dev/null; then
        fail "storage node $((i+1)) died; log:"
        sed 's/^/    /' "$WORKDIR/stor$((i+1)).log"
        exit 1
    fi
done
ok "3 storage nodes up on ${STOR_PORTS[*]}"

log "starting 3 raft nodes"
for i in 0 1 2; do
    id=${IDS[$i]}
    "$BIN/raft_node" \
        -id "$id" \
        -cluster "$CLUSTER" \
        -metadata-dir "$WORKDIR/n$id" \
        -heartbeat-ms "$HEARTBEAT_MS" \
        -ring-pages "$RING_PAGES" \
        -identity-pba \
        > "$WORKDIR/node$id.log" 2>&1 &
    PIDS+=($!)
done
sleep 1
for i in 0 1 2; do
    id=${IDS[$i]}
    if ! kill -0 "${PIDS[$((i+3))]}" 2>/dev/null; then
        fail "raft node $id died; log:"
        sed 's/^/    /' "$WORKDIR/node$id.log"
        exit 1
    fi
done
ok "3 raft nodes up on ${RAFT_PORTS[*]}"

ADDRS="127.0.0.1:${RAFT_PORTS[0]},127.0.0.1:${RAFT_PORTS[1]},127.0.0.1:${RAFT_PORTS[2]}"

log "test 1+2: leader election and Apply of $NCMD commands (${CMD_SIZE}B, batch=$BATCH)"
APPLY_OUT="$("$BIN/raft_client" -addrs "$ADDRS" -op apply \
    -n "$NCMD" -size "$CMD_SIZE" -batch "$BATCH" -timeout-s 20 2>&1)"
echo "$APPLY_OUT" | sed 's/^/  /'
if echo "$APPLY_OUT" | grep -q "^TOTAL_APPLIED="; then
    ok "Apply completed"
    TOTAL_APPLIED="$(echo "$APPLY_OUT" | sed -n 's/^TOTAL_APPLIED=//p')"
else
    fail "Apply did not complete"
    TOTAL_APPLIED=0
fi

# 기대값: 로그 인덱스 1 = become_leader의 no-op,
#         2 = 클라이언트의 리더 탐색용 1바이트 명령,
#         3..(NCMD+2) = 본 명령들.
# 상태머신에 적용되는 것은 명령이 있는 엔트리만 -> count = NCMD + 1
EXPECT_COUNT=$((NCMD+1))
EXPECT_COMMIT=$((NCMD+2))

log "test 3: commit_index catches up on all 3 nodes (want >= $EXPECT_COMMIT)"
# 쿼럼(2/3)만 있으면 커밋되므로 세 번째 노드는 하트비트로 따라온다.
CI_OK=0
for _ in $(seq 1 60); do
    CI_OUT="$("$BIN/raft_client" -addrs "$ADDRS" -op commit-index 2>&1)"
    LAGGING="$(echo "$CI_OUT" | awk -v w="$EXPECT_COMMIT" \
        '/commit_index=/{split($2,a,"="); if (a[2]+0 < w) c++} END{print c+0}')"
    if [[ "$LAGGING" == "0" ]]; then CI_OK=1; break; fi
    sleep 0.25
done
echo "$CI_OUT" | sed 's/^/  /'
if [[ "$CI_OK" == "1" ]]; then
    ok "all 3 nodes reached commit_index >= $EXPECT_COMMIT"
else
    fail "some node did not reach commit_index $EXPECT_COMMIT"
fi

log "test 4: leader state machine count/hash (want count=$EXPECT_COUNT)"
HASH_OUT="$("$BIN/raft_client" -addrs "$ADDRS" -op hash 2>&1)"
echo "$HASH_OUT" | sed 's/^/  /'
LEADER_COUNT="$(echo "$HASH_OUT" | awk '{for(i=1;i<=NF;i++) if($i ~ /^count=/){split($i,a,"=");
    if (a[2]+0 > m) m=a[2]+0}} END{print m+0}')"
if [[ "$LEADER_COUNT" == "$EXPECT_COUNT" ]]; then
    ok "leader applied exactly $EXPECT_COUNT commands"
else
    fail "leader count=$LEADER_COUNT, expected $EXPECT_COUNT"
fi

log "test 5: ring files are byte-identical past the 512B header"
# 헤더(offset 0..511)는 노드별 term/tail/commit이 들어가므로 제외한다.
# 그 뒤는 PBA 블록복사가 옮긴 내용이라 전 구간이 같아야 한다
# (쓰이지 않은 뒤쪽은 양쪽 다 0).
R1="$WORKDIR/n1/raft-1.ring"

# 먼저 "비교 구간이 실제로 데이터를 담고 있는지"를 확인한다. 이게 없으면
# 복제가 아예 일어나지 않아 세 파일이 전부 0인 경우에도 cmp가 통과해서
# 이 테스트가 무의미해진다 (실제로 리더 선출이 안 되던 동안 그랬다).
NONZERO="$(dd if="$R1" bs=512 skip=1 2>/dev/null | tr -d '\0' | wc -c)"
if [[ "${NONZERO:-0}" -gt 0 ]]; then
    ok "leader-replicated ring payload is non-empty ($NONZERO non-zero bytes)"
else
    fail "ring payload past the header is all zeros -- nothing was replicated, so the byte comparison below would pass trivially"
fi

for id in 2 3; do
    R="$WORKDIR/n$id/raft-$id.ring"
    if cmp -s -i 512:512 "$R1" "$R"; then
        ok "node $id ring content matches node 1 (PBA copy moved the right bytes)"
    else
        DIFF_AT="$(cmp -i 512:512 "$R1" "$R" 2>&1 | head -1)"
        fail "node $id ring differs from node 1: $DIFF_AT"
    fi
done

log "test 6: no runaway no-op growth (become_leader re-entry via main_loop)"
# 리더가 매 메인루프마다 no-op을 쌓으면 commit_index가 명령 수와
# 무관하게 폭증한다. 여유를 두고 상한을 확인한다.
MAX_CI="$(echo "$CI_OUT" | awk '{for(i=1;i<=NF;i++) if($i ~ /^commit_index=/){split($i,a,"=");
    if (a[2]+0 > m) m=a[2]+0}} END{print m+0}')"
UPPER=$((EXPECT_COMMIT+20))
if [[ "$MAX_CI" -le "$UPPER" ]]; then
    ok "max commit_index=$MAX_CI within expected bound ($UPPER)"
else
    fail "max commit_index=$MAX_CI exceeds $UPPER -- no-op / election churn?"
fi

log "node log tails (informational)"
for id in 1 2 3; do
    echo "  --- node$id:"
    tail -5 "$WORKDIR/node$id.log" | sed 's/^/    /'
done

log "result"
if [[ "$FAILURES" == "0" ]]; then
    echo "  ALL PASSED"
    exit 0
else
    echo "  $FAILURES FAILURE(S)"
    echo "  logs in $WORKDIR/"
    exit 1
fi
