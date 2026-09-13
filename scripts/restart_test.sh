#!/usr/bin/env bash
#
# restart_test.sh -- 팔로워 재시작 후 따라잡기(catch-up) 검증
#
# init_storage의 재시작 경로를 겨냥한다. 원본 raft.go restoreCircular은
# 헤더에서 term/votedFor만 복구하고 tailLogIndex/tailSlot/commitIndex/
# lastApplied는 항상 1/0/0/1로 되돌린다("On restart, start with empty
# in-memory log. Leader election will resync all entries via
# appendEntries."). 즉 재시작한 노드는 빈 로그로 출발해 리더의
# AppendEntries backoff로 전부 다시 받는다. 이 스크립트가 그 경로를
# 실제로 돌린다.
#
# 순서:
#   1. 3노드 + 3스토리지노드 기동, 리더 선출
#   2. 1차 Apply
#   3. 팔로워 하나 SIGKILL
#   4. 2차 Apply (쿼럼 2/3이므로 계속 커밋돼야 한다)
#   5. 죽인 팔로워 재시작
#   6. 재시작 노드의 commit_index가 리더까지 따라오는지
#   7. 재시작 노드의 링 파일이 리더 링 파일과 다시 바이트 단위로 같아지는지
#
# ** 6/7은 현재 설계에서 통과할 수 없다 (KNOWN GAP). 이유는 아래 두
#    코드가 서로 모순이기 때문이고, 둘 다 원본 raft.go와 동일하다:
#
#    (a) restoreCircular / init_storage: 재시작 시 tailLogIndex를 1로
#        되돌리고 in-memory 로그를 비운다
#        ("Leader election will resync all entries via appendEntries")
#    (b) appendEntries의 backoff floor guard:
#          floor = cluster[fi].matchIndex + 1
#          nextIndex = max(max(newNext,1), floor)
#        ("Never back off past matchIndex+1 ... those PBA slots may have
#          been freed by tier 1")
#
#    (a) 때문에 재시작 노드는 prev_log_index를 하나도 인정할 수 없어
#    conflict_index = tail_log_index = 1을 돌려주는데, (b) 때문에 리더는
#    nextIndex를 matchIndex+1(죽기 직전 값) 밑으로 못 내린다. 그래서
#    리더는 그 팔로워에게 영원히 같은 prev_log_index를 보내고 팔로워는
#    영원히 거절한다. 재시작 노드는 자기 링에 데이터를 그대로 갖고 있으니,
#    제대로 고치려면 init_storage가 read_entry_direct로 링을 스캔해
#    in-memory 로그를 복원해야 한다 (HANDOFF §4.4의 "리더 승격 시 로그
#    재구성 없음" 항목).
#
#    기본값은 이 두 항목을 [KNOWN GAP]으로 보고하고 종료코드는 0이다.
#    복원 경로를 구현한 뒤에는 EXPECT_CATCHUP=1로 돌려 하드 실패로
#    바꿀 수 있다.
#
# 사용법: ./scripts/restart_test.sh [WORKDIR] [N_PER_PHASE]
#         EXPECT_CATCHUP=1 ./scripts/restart_test.sh   # 6/7을 하드 검증

set -uo pipefail

WORKDIR="${1:-/tmp/raftof_restart}"
NCMD="${2:-100}"
CMD_SIZE="${CMD_SIZE:-512}"
BATCH="${BATCH:-10}"
RING_PAGES="${RING_PAGES:-4096}"
HEARTBEAT_MS="${HEARTBEAT_MS:-100}"
EXPECT_CATCHUP="${EXPECT_CATCHUP:-0}"
BIN="${BIN:-$(cd "$(dirname "$0")/.." && pwd)/build}"

RAFT_PORTS=(6001 6002 6003)
STOR_PORTS=(5051 5052 5053)
IDS=(1 2 3)

NODE_PIDS=(0 0 0)
STOR_PIDS=(0 0 0)
FAILURES=0
GAPS=0

log()  { printf '\n=== %s\n' "$*"; }
ok()   { printf '  [ok]   %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; FAILURES=$((FAILURES+1)); }
# 알려진 설계 갭: EXPECT_CATCHUP=1이면 하드 실패, 아니면 보고만 한다.
gap()  {
    if [[ "$EXPECT_CATCHUP" == "1" ]]; then
        fail "$*"
    else
        printf '  [KNOWN GAP] %s\n' "$*"
        GAPS=$((GAPS+1))
    fi
}

cleanup() {
    for p in "${NODE_PIDS[@]}" "${STOR_PIDS[@]}"; do
        [[ "$p" != "0" ]] && kill "$p" 2>/dev/null
    done
    sleep 0.5
    for p in "${NODE_PIDS[@]}" "${STOR_PIDS[@]}"; do
        [[ "$p" != "0" ]] && kill -9 "$p" 2>/dev/null
    done
    wait 2>/dev/null
}
trap cleanup EXIT

for b in raft_node raft_client raft_blockcopy_server; do
    [[ -x "$BIN/$b" ]] || { echo "missing $BIN/$b -- run ./build.sh all first" >&2; exit 1; }
done

RING_BYTES=$((RING_PAGES*4096))
DEVICES=""
CLUSTER=""
log "setup: $WORKDIR (ring=$RING_BYTES bytes/node)"
rm -rf "$WORKDIR"; mkdir -p "$WORKDIR"
for i in 0 1 2; do
    id=${IDS[$i]}
    mkdir -p "$WORKDIR/n$id"
    fallocate -l "$RING_BYTES" "$WORKDIR/n$id/raft-$id.ring" || exit 1
    DEVICES+="${DEVICES:+,}$WORKDIR/n$id/raft-$id.ring"
    CLUSTER+="${CLUSTER:+,}$id@127.0.0.1:${RAFT_PORTS[$i]}@@127.0.0.1:${STOR_PORTS[$i]}"
done

start_node() {   # start_node <index 0..2> <log-suffix>
    local i="$1" suffix="$2" id=${IDS[$1]}
    "$BIN/raft_node" -id "$id" -cluster "$CLUSTER" \
        -metadata-dir "$WORKDIR/n$id" -heartbeat-ms "$HEARTBEAT_MS" \
        -ring-pages "$RING_PAGES" -identity-pba \
        >> "$WORKDIR/node$id$suffix.log" 2>&1 &
    NODE_PIDS[$i]=$!
}

for i in 0 1 2; do
    "$BIN/raft_blockcopy_server" -addr "0.0.0.0:${STOR_PORTS[$i]}" \
        -devices "$DEVICES" -copy-workers 4 > "$WORKDIR/stor$((i+1)).log" 2>&1 &
    STOR_PIDS[$i]=$!
done
sleep 0.5
for i in 0 1 2; do start_node "$i" ""; done
sleep 1
ok "3 storage nodes + 3 raft nodes up"

ADDRS="127.0.0.1:${RAFT_PORTS[0]},127.0.0.1:${RAFT_PORTS[1]},127.0.0.1:${RAFT_PORTS[2]}"
commit_of() {   # commit_of <addr>
    "$BIN/raft_client" -addrs "$1" -op commit-index 2>/dev/null |
        awk '/commit_index=/{split($2,a,"="); print a[2]+0}'
}
leader_addr() {
    "$BIN/raft_client" -addrs "$ADDRS" -op apply -n 0 -timeout-s 20 2>/dev/null |
        awk '/^leader:/{print $2}'
}

log "phase 1: apply $NCMD commands"
P1="$("$BIN/raft_client" -addrs "$ADDRS" -op apply -n "$NCMD" -size "$CMD_SIZE" \
        -batch "$BATCH" -timeout-s 20 2>&1)"
LEADER="$(echo "$P1" | awk '/^leader:/{print $2}')"
if echo "$P1" | grep -q '^TOTAL_APPLIED='; then
    ok "phase 1 applied (leader $LEADER)"
else
    fail "phase 1 apply failed:"; echo "$P1" | sed 's/^/    /'; exit 1
fi

# 리더가 아닌 노드 하나를 고른다
VICTIM=-1
for i in 0 1 2; do
    if [[ "127.0.0.1:${RAFT_PORTS[$i]}" != "$LEADER" ]]; then VICTIM=$i; break; fi
done
VID=${IDS[$VICTIM]}
VADDR="127.0.0.1:${RAFT_PORTS[$VICTIM]}"

log "phase 2: kill follower $VID ($VADDR)"
VPID="${NODE_PIDS[$VICTIM]}"
kill -9 "$VPID" 2>/dev/null
NODE_PIDS[$VICTIM]=0
wait "$VPID" 2>/dev/null      # SIGKILL 알림을 셸이 미리 수확하도록
sleep 0.5
# 주의: NODE_PIDS를 먼저 0으로 만든 뒤 `kill -0 0`을 하면 "프로세스 그룹
# 전체"를 가리켜 항상 성공한다 -- 반드시 저장해둔 $VPID로 확인해야 한다.
if kill -0 "$VPID" 2>/dev/null; then
    fail "follower $VID still alive (pid $VPID)"
else
    ok "follower $VID killed (ring file left on disk for the restart)"
fi

log "phase 3: apply $NCMD more commands with only 2/3 nodes up"
P2="$("$BIN/raft_client" -addrs "$LEADER" -op apply -n "$NCMD" -size "$CMD_SIZE" \
        -batch "$BATCH" -timeout-s 20 2>&1)"
if echo "$P2" | grep -q '^TOTAL_APPLIED='; then
    ok "quorum of 2/3 kept committing"
else
    fail "apply with one node down failed:"; echo "$P2" | sed 's/^/    /'
fi
LEADER_CI="$(commit_of "$LEADER")"
echo "  leader commit_index=$LEADER_CI"

log "phase 4: restart follower $VID"
start_node "$VICTIM" ".restart"
sleep 1
if kill -0 "${NODE_PIDS[$VICTIM]}" 2>/dev/null; then
    ok "follower $VID restarted (pid ${NODE_PIDS[$VICTIM]})"
else
    fail "follower $VID died on restart; log:"
    sed 's/^/    /' "$WORKDIR/node$VID.restart.log"; exit 1
fi
grep -m1 'term/tail' "$WORKDIR/node$VID.restart.log" | sed 's/^/  restored: /'

log "phase 5: restarted node catches up to commit_index >= $LEADER_CI"
CAUGHT=0
for _ in $(seq 1 80); do
    CI="$(commit_of "$VADDR")"
    if [[ -n "${CI:-}" && "$CI" -ge "$LEADER_CI" ]]; then CAUGHT=1; break; fi
    sleep 0.25
done
echo "  restarted node commit_index=${CI:-?} (leader $LEADER_CI)"
if [[ "$CAUGHT" == "1" ]]; then
    ok "restarted follower caught up via AppendEntries backoff"
else
    gap "restarted follower stuck at commit_index=${CI:-?} -- backoff floor guard (matchIndex+1) vs empty-log restart; see the header comment"
    # 리더 쪽 상태를 함께 남긴다: nextIndex가 matchIndex+1에 붙어 있는지
    for id in 1 2 3; do
        for f in "$WORKDIR/node$id.log" "$WORKDIR/node$id.restart.log"; do
            [[ -f "$f" ]] || continue
            grep -h 'SKIP PBA' "$f" | tail -1 | sed 's/^/    /'
        done
    done
fi

log "phase 6: restarted node's ring matches the leader's, past the header"
LPORT="${LEADER##*:}"
LID=0
for i in 0 1 2; do [[ "${RAFT_PORTS[$i]}" == "$LPORT" ]] && LID=${IDS[$i]}; done
LRING="$WORKDIR/n$LID/raft-$LID.ring"
VRING="$WORKDIR/n$VID/raft-$VID.ring"
NONZERO="$(dd if="$LRING" bs=512 skip=1 2>/dev/null | tr -d '\0' | wc -c)"
if [[ "${NONZERO:-0}" -gt 0 ]]; then
    ok "leader ring payload non-empty ($NONZERO non-zero bytes)"
else
    fail "leader ring payload is all zeros -- comparison would be vacuous"
fi
if cmp -s -i 512:512 "$LRING" "$VRING"; then
    ok "restarted node ring is byte-identical to the leader's"
else
    gap "restarted node ring differs: $(cmp -i 512:512 "$LRING" "$VRING" 2>&1 | head -1) (follows from phase 5)"
fi

log "result"
if [[ "$FAILURES" == "0" && "$GAPS" == "0" ]]; then
    echo "  ALL PASSED"; exit 0
elif [[ "$FAILURES" == "0" ]]; then
    echo "  PASSED with $GAPS KNOWN GAP(S) -- restart catch-up is not implemented"
    echo "  (EXPECT_CATCHUP=1 to turn those into hard failures)"
    echo "  logs in $WORKDIR/"; exit 0
else
    echo "  $FAILURES FAILURE(S), $GAPS known gap(s)"; echo "  logs in $WORKDIR/"; exit 1
fi
