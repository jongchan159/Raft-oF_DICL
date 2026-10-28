#!/usr/bin/env bash
set -euo pipefail
BATCH="${BATCH:-1}"
N="${1:?usage: $0 N [payload] [count] [tcp|tcp-ib|rdma]}"
PAYLOAD="${2:-4096}"
COUNT="${3:-100}"                         # 프로세스당 측정 명령 수
TRANSPORT_MODE="${4:-rdma}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"
cd "$RAFT_ROOT"

case "$TRANSPORT_MODE" in
  tcp)    select_transport_env tcp;    CLIENT_TRANSPORT=tcp ;;
  tcp-ib) select_transport_env tcp-ib; CLIENT_TRANSPORT=tcp ;;
  rdma)   select_transport_env rdma;   CLIENT_TRANSPORT=rdma ;;
  *) echo "bad mode"; exit 1 ;;
esac

D="$RAFT_ROOT/results/par_$(date +%Y%m%d_%H%M%S)_N${N}_${PAYLOAD}B_${TRANSPORT_MODE}_b${BATCH}"
mkdir -p "$D"
RC="./build/raft_client -addrs $ADDRS -transport $CLIENT_TRANSPORT"

# ---- 시작 장벽 파일: 결과 폴더 밖 임시 폴더에 둔다 (go, go.ready.<pid>) ----
BAR="$(mktemp -d /tmp/raftbar.XXXXXX)"
GO="$BAR/go"
cleanup() {
  # 스크립트가 중간에 끝나면(Ctrl-C, 에러) go를 기다리는 클라이언트가 남지 않게 정리한다.
  # (비대화형 셸에서 백그라운드 작업은 SIGINT를 무시하므로 직접 죽여야 한다)
  local p
  p="$(jobs -p)"
  if [ -n "$p" ]; then kill $p 2>/dev/null || true; fi
  rm -rf "$BAR"
}
trap cleanup EXIT

$RC -op ae-stats > "$D/ae_before.txt" 2>&1
$RC -op apply -n 1000 -size "$PAYLOAD" -batch 1 >/dev/null     # 워밍업 (1개 프로세스)
"$SCRIPT_DIR/node_cfg.sh" raft_node > "$D/node_args.txt" 2>&1

# 1) 클라이언트 N개를 띄운다. 각자 연결/리더 확인 후 준비 파일을 만들고 $GO를 기다린다.
for i in $(seq 1 "$N"); do
  $RC -op apply-timed -n "$COUNT" -size "$PAYLOAD" -batch "$BATCH" -start-file "$GO" > "$D/c$i.txt" 2>&1 &
done

# 2) N개가 모두 준비될 때까지 대기 (최대 120초)
n_ready=0
for _ in $(seq 1 240); do
  n_ready="$(find "$BAR" -maxdepth 1 -name 'go.ready.*' 2>/dev/null | wc -l)"
  [ "$n_ready" -ge "$N" ] && break
  sleep 0.5
done
if [ "$n_ready" -lt "$N" ]; then
  echo "WARN: 준비된 클라이언트 $n_ready/$N 만으로 시작합니다 (c*.txt에서 실패한 클라이언트를 확인하세요)" >&2
fi

# 3) 동시에 시작. t0는 장벽 해제 시점이다 (준비 대기 시간은 elapsed에 넣지 않는다).
t0=$(date +%s.%N)
touch "$GO"
wait
t1=$(date +%s.%N)

$RC -op ae-stats > "$D/ae_after.txt" 2>&1
$RC -op commit-index > "$D/commit_after.txt" 2>&1

python3 "$SCRIPT_DIR/summarize_par.py" "$D" --elapsed "$(echo "$t1 - $t0" | bc)" --size "$PAYLOAD" --batch "${BATCH:-1}" | tee "$D/summary.txt"