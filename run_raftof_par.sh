#!/usr/bin/env bash
set -euo pipefail
BATCH="${BATCH:-1}"
N="${1:?usage: $0 N [payload] [count] [tcp|tcp-ib|rdma]}"
PAYLOAD="${2:-4096}"
COUNT="${3:-30000}"                         # 프로세스당 측정 명령 수
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

D="$RAFT_ROOT/results/par_$(date +%Y%m%d_%H%M%S)_N${N}_${PAYLOAD}B_${TRANSPORT_MODE}_gc_b${BATCH}"
mkdir -p "$D"
RC="./build/raft_client -addrs $ADDRS -transport $CLIENT_TRANSPORT"

$RC -op ae-stats > "$D/ae_before.txt" 2>&1
$RC -op apply -n 1000 -size "$PAYLOAD" -batch 1 >/dev/null     # 워밍업 (1개 프로세스)
"$SCRIPT_DIR/node_cfg.sh" raft_node > "$D/node_args.txt" 2>&1

GO="$D/go"
for i in $(seq 1 "$N"); do
  $RC -op apply-timed -n "$COUNT" -size "$PAYLOAD" -batch "$BATCH" -start-file "$GO" > "$D/c$i.txt" 2>&1 &
done
for _ in $(seq 1 240); do                       # 최대 120초
  [ "$(ls "$D"/go.ready.* 2>/dev/null | wc -l)" -ge "$N" ] && break
  sleep 0.5
done
t0=$(date +%s.%N)
touch "$GO"
wait
t1=$(date +%s.%N)

$RC -op ae-stats > "$D/ae_after.txt" 2>&1
$RC -op commit-index > "$D/commit_after.txt" 2>&1        # ← 추가

python3 "$SCRIPT_DIR/summarize_par.py" "$D" --elapsed "$(echo "$t1 - $t0" | bc)" --size "$PAYLOAD" --batch "${BATCH:-1}" | tee "$D/summary.txt"