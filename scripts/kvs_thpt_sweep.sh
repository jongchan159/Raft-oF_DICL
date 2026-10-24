#!/usr/bin/env bash
#
# kvs_thpt_sweep.sh -- KVS 처리량이 클라이언트 수에 따라 어디서 포화하는지
#                      재고, 같은 축에서 etcd 와 비교한다
#
# 묻는 것: **동시성 1에서 본 2~3배 지연 우위가 포화 처리량에서도 남는가.**
# 지금까지의 kvs-lat / kvs-loop 숫자는 전부 outstanding 1 이라 ops/s 가
# 1/latency 의 재진술이었다. 용량을 재려면 포화시켜야 한다.
#
# ── 왜 PUT 과 GET 을 따로 스윕하는가 ───────────────────────────────────────
# 동시성 1 에서는 요청이 한 번에 하나뿐이라 읽기·쓰기가 간섭할 수 없고,
# 그래서 따로 잰 지연의 가중평균으로 혼합 성능을 합성해도 정당했다.
# **C > 1 에서는 성립하지 않는다** -- 둘이 같은 서버를 두고 경합한다.
# 그래서 합성하지 않고 PUT-only / GET-only 를 각각 스윕해 1:1 로 대응시킨다:
#
#     raft_client -put-pct 100   <->  benchmark put
#     raft_client -put-pct 0     <->  benchmark range  (linearizable)
#
# 혼합 지점은 넣지 않았다. etcd 쪽 대응물인 `benchmark txn-mixed` 의 읽기가
# 단일 키 조회(0.4ms)의 25배인 11~13ms 로 나오는데 원인이 확정되지 않았다
# (clients=1 이라 부하 간섭은 불가능하므로, 그 읽기가 단일 키 조회가 아닐
# 가능성이 높다 -- tools/benchmark/cmd/txn_mixed.go 확인 필요).
# 정체를 모르는 연산을 비교축에 올리지 않는다.
#
# ── 두 시스템이 같은 주소를 쓴다 ──────────────────────────────────────────
# raft 리더도 etcd 도 10.0.0.x:6001 이라 **동시에 띄울 수 없다.** 그래서
# SYSTEM 으로 한쪽만 돌리고, 클러스터를 바꿔 끼운 뒤 다시 돌린다. 두 번의
# 실행이 같은 summary.csv 에 누적되므로 OUTDIR 을 같게 주면 된다.
#
# ── etcd --conns 를 clients 와 같이 올려야 한다 ───────────────────────────
# etcd benchmark 의 --conns 기본값은 1 이다. --clients=32 만 주면 32 개
# 클라이언트가 HTTP/2 커넥션 하나를 공유한다. 반면 raft_client 는 클라이언트
# 스레드마다 커넥션을 따로 맺는다 (apps/raft_client_main.cpp:397 -- "클라이언트
# 마다 커넥션 하나, 서버는 커넥션마다 스레드를 띄운다"). 조건을 맞추려면
# --conns=C 가 필요하다.
#
# ── 실행 시간 기준이 다르다 ───────────────────────────────────────────────
# raft_client 는 -duration-s (시간 고정), etcd benchmark 는 --total (횟수 고정)
# 이다. 횟수 고정이면 C 가 커질수록 run 이 짧아져 워밍업 비중이 커지므로,
# etcd 쪽 --total 을 TOTAL_PER_CLIENT * C 로 스케일해 run 길이를 비슷하게 맞춘다.
#
# 사용법:
#   SYSTEM=raftof OUTDIR=~/kvs_thpt ./scripts/kvs_thpt_sweep.sh
#   (클러스터 교체)
#   SYSTEM=etcd   OUTDIR=~/kvs_thpt ./scripts/kvs_thpt_sweep.sh
#
# 환경변수:
#   SYSTEM       raftof | etcd  (필수)
#   OUTDIR       결과 디렉터리 (기본 ~/kvs_thpt_<timestamp>)
#   CLIENTS      클라이언트 수 목록 (기본 "1 2 4 8 16 32 64")
#   SIZE         값 크기 바이트 (기본 4096)
#   KEYS         키 공간 크기 (기본 10000)
#   OPS          돌릴 연산 (기본 "put get")
#
#   raftof 전용:
#   ADDRS        raft 노드 주소 (필수)
#   RAFT_BIN     raft_client 경로 (기본 <repo>/build/raft_client)
#   DURATION     지점당 측정 시간 초 (기본 30)
#
#   etcd 전용:
#   EP               etcd 엔드포인트 (필수)
#   BENCHMARK_BIN    benchmark 경로 (기본 PATH 의 benchmark)
#   TOTAL_PER_CLIENT 클라이언트당 요청 수 (기본 20000)
#   RANGE_KEY        GET 스윕이 읽을 키 (기본 benchkey). 없으면 만든다
#   ETCDCTL_BIN      etcdctl 경로 (기본 PATH 의 etcdctl)

set -uo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"

SYSTEM="${SYSTEM:-}"
OUTDIR="${OUTDIR:-$HOME/kvs_thpt_$(date +%Y%m%d-%H%M%S)}"
CLIENTS="${CLIENTS:-1 2 4 8 16 32 64}"
SIZE="${SIZE:-4096}"
KEYS="${KEYS:-10000}"
OPS="${OPS:-put get}"

RAFT_BIN="${RAFT_BIN:-$REPO/build/raft_client}"
DURATION="${DURATION:-30}"

BENCHMARK_BIN="${BENCHMARK_BIN:-benchmark}"
ETCDCTL_BIN="${ETCDCTL_BIN:-etcdctl}"
TOTAL_PER_CLIENT="${TOTAL_PER_CLIENT:-20000}"
RANGE_KEY="${RANGE_KEY:-benchkey}"

log()  { printf '\n=== %s\n' "$*"; }
ok()   { printf '  [ok]   %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; }

if [[ "$SYSTEM" != "raftof" && "$SYSTEM" != "etcd" ]]; then
    fail "SYSTEM must be 'raftof' or 'etcd' (두 클러스터가 같은 포트를 쓰므로 한 번에 하나씩)"
    exit 1
fi

mkdir -p "$OUTDIR"
SUMMARY_CSV="$OUTDIR/summary.csv"
# 두 번의 실행(raftof / etcd)이 같은 파일에 누적된다. 헤더는 처음 한 번만.
if [[ ! -f "$SUMMARY_CSV" ]]; then
    echo "system,op,clients,size,ops_per_s,p50_us,p99_us" > "$SUMMARY_CSV"
fi

# ---------------------------------------------------------------------------
# raftof 지점 하나.  $1 = put|get   $2 = clients
#
# -put-pct 100 이면 전부 PUT, 0 이면 전부 GET 이다 (kvs_loop 의 분기가
# `pct(rng) <= put_pct` 이고 pct 는 1..100 이라 0 은 절대 참이 아니다).
# ---------------------------------------------------------------------------
run_raftof() {
    local op="$1" c="$2"
    local pct=100; [[ "$op" == "get" ]] && pct=0
    local tag="raftof-$op-c$c"
    local blog="$OUTDIR/$tag.log"

    local out
    out=$("$RAFT_BIN" -addrs "$ADDRS" -op kvs-loop \
        -size "$SIZE" -keys "$KEYS" -put-pct "$pct" \
        -clients "$c" -duration-s "$DURATION" \
        -csv "$OUTDIR/series-$tag.csv" 2>&1)
    local rc=$?
    printf '%s\n' "$out" > "$blog"
    if [[ $rc -ne 0 ]]; then
        fail "$tag -- see $blog"
        printf '%s\n' "$out" | tail -3 | sed 's/^/       /'
        return 1
    fi

    # "kvs-loop: 214759 ops in 60.00s = 3579 ops/s (PUT ..., GET ...)"
    local ops
    ops=$(sed -n 's/.*= \([0-9]*\) ops\/s.*/\1/p' <<<"$out" | tail -1)
    # 지연은 해당 연산 줄에서. PUT-only 면 PUT 줄, GET-only 면 GET 줄.
    local lat_line
    lat_line=$(grep -E "^  $(tr '[:lower:]' '[:upper:]' <<<"$op") " <<<"$out" | tail -1)
    local p50 p99
    p50=$(sed -n 's/.*p50= *\([0-9]*\).*/\1/p' <<<"$lat_line")
    p99=$(sed -n 's/.*p99= *\([0-9]*\).*/\1/p' <<<"$lat_line")

    if [[ -z "$ops" ]]; then
        fail "$tag -- ops/s 를 못 읽었다. see $blog"
        return 1
    fi
    echo "raftof,$op,$c,$SIZE,$ops,${p50:--},${p99:--}" >> "$SUMMARY_CSV"
    printf '  %-6s C=%-3s  %8s ops/s   p50=%sus p99=%sus\n' "$op" "$c" "$ops" "${p50:--}" "${p99:--}"
    return 0
}

# ---------------------------------------------------------------------------
# etcd 지점 하나.  $1 = put|get   $2 = clients
#
# --conns 를 --clients 와 같이 준다 (머리 주석 참조). --total 은 C 에 비례
# 시켜 run 길이를 raftof 쪽 -duration-s 와 비슷하게 맞춘다.
# ---------------------------------------------------------------------------
run_etcd() {
    local op="$1" c="$2"
    local tag="etcd-$op-c$c"
    local blog="$OUTDIR/$tag.log"
    local total=$((TOTAL_PER_CLIENT * c))

    local out
    if [[ "$op" == "put" ]]; then
        out=$("$BENCHMARK_BIN" put --endpoints="$EP" \
            --clients="$c" --conns="$c" --total="$total" \
            --key-size=64 --val-size="$SIZE" --key-space-size="$KEYS" 2>&1)
    else
        # linearizable read. 값이 실제로 든 키를 읽어야 한다 -- 없는 키를
        # 읽으면 빈 응답이라 payload 비용이 빠진다.
        out=$("$BENCHMARK_BIN" range "$RANGE_KEY" --consistency=l \
            --endpoints="$EP" --clients="$c" --conns="$c" --total="$total" 2>&1)
    fi
    local rc=$?
    printf '%s\n' "$out" > "$blog"
    if [[ $rc -ne 0 ]]; then
        fail "$tag -- see $blog"
        printf '%s\n' "$out" | tail -3 | sed 's/^/       /'
        return 1
    fi

    # "  Requests/sec: 2475.4433"
    local ops p50 p99
    ops=$(sed -n 's/.*Requests\/sec: *\([0-9.]*\).*/\1/p' <<<"$out" | tail -1)
    # 분위수 줄은 etcd 버전에 따라 두 형식으로 나온다. 둘 다 받는다:
    #   "  50% in 0.0004 secs."          -> 초
    #   "  50%    in 400 us (0.400 ms)"  -> 마이크로초
    # 주의: "99.9% in ..." 이 "99% in ..." 패턴에 걸리면 안 되므로 % 앞을
    # 숫자로 고정한다 (99 뒤에 바로 % 가 와야 한다).
    pctile() {   # $1 = 50 | 99  -> us, 없으면 "-"
        local line
        line=$(grep -E "^ *$1% +in " <<<"$out" | tail -1)
        [[ -z "$line" ]] && { echo "-"; return; }
        if [[ "$line" == *secs* ]]; then
            sed -n 's/.* in \([0-9.]*\) secs.*/\1/p' <<<"$line" |
                awk '{ printf "%d", $1*1e6 }'
        else
            sed -n 's/.* in \([0-9]*\) us.*/\1/p' <<<"$line"
        fi
    }
    p50=$(pctile 50)
    p99=$(pctile 99)
    [[ -z "$p50" ]] && p50="-"
    [[ -z "$p99" ]] && p99="-"

    if [[ -z "$ops" ]]; then
        fail "$tag -- Requests/sec 를 못 읽었다. see $blog"
        return 1
    fi
    echo "etcd,$op,$c,$SIZE,$ops,$p50,$p99" >> "$SUMMARY_CSV"
    printf '  %-6s C=%-3s  %8s ops/s   p50=%sus p99=%sus\n' "$op" "$c" "$ops" "$p50" "$p99"
    return 0
}

# ---------------------------------------------------------------------------
log "system=$SYSTEM  output -> $OUTDIR"
printf '  clients : %s\n  size    : %s B\n  keys    : %s\n  ops     : %s\n' \
    "$CLIENTS" "$SIZE" "$KEYS" "$OPS"

if [[ "$SYSTEM" == "raftof" ]]; then
    : "${ADDRS:?ADDRS 가 필요하다}"
    [[ -x "$RAFT_BIN" ]] || { fail "no raft_client at $RAFT_BIN"; exit 1; }
    printf '  duration: %ss/point\n' "$DURATION"
    # 지점마다 -keys 개를 preload 한다 (kvs_loop 도입부). 10000 x 4096B 면
    # 지점당 수 초가 더 붙는다 -- 측정 구간 밖이라 결과에는 영향이 없다.
else
    : "${EP:?EP 가 필요하다}"
    printf '  total   : %s x clients\n' "$TOTAL_PER_CLIENT"
    # GET 스윕이 읽을 키를 준비한다. 이미 있으면 덮어써도 무해하다.
    if ! "$ETCDCTL_BIN" --endpoints="$EP" put "$RANGE_KEY" \
            "$(head -c "$SIZE" /dev/zero | tr '\0' x)" >/dev/null 2>&1; then
        fail "etcdctl put $RANGE_KEY 실패 -- GET 스윕이 빈 응답을 읽게 된다"
        exit 1
    fi
    ok "seeded $RANGE_KEY with ${SIZE}B"
fi

FAILED=0
for op in $OPS; do
    log "$SYSTEM / $op"
    for c in $CLIENTS; do
        if [[ "$SYSTEM" == "raftof" ]]; then
            run_raftof "$op" "$c" || FAILED=$((FAILED + 1))
        else
            run_etcd "$op" "$c" || FAILED=$((FAILED + 1))
        fi
    done
done

# ---------------------------------------------------------------------------
log "summary -- ops/s (rows = clients)"
for op in $OPS; do
    printf '\n  op = %s\n' "$op"
    printf '  %8s %14s %14s %10s\n' "clients" "raftof" "etcd" "ratio"
    for c in $CLIENTS; do
        r=$(awk -F, -v o="$op" -v c="$c" \
            'NR>1 && $1=="raftof" && $2==o && $3==c { print $5 }' "$SUMMARY_CSV" | tail -1)
        e=$(awk -F, -v o="$op" -v c="$c" \
            'NR>1 && $1=="etcd" && $2==o && $3==c { print $5 }' "$SUMMARY_CSV" | tail -1)
        ratio=$(awk -v r="${r:-}" -v e="${e:-}" \
            'BEGIN{ if (r=="" || e=="" || e+0==0) print "-"; else printf "%.2fx", r/e }')
        printf '  %8s %14s %14s %10s\n' "$c" "${r:--}" "${e:--}" "$ratio"
    done
done

log "how to read this"
cat <<'NOTE'
  - ops/s 가 C 를 올려도 더 안 오르는 지점이 포화점이다. 그 전까지는 여전히
    1/latency 를 클라이언트 수만큼 곱한 값일 뿐이라 용량이 아니다.
  - 포화 전에 p99 가 먼저 치솟으면 큐잉이 시작된 것이다. 포화 ops/s 와 함께
    그 지점의 p99 를 같이 보고할 것.
  - 두 시스템을 한 번에 띄울 수 없으므로 두 실행 사이에 클러스터 상태(캐시,
    페이지캐시, SSD 상태)가 달라질 수 있다. 의심되면 순서를 바꿔 한 번 더
    돌려 재현되는지 본다.
  - etcd range 는 같은 키 하나를 반복해서 읽는다. raftof 의 GET 은 -keys
    공간에서 무작위로 고른다 (kvs_loop). etcd 쪽이 캐시 지역성에서 유리하다.
NOTE

printf '\n  summary csv : %s\n  logs        : %s\n' "$SUMMARY_CSV" "$OUTDIR"
if [[ $FAILED -gt 0 ]]; then
    fail "$FAILED point(s) failed"
    exit 1
fi
ok "all points completed"
exit 0
