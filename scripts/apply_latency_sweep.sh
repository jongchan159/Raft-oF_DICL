#!/usr/bin/env bash
#
# apply_latency_sweep.sh -- 페이로드 크기를 1KiB 부터 2배씩 키우며 Raft apply
#                           지연이 어떻게 변하는지 재고, 결과를 CSV 한 장으로 낸다
#
# 묻는 것: **엔트리 하나가 커질 때 지연의 어느 항이 늘어나는가.** ApplyTimings
# 11항(Total/LHandler/LPersist/AENet/FHandler/ReplNet/StorageIO/QuorumWait/
# Mutex/CommitWait/residual)을 크기별로 나란히 놓는다.
#
# **서버는 이미 떠 있다고 가정한다.** 이 스크립트는 raft_node 도
# raft_blockcopy_server 도 띄우지 않고 죽이지도 않으며, 링 파일을 지우지도
# 않는다 (기동 절차는 E2E_EXPERIMENT.md §5). 하는 일은 raft_client 를 크기마다
# 돌리고 그 stdout 을 파싱하는 것뿐이다.
#
# 사용법:
#   ./scripts/apply_latency_sweep.sh [출력디렉터리]
#
# 환경변수:
#   ADDRS        -addrs 에 줄 주소 목록 (기본 10.0.0.7:6001,10.0.0.5:6001,10.0.0.6:6001)
#                **IPoIB 주소여야 한다** -- rdma_cm 이 IP 로 장치를 찾는다
#   SIZES        페이로드 바이트 목록 (기본 1Ki..128Ki 를 2배씩)
#   N            지점당 명령 수 (기본 10000)
#   BATCH        RPC 당 명령 수 (기본 1). 지연을 재는 것이므로 1 이 기본이다
#   REPS         지점당 반복 횟수 (기본 1). CSV 에 rep 컬럼으로 들어간다
#   TRANSPORT    rdma (기본) | tcp
#   TIMEOUT_S    리더 탐색 타임아웃 (기본 15)
#   BIN          바이너리 디렉터리 (기본 <repo>/build)
#   RING_BYTES   노드의 링 크기 = -ring-pages × 4096 (기본 16MiB).
#                워밍업 분량을 여기서 유도한다 -- 노드를 다른 -ring-pages 로
#                띄웠다면 **반드시 맞춰줄 것**
#   WARMUP       1 이면 시작 시 워밍업 1회 (기본 1). 0 이면 건너뛴다
#   WARMUP_SIZE / WARMUP_BATCH   워밍업 파라미터 (기본 4064 / 10)
#   WARMUP_N     워밍업 명령 수 (기본: RING_BYTES 에서 링 1.2 바퀴로 유도)
#   BUSY_WARN    busy_retries 가 이 값을 넘으면 경고 (기본 100)
#
# ── 측정 전에 반드시 확인할 것 ───────────────────────────────────────────
#
# **(1) 노드에 -profile 이 켜져 있어야 한다.** 꺼져 있으면 ApplyTimings 항들이
# 0/폴백으로 나오고 항등식이 성립하지 않는다. 첫 지점에서 LPersist/StorageIO/
# QuorumWait 가 전부 0 이면 이 스크립트가 경고한다.
#
# **(2) 노드에 -ae-batch 상한을 줄 것.** MaxAppendEntriesBatch 기본값이
# 1,000,000(사실상 무제한)이라 팔로워 하나가 뒤처지면 리더가 매 라운드 밀린
# 백로그를 통째로 재전송한다 (E2E_EXPERIMENT.md §7-5). **노드 기동 플래그라
# 클라이언트 쪽에서 줄 수 없다** -- 여기서 강제하지 못하는 항목이다.
#
# **(3) 링이 작으면 큰 페이로드에서 지연이 아니라 백프레셔를 재게 된다.**
# 엔트리 하나가 먹는 링 슬롯은 ceil((32 + size) / 512) 다
# (core/src/raft_ring_helpers.cpp:13). 런북 기본인 -ring-pages 4096 (16MiB,
# ring_slots=32767) 에서:
#
#     size     slots/entry   링 B/entry   한 랩 엔트리 수
#     1KiB         3           1,536        10,922
#     16KiB       33          16,896           993
#     128KiB     257         131,584           127      <- 10,000 개면 78 바퀴
#
# 랩마다 슬롯 GC(core/src/raft_commit.cpp:219-258)와 팔로워 match 진행을
# 기다려야 하고, 못 따라가면 apply 가 busy 를 돌려주고 5ms 뒤 재시도한다
# (core/src/raft_apply.cpp:110-127). **busy_retries 가 크게 나온 지점의 수치는
# 지연이 아니라 백프레셔다.** 128KiB 까지 제대로 보려면 노드를 더 큰 링으로
# 띄우고(예: -ring-pages 65536 = 256MiB) RING_BYTES 도 같이 올릴 것.
# -ring-pages 는 **세 노드가 같아야 한다.**
#
# **(4) 워밍업은 건너뛰지 말 것 (U11).** create_ring_file 이 매 기동마다 링
# 전체에 FALLOC_FL_ZERO_RANGE 를 걸지만(blockio/cached_fd.cpp:154-207), ext4 에서
# ZERO_RANGE 는 extent 를 written 으로 만들지 않는다. 그래서 각 extent 에 처음
# 닿는 쓰기가 파일시스템 저널을 통한 상태 변환으로 직렬화되고, 첫 랩의
# LPersist 가 그 비용까지 안고 부풀어 오른다. dd 로 미리 써두는 방법은 다음
# 기동 때 되돌려지므로 통하지 않는다 -- 링을 한 바퀴 돌리는 수밖에 없다.
# 링은 원형이므로 **한 번만** 하면 이후 모든 크기가 그 혜택을 본다.
#
# ── 한계 ────────────────────────────────────────────────────────────────
#
# **RDMA 프레임 상한이 1 MiB 다** (net/include/raft_rdma_transport.h:54).
# ClientApply 요청은 size × batch 바이트를 그대로 싣기 때문에 BATCH 를 올리면
# 큰 크기가 encode_frame 에서 예외로 죽는다 (128KiB 는 batch 7 이 상한).
# preflight 가 걸리는 크기를 목록에서 빼고 경고한다. AppendEntries 는 엔트리당
# 16B 메타만 보내므로(proto/rpcproto.proto:8-11) 이 제약과 무관하다.
#
# **RPC 호출 타임아웃이 사실상 요청당 상한이다.** RDMA 5s
# (net/src/raft_rdma_transport.cpp:46), TCP 2s (net/src/raft_tcp_transport.cpp:42-45).
# apply_internal 의 커밋 대기에는 자체 데드라인이 없어서
# (core/src/raft_apply.cpp:198-218) 느린 지점은 여기서 끊긴다. 지점이 실패해도
# 스윕은 계속하고 마지막에 실패 수를 보고한다 (종료코드 1).
#
# **Apply 당 타이밍 표본은 1건뿐이다** (ReplSink::first_sample). 한 번의 출력으로
# 분포를 논하지 말 것 -- REPS 를 올려 여러 번 모은다.
#
# **IB 장치가 있는 호스트에서 로컬 실행할 것.** -transport rdma 는 장치가 없으면
# 클라이언트가 거부한다 (apps/raft_client_main.cpp:175-179). 없으면 TRANSPORT=tcp
# 지만 2s 상한 때문에 큰 페이로드에서 더 잘 끊긴다.

set -uo pipefail

OUTDIR="${1:-$HOME/raftof_clean/csv/apply_latency_$(date +%Y%m%d-%H%M%S)}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$REPO/build}"

ADDRS="${ADDRS:-10.0.0.7:6001,10.0.0.5:6001,10.0.0.6:6001}"
SIZES="${SIZES:-1024 2048 4096 8192 16384 32768 65536 131072}"
N="${N:-10000}"
BATCH="${BATCH:-1}"
REPS="${REPS:-1}"
TRANSPORT="${TRANSPORT:-rdma}"
TIMEOUT_S="${TIMEOUT_S:-15}"

RING_BYTES="${RING_BYTES:-$((16 * 1024 * 1024))}"
WARMUP="${WARMUP:-1}"
WARMUP_SIZE="${WARMUP_SIZE:-4064}"
WARMUP_BATCH="${WARMUP_BATCH:-10}"
BUSY_WARN="${BUSY_WARN:-100}"

# 링 한 바퀴에 필요한 명령 수를 WARMUP_SIZE 에서 유도한다. 엔트리 하나가 먹는
# 링 바이트 = ceil((32 + size) / 512) * 512 다 (slots_for_entry). 1.2 배를
# 곱해 한 바퀴를 확실히 넘긴다.
warmup_slot_bytes=$(( ((32 + WARMUP_SIZE + 511) / 512) * 512 ))
WARMUP_N="${WARMUP_N:-$(( (RING_BYTES / warmup_slot_bytes) * 12 / 10 + 1 ))}"

CLIENT="$BIN/raft_client"

log()  { printf '\n=== %s\n' "$*"; }
ok()   { printf '  [ok]   %s\n' "$*"; }
warn() { printf '  [warn] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; }

# 1024 -> 1K, 131072 -> 128K. CSV 를 눈으로 볼 때와 축 라벨로 쓸 때를 위한 것.
size_label() {
    local b="$1"
    if [ "$b" -ge 1048576 ] && [ $((b % 1048576)) -eq 0 ]; then
        echo "$((b / 1048576))M"
    elif [ "$b" -ge 1024 ] && [ $((b % 1024)) -eq 0 ]; then
        echo "$((b / 1024))K"
    else
        echo "${b}B"
    fi
}

if [ ! -x "$CLIENT" ]; then
    fail "raft_client not found or not executable: $CLIENT"
    printf '       BIN=%s -- ./build.sh client 로 빌드하거나 BIN 을 지정할 것\n' "$BIN"
    exit 1
fi

mkdir -p "$OUTDIR" || exit 1

log "output -> $OUTDIR"
printf '  addrs     : %s\n  transport : %s\n' \
    "$ADDRS" "$TRANSPORT"
printf '  n         : %s   batch: %s   reps: %s\n' "$N" "$BATCH" "$REPS"
printf '  client    : %s\n' "$CLIENT"

# ---------------------------------------------------------------------------
# preflight A -- RDMA 1 MiB 프레임 상한에 걸리는 크기를 뺀다
#
# ClientApply 요청 본문은 size × batch 바이트에 protobuf 오버헤드(필드당 몇
# 바이트)와 프레임 헤더가 붙는다. 여유를 조금 두고 자른다. TCP 는 4바이트 길이
# 필드라 사실상 상한이 없으므로 검사하지 않는다.
# ---------------------------------------------------------------------------
RDMA_MAX=$((1024 * 1024))
SWEEP_SIZES=""
for size in $SIZES; do
    if [ "$TRANSPORT" = "rdma" ]; then
        need=$(( size * BATCH + BATCH * 8 + 64 ))
        if [ "$need" -gt "$RDMA_MAX" ]; then
            warn "size=$(size_label "$size") × batch=$BATCH = ${need}B > 1MiB RDMA frame -- skipping"
            warn "  (이 크기를 보려면 BATCH 를 낮추거나 TRANSPORT=tcp)"
            continue
        fi
    fi
    SWEEP_SIZES="$SWEEP_SIZES $size"
done
SWEEP_SIZES="${SWEEP_SIZES# }"

if [ -z "$SWEEP_SIZES" ]; then
    fail "no sizes left to sweep after the 1MiB frame check"
    exit 1
fi

# ---------------------------------------------------------------------------
# preflight B -- 클러스터가 실제로 떠 있는가
#
# -op commit-index 는 리더 탐색 없이 모든 주소에 물어보고, 못 붙은 주소에는
# "unreachable" 을 찍는다. 주소 수만큼 commit_index= 줄이 나와야 한다.
# ---------------------------------------------------------------------------
log "preflight: commit-index"
naddrs=$(printf '%s\n' "$ADDRS" | tr ',' '\n' | grep -c '[^[:space:]]')
"$CLIENT" -addrs "$ADDRS" -op commit-index -transport "$TRANSPORT" \
    > "$OUTDIR/preflight.log" 2>&1
rc=$?
sed 's/^/       /' "$OUTDIR/preflight.log"
nreach=$(grep -c 'commit_index=' "$OUTDIR/preflight.log")
if [ $rc -ne 0 ] || [ "$nreach" -lt "$naddrs" ]; then
    fail "cluster not ready: $nreach/$naddrs nodes answered (rc=$rc)"
    printf '       서버 기동은 E2E_EXPERIMENT.md §5. 주소가 IPoIB 인지도 확인할 것\n'
    exit 1
fi
ok "$nreach/$naddrs nodes answered"

# ---------------------------------------------------------------------------
# 워밍업 -- U11. 결과는 버린다
# ---------------------------------------------------------------------------
if [ "$WARMUP" != "0" ]; then
    log "warmup: $WARMUP_N × ${WARMUP_SIZE}B (ring=${RING_BYTES}B, ~1.2 laps) -- discarded"
    "$CLIENT" -addrs "$ADDRS" -op apply -n "$WARMUP_N" -size "$WARMUP_SIZE" \
        -batch "$WARMUP_BATCH" -timeout-s "$TIMEOUT_S" -transport "$TRANSPORT" \
        > "$OUTDIR/warmup.log" 2>&1
    if [ $? -ne 0 ]; then
        fail "warmup failed -- see $OUTDIR/warmup.log"
        tail -5 "$OUTDIR/warmup.log" | sed 's/^/       /'
        exit 1
    fi
    ok "$(grep '^applied ' "$OUTDIR/warmup.log" | tail -1)"
else
    warn "warmup skipped (WARMUP=0) -- 첫 지점의 LPersist 가 부풀 수 있다 (U11)"
fi

# ---------------------------------------------------------------------------
LATENCY_CSV="$OUTDIR/latency.csv"
echo "size_bytes,n_samples,term,mean_us,p50_us,p99_us,wall_ms,busy_retries" \
    > "$LATENCY_CSV"

FAILED=0
POINTS=0
PROFILE_CHECKED=0

# $1 rep   $2 size
run_point() {
    local rep="$1" size="$2"
    local slabel
    slabel=$(size_label "$size")
    local plog="$OUTDIR/apply-timed-r${rep}-${size}.log"

    "$CLIENT" -addrs "$ADDRS" -op apply-timed -n "$N" -size "$size" \
        -batch "$BATCH" -timeout-s "$TIMEOUT_S" -transport "$TRANSPORT" \
        > "$plog" 2>&1
    local rc=$?
    if [ $rc -ne 0 ]; then
        fail "size=$slabel rep=$rep -- client exited $rc, see $plog"
        tail -3 "$plog" | sed 's/^/       /'
        return 1
    fi

    # ---- 지점 메타. 전부 ^ 로 앵커한다 -- per-RPC 줄에도 "total=" 이 있다
    # 리더가 누구였는지는 CSV 에 넣지 않는다 -- 원본 로그의 "leader:" 줄에 남는다
    # (E2E_EXPERIMENT.md §7-5 는 그것을 기록하라고 한다)
    local n_samples wall_ms busy
    n_samples=$(sed -n 's/^apply-timed summary (n=\([0-9]*\) samples.*/\1/p' "$plog" | tail -1)
    wall_ms=$(sed -n 's/^applied .*, \([0-9.]*\) ms total.*/\1/p' "$plog" | tail -1)
    busy=$(sed -n 's/^applied .*busy_retries=\([0-9]*\).*/\1/p' "$plog" | tail -1)

    if [ -z "$n_samples" ]; then
        fail "size=$slabel rep=$rep -- no apply-timed summary block in $plog"
        return 1
    fi
    : "${wall_ms:=}"
    : "${busy:=0}"

    # ---- 요약 블록 11행.
    #
    # print_stats_brief 는 "mean=%9.1f" 이라 보통 '=' 뒤에 공백이 붙지만, 값이
    # 폭을 꽉 채우면(mean=1234567.8) 공백이 사라진다. 단순 필드 분할로는 두
    # 경우를 다 버틸 수 없어서 "= +" 를 먼저 지우고 나눈다.
    #
    # 닫는 줄 "(residual = Total - ...)" 은 in_sum 을 끄고, per-RPC 잡음 줄
    # "  batch=1 total=..." 은 mean= 이 없어 애초에 안 걸린다.
    # gawk 전용 기능은 쓰지 않는다 (mawk 에서도 같아야 한다).
    local terms_tmp="$OUTDIR/.terms.$$"
    awk '
        /^apply-timed summary/ { in_sum = 1; next }
        in_sum && /residual =/ { in_sum = 0; next }
        in_sum && /mean=/ {
            line = $0
            sub(/^[ \t]+/, "", line)
            gsub(/= +/, "=", line)
            nf = split(line, a, /[ \t]+/)
            term = a[1]; mean = ""; p50 = ""; p99 = ""
            for (i = 2; i <= nf; i++) {
                if (sub(/^mean=/, "", a[i]))     mean = a[i]
                else if (sub(/^p50=/, "", a[i])) p50  = a[i]
                else if (sub(/^p99=/, "", a[i])) p99  = a[i]
            }
            printf "%s,%s,%s,%s\n", term, mean, p50, p99
        }
    ' "$plog" > "$terms_tmp"

    local nterms
    nterms=$(grep -c . "$terms_tmp")
    if [ "$nterms" -eq 0 ]; then
        fail "size=$slabel rep=$rep -- summary block present but no terms parsed"
        rm -f "$terms_tmp"
        return 1
    fi

    local total_p50="" total_p99="" zero_terms=0
    local term mean p50 p99
    while IFS=, read -r term mean p50 p99; do
        [ -n "$term" ] || continue
        printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$size" "$n_samples" "$term" "$mean" "$p50" "$p99" \
            "$wall_ms" "$busy" >> "$LATENCY_CSV"
        case "$term" in
            Total) total_p50="$p50"; total_p99="$p99" ;;
            LPersist|StorageIO|QuorumWait)
                case "$mean" in
                    0|0.0|-0.0|0.00) zero_terms=$((zero_terms + 1)) ;;
                esac
                ;;
        esac
    done < "$terms_tmp"
    rm -f "$terms_tmp"

    printf '  size=%-5s rep=%-3s  Total p50=%-10s p99=%-10s  busy=%-6s n=%s\n' \
        "$slabel" "$rep" "${total_p50:--}" "${total_p99:--}" "$busy" "$n_samples"

    # -profile 이 꺼져 있으면 서버가 채우는 항들이 전부 0 으로 나온다.
    # 항등식을 논하기 전에 여기서 걸러준다. 첫 지점에서 한 번만 본다.
    if [ "$PROFILE_CHECKED" -eq 0 ]; then
        PROFILE_CHECKED=1
        if [ "$zero_terms" -eq 3 ]; then
            warn "LPersist/StorageIO/QuorumWait 가 모두 0 이다 -- 노드에 -profile 이"
            warn "  빠졌거나 샘플 폴백 경로다. 항등식이 성립하지 않는다"
        fi
    fi
    if [ "$busy" -gt "$BUSY_WARN" ]; then
        warn "busy_retries=$busy > $BUSY_WARN -- 이 지점은 지연이 아니라 링"
        warn "  백프레셔를 재고 있다. 노드의 -ring-pages 를 키울 것 (머리 주석 (3))"
    fi
    return 0
}

log "sweep: sizes =$(for s in $SWEEP_SIZES; do printf ' %s' "$(size_label "$s")"; done)"
for rep in $(seq 1 "$REPS"); do
    for size in $SWEEP_SIZES; do
        POINTS=$((POINTS + 1))
        run_point "$rep" "$size" || FAILED=$((FAILED + 1))
    done
done

# ---------------------------------------------------------------------------
log "done"
printf '  points   : %d run, %d failed\n' "$POINTS" "$FAILED"
printf '  csv      : %s (%d rows + header)\n' \
    "$LATENCY_CSV" "$(($(grep -c . "$LATENCY_CSV") - 1))"
printf '  raw logs : %s/apply-timed-*.log\n' "$OUTDIR"

if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
exit 0
