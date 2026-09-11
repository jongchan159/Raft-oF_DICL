#!/usr/bin/env bash
#
# blkcopy_scaling.sh -- blockcopy 처리량이 워커 수와 배치 크기에 따라
#                       어떻게 확장되고 어디서 포화하는지 잰다
#
# 묻는 것: **스토리지 노드를 -copy-workers 몇으로 운영해야 하는가**, 그리고
# 실제 Raft 경로가 만드는 배치 크기에서 그 값이 의미가 있는가.
#
# 축 3개를 훑는다:
#   W = 서버의 -copy-workers   (서버 시작 플래그 -> 지점마다 서버 재기동)
#   B = 벤치의 -batch          (RPC 당 청크 수)
#   chunk = 청크 바이트
#
# 구조적으로 알고 시작하는 것 (storage/raft_blockcopy_server.cpp):
#   - `w = min(copy_workers_, count)` (:208) -- **배치가 병렬도의 상한이다.**
#     따라서 W > B 구간은 W = B 와 같아야 하고, 안 그러면 계측이 틀린 것이다
#   - 워커 스레드를 **RPC 마다 새로 생성**한다 (:244). 작은 청크 + 큰 W 에서
#     이게 지배하면 처리량이 되레 떨어질 수 있다
#   - copy_nanos 는 **워커별 시간의 합**이다 (:284). 처리량 분모로 쓰면 안 된다.
#     처리량은 벤치가 wall 기준으로 낸다
#   - 클라이언트는 커넥션당 완전 직렬화라 in-flight RPC 가 1개다. 실제 Raft
#     경로도 팔로워 핸들러들이 클라이언트 하나를 공유하므로 이 제약은 충실하다
#
# **하드코딩된 arm 표가 없다.** 호스트와 경로를 전부 환경변수로 받는다 --
# 예전 blkcopy_latency.sh 가 arm 표를 코드에 박아두어 실제 배치와 어긋난 채
# 낡아버린 전철을 밟지 않기 위해서다. 스토리지 노드를 옮겨도 이 파일은
# 수정 대상이 아니다.
#
# 서버와 벤치는 **항상 같은 호스트(loopback)** 에서 돈다. 그래야 클라이언트↔
# 스토리지 RPC 구간이 모든 지점에서 같아져 비교가 오염되지 않는다.
#
# 사용법:
#   ARM_LABEL=A-local ./scripts/blkcopy_scaling.sh [OUTDIR]
#
# 환경변수:
#   STORAGE_HOST  서버+벤치가 돌 호스트 (기본 eternitystorage, "localhost" 가능)
#   SRC_FILE      src 로 쓸 파일. 읽기만 한다
#   DST_FILE      dst 로 쓸 파일. **덮어쓴다**
#   ARM_LABEL     결과에 붙는 arm 이름 (예: A-local, B-rdma)
#   WORKERS       W 목록 (기본 "1 2 4 8 16 32")
#   BATCHES       B 목록 (기본 "1 4 16 64")
#   CHUNKS        청크 바이트 목록 (기본 4Ki 64Ki 1Mi)
#   TARGET_BYTES  지점당 옮길 바이트 (기본 2GiB). 반복 횟수는 여기서 파생된다
#   MIN_ITERS / MAX_ITERS   반복 횟수 clamp (기본 30 / 2000)
#   SRC_OFF / DST_OFF / REGION   측정 구간 (기본 4GiB / 4GiB / 32GiB)
#   PORT          스토리지 서버 포트 (기본 5060 -- 라이브 클러스터의 5050 과 분리)
#   BIN           바이너리 디렉터리 (기본 <repo>/build)
#   RAW_CONTROL   1 이면 raw 블록 대조점을 추가로 돈다 (아래)
#   RAW_SRC / RAW_DST   그 대조점의 블록 디바이스
#                       (기본 /dev/nvme12n1, /dev/nvme13n1)
#
# ⚠ DST_FILE 의 [DST_OFF, DST_OFF+REGION) 구간을 덮어쓴다.
#
# ── ext4 파일 기반이라는 것 ──────────────────────────────────────────────
# -devices 에 블록 디바이스가 아니라 **파일**을 준다. open_device 가
# O_RDWR|O_DIRECT 로 열므로 파일에도 그대로 동작한다. 비파괴이고 disk 그룹도
# 필요 없지만, production 은 블록 디바이스 + FIEMAP 물리 오프셋이므로
# **완전히 같은 경로는 아니다.** 결과에 file-backed 를 병기할 것.
#
# 파일은 반드시 **실제 랜덤 데이터로 미리 채워야** 한다. 이유가 둘이다:
#   (1) fallocate -l 만 하면 extent 가 unwritten 으로 남고, 거기 첫 쓰기가
#       extent 상태 변환을 파일시스템 저널로 **직렬화**시켜 W 를 올려도
#       처리량이 안 오르는 가짜 결과가 나온다
#   (2) **미기록 LBA 의 pread 는 FTL 이 즉답해서 비현실적으로 빠르다.**
#       0 으로만 채우면 (1)은 풀려도 (2)가 남는다 -- SSD 가 0 블록을
#       NAND 를 거치지 않고 돌려줄 수 있다
# 그래서 0 이 아니라 랜덤 시드로 채운다 (urandom 직접 쓰기는 느리므로 1GiB
# 시드를 만들어 반복 복사):
#     dd if=/dev/urandom of=/tmp/seed bs=1M count=1024 status=none
#     for i in $(seq 0 35); do
#         dd if=/tmp/seed of=<file> bs=1M seek=$((i*1024)) conv=notrunc \
#            oflag=direct status=none
#     done
#
# ⚠ `fallocate -z`(ZERO_RANGE) 로는 해결되지 않는다. ext4 에서 ZERO_RANGE 는
#   범위를 **unwritten 으로 두는 것**이 가장 싼 zeroing 구현이라 extent 를
#   written 으로 뒤집지 않는다 (커널 4.15 와 6.8 양쪽에서 실측 확인,
#   2026-09-10). blockio/cached_fd.h:29-35 와 DECISIONS.md D9 는 ZERO_RANGE 가
#   written 으로 만든다고 서술하는데 그 전제가 성립하지 않는다 -- DECISIONS.md
#   U11 에 기록했다. 그래서 여기서는 dd 로 실제로 쓴다.
#
# preflight 가 unwritten extent 를 검사해 남아 있으면 거부한다.
#
# RAW_CONTROL 은 그래도 W 스케일링이 안 나올 때만 쓰는 **진단**이다. 해결책이
# 아니라, "ext4 탓인가 blockcopy 구조 탓인가" 를 가르는 대조군이다. 실험용
# dst 가 아니라 별도의 빈 블록 디바이스에 돌린다 (마운트된 ext4 위에 raw 로
# 쓰면 그 파일시스템이 깨진다). 이 경로만 블록 디바이스 접근 권한을 요구하는데,
# `usermod -aG disk` 는 쓰지 말 것 -- 이 호스트에는 Lustre MDT/OST 가 있어서
# disk 그룹은 그것까지 전부 연다. 해당 wwid 만 지정한 udev 규칙을 쓴다
# (BLKCOPY_EXPERIMENT.md 참조).

set -uo pipefail

OUTDIR="${1:-$HOME/blkcopy_scale_$(date +%Y%m%d-%H%M%S)}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${BIN:-$REPO/build}"

STORAGE_HOST="${STORAGE_HOST:-eternitystorage}"
SRC_FILE="${SRC_FILE:-/mnt/bc-src/blkcopy/src}"
DST_FILE="${DST_FILE:-/mnt/bc-dst/blkcopy/dst}"
ARM_LABEL="${ARM_LABEL:-unnamed}"

WORKERS="${WORKERS:-1 2 4 8 16 32}"
BATCHES="${BATCHES:-1 4 16 64}"
CHUNKS="${CHUNKS:-4096 65536 1048576}"

TARGET_BYTES="${TARGET_BYTES:-$((2 * 1024 * 1024 * 1024))}"
MIN_ITERS="${MIN_ITERS:-30}"
MAX_ITERS="${MAX_ITERS:-2000}"
SRC_OFF="${SRC_OFF:-$((4 * 1024 * 1024 * 1024))}"
DST_OFF="${DST_OFF:-$((4 * 1024 * 1024 * 1024))}"
REGION="${REGION:-$((32 * 1024 * 1024 * 1024))}"
PORT="${PORT:-5060}"

RAW_CONTROL="${RAW_CONTROL:-0}"
RAW_SRC="${RAW_SRC:-/dev/nvme12n1}"
RAW_DST="${RAW_DST:-/dev/nvme13n1}"

SSH="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=8"

log()  { printf '\n=== %s\n' "$*"; }
ok()   { printf '  [ok]   %s\n' "$*"; }
warn() { printf '  [warn] %s\n' "$*"; }
fail() { printf '  [FAIL] %s\n' "$*"; }

# 원격/로컬을 한 인터페이스로. host == "localhost" 면 그냥 실행한다.
run_on() {
    local host="$1"; shift
    if [[ "$host" == "localhost" || "$host" == "$(hostname)" ]]; then
        bash -c "$*"
    else
        # shellcheck disable=SC2086
        $SSH "$host" "$*"
    fi
}

mkdir -p "$OUTDIR"
SUMMARY_CSV="$OUTDIR/summary.csv"
echo "arm,workers,batch,chunk,thr_mibs,thr_p50_mibs,eff_workers,wall_p50_us,wall_p99_us,n" \
    > "$SUMMARY_CSV"

# ---------------------------------------------------------------------------
# preflight -- 하나라도 걸리면 아예 시작하지 않는다
# ---------------------------------------------------------------------------
preflight() {
    local need_src=$((SRC_OFF + REGION))
    local need_dst=$((DST_OFF + REGION))

    log "preflight on $STORAGE_HOST"

    # (1) 라이브 raft 가 돌고 있으면 차단한다. 같은 SSD 를 쓰므로 측정이
    #     오염될 뿐 아니라 그쪽 데이터를 건드릴 수 있다.
    # pgrep -f 는 우리가 방금 보낸 명령줄 자신을 매칭할 수 있다. -x 로
    # 프로세스 **이름** 전체 일치를 보면 그 자기참조가 원천적으로 없다.
    local live
    live=$(run_on "$STORAGE_HOST" "pgrep -c -x 'raft_node|raft_blockcopy_server' || true")
    live="${live//[^0-9]/}"
    if [[ -n "$live" && "$live" -gt 0 ]]; then
        fail "live raft processes on $STORAGE_HOST ($live). stop the cluster first."
        run_on "$STORAGE_HOST" "pgrep -a -x 'raft_node|raft_blockcopy_server' || true" | sed 's/^/       /'
        return 1
    fi
    ok "no live raft processes"

    # (2) 파일 존재와 크기. 예전 preflight 는 파일이면 그냥 통과시켜서,
    #     작은 파일이면 벤치가 short read 로 죽었다.
    local out
    out=$(run_on "$STORAGE_HOST" "
        for spec in '$SRC_FILE:$need_src' '$DST_FILE:$need_dst'; do
            f=\${spec%:*}; need=\${spec##*:}
            if [ ! -f \"\$f\" ]; then echo \"missing: \$f\"; exit 1; fi
            sz=\$(stat -c %s \"\$f\")
            if [ \"\$sz\" -lt \"\$need\" ]; then
                echo \"too small: \$f is \$sz bytes, need \$need\"; exit 1
            fi
        done
        exit 0
    " 2>&1)
    if [[ -n "$out" ]]; then
        fail "$out"
        return 1
    fi
    ok "src/dst files present and large enough"

    # (3) unwritten extent. 남아 있으면 첫 쓰기가 저널로 직렬화되어
    #     병렬도 측정이 조용히 망가진다 -- 파일 머리 주석 참조.
    for f in "$SRC_FILE" "$DST_FILE"; do
        local nu frag
        # grep -c 는 0건일 때 종료코드 1이다. || true 가 없으면 정상(=0건)이
        # 실패로 읽힌다.
        nu=$(run_on "$STORAGE_HOST" "filefrag -v '$f' 2>/dev/null | grep -c unwritten || true")
        nu="${nu//[^0-9]/}"
        frag=$(run_on "$STORAGE_HOST" "filefrag '$f' 2>/dev/null | sed 's/.*: //' || true")
        if [[ -z "$nu" ]]; then
            fail "$f: filefrag failed (is filefrag installed on $STORAGE_HOST?)"
            return 1
        fi
        if [[ "$nu" != "0" ]]; then
            fail "$f has $nu unwritten extents -- first writes would serialize through"
            fail "  the ext4 journal and hide the parallelism you are trying to measure."
            fail "  Fix by prefilling with RANDOM data (fallocate -z does NOT clear these"
            fail "  on ext4, and all-zero fill leaves reads unrealistically fast):"
            fail "    dd if=/dev/urandom of=/tmp/seed bs=1M count=1024 status=none"
            fail "    for i in \$(seq 0 \$((\$(stat -c %s '$f')/1073741824))); do"
            fail "      dd if=/tmp/seed of='$f' bs=1M seek=\$((i*1024)) conv=notrunc oflag=direct status=none"
            fail "    done"
            return 1
        fi
        ok "$f: no unwritten extents, $frag"
        echo "$f: unwritten=0 $frag" >> "$OUTDIR/filefrag.txt"
    done

    # (4) src 가 전부 0 이면 pread 가 FTL 즉답으로 비현실적으로 빨라진다.
    #     측정 구간에서 표본 3개를 떠서 전부 0 이면 거부한다.
    local nz
    nz=$(run_on "$STORAGE_HOST" "
        tot=0
        for k in 0 1 2; do
            off=\$(( $SRC_OFF + k * ($REGION / 4) ))
            n=\$(dd if='$SRC_FILE' bs=4096 count=1 skip=\$((off / 4096)) \
                    iflag=direct status=none 2>/dev/null | tr -d '\\000' | wc -c)
            tot=\$((tot + n))
        done
        echo \$tot
    " || echo "")
    nz="${nz//[^0-9]/}"
    if [[ -n "$nz" && "$nz" == "0" ]]; then
        fail "$SRC_FILE reads as all zeros in the measured region."
        fail "  Unwritten/zero LBAs are answered by the SSD FTL without touching NAND,"
        fail "  so reads would be unrealistically fast. Prefill with random data first"
        fail "  (see the dd/seed recipe in this script's header)."
        return 1
    fi
    ok "src region carries non-zero data (${nz:-?} non-zero bytes in 3x4KiB samples)"
    return 0
}

# ---------------------------------------------------------------------------
# 지점 하나: 서버 기동 -> 벤치 -> 서버 정리
#   $1 W   $2 B   $3 chunk   $4 src   $5 dst   $6 arm suffix
# ---------------------------------------------------------------------------
stop_server() {
    # 패턴의 첫 글자를 [r] 로 감싸는 것이 **의도적**이다. 이 pkill 을 실행하는
    # 셸(로컬이면 bash -c, 원격이면 ssh 가 띄운 셸) 자신의 명령줄에 패턴 문자열이
    # 그대로 들어가므로, 브래킷이 없으면 pkill 이 자기 부모 셸을 죽인다.
    # (실제로 그렇게 되어 모든 지점이 "server did not start" 로 실패했다.)
    # [r]aft... 라는 정규식은 "raft..." 에는 맞고 "[r]aft..." 라는 리터럴에는
    # 맞지 않으므로 자기참조가 끊긴다.
    run_on "$STORAGE_HOST" \
        "pkill -f \"[r]aft_blockcopy_server -addr 0.0.0.0:$PORT\" >/dev/null 2>&1; exit 0" || true
}

run_point() {
    local w="$1" b="$2" chunk="$3" src="$4" dst="$5" suffix="${6:-}"
    local arm="$ARM_LABEL$suffix"
    local tag="w${w}-b${b}-c${chunk}${suffix}"
    local slog="$OUTDIR/server-$tag.log"
    local blog="$OUTDIR/bench-$tag.log"
    local csv="$OUTDIR/raw-$tag.csv"

    stop_server
    # 지점마다 서버를 새로 띄운다: W 가 시작 플래그이고, AlignedBufPool 상태를
    # 지점 간에 끌고 가지 않기 위해서다.
    #
    # stdbuf -oL 이 **필수**다. 서버는 std::printf 로 기동 배너를 찍고 바로
    # accept 루프로 들어가는데, stdout 이 파일이면 블록 버퍼링이라 배너가
    # 버퍼에 갇혀 로그가 빈 채로 남는다. 그러면 아래 준비 확인이 "서버가 안
    # 떴다"로 오판한다 (실제로는 잘 돌고 있다).
    run_on "$STORAGE_HOST" "setsid stdbuf -oL -eL '$BIN/raft_blockcopy_server' \
        -addr 0.0.0.0:$PORT -devices '$src,$dst' -copy-workers $w \
        > '$slog' 2>&1 < /dev/null &" || true

    # 서버가 실제로 받은 W 를 확인한다. -workers 는 벤치에게는 라벨일 뿐이라
    # 여기서 대조하지 않으면 어긋나도 모른다. 고정 sleep 대신 폴링한다 --
    # 큰 -devices 목록이나 느린 디스크에서 1초로는 모자랄 수 있다.
    local got="" tries=0
    while [[ $tries -lt 50 ]]; do
        got=$(run_on "$STORAGE_HOST" \
            "grep -o 'copy-workers *: *[0-9]*' '$slog' 2>/dev/null | grep -o '[0-9]*$' || true")
        got="${got//[^0-9]/}"
        [[ -n "$got" ]] && break
        sleep 0.2
        tries=$((tries + 1))
    done
    if [[ -z "$got" ]]; then
        fail "server did not start within 10s (arm=$arm $tag); log:"
        run_on "$STORAGE_HOST" "tail -5 '$slog' 2>/dev/null || true" | sed 's/^/       /'
        stop_server
        return 1
    fi
    if [[ "$got" != "$w" ]]; then
        fail "server reports copy-workers=$got but we asked for $w"
        stop_server
        return 1
    fi

    local out
    out=$(run_on "$STORAGE_HOST" "'$BIN/raft_blkcopy_scale' \
        -storage 127.0.0.1:$PORT -src-dev 0 -dst-dev 1 \
        -chunk $chunk -batch $b -workers $w \
        -target-bytes $TARGET_BYTES -min-iters $MIN_ITERS -max-iters $MAX_ITERS \
        -src-off $SRC_OFF -dst-off $DST_OFF -region $REGION \
        -csv '$csv' -label '$tag' -arm '$arm' -yes-destroy-dst 2>&1")
    local rc=$?
    printf '%s\n' "$out" > "$blog"
    stop_server

    if [[ $rc -ne 0 ]]; then
        fail "bench failed (arm=$arm $tag) -- see $blog"
        printf '%s\n' "$out" | tail -5 | sed 's/^/       /'
        return 1
    fi

    local s
    s=$(printf '%s\n' "$out" | grep '^SUMMARY ' | tail -1)
    if [[ -z "$s" ]]; then
        fail "no SUMMARY line (arm=$arm $tag) -- see $blog"
        return 1
    fi

    # SUMMARY arm=.. workers=.. batch=.. chunk=.. thr_mibs=.. thr_p50_mibs=..
    #         eff_workers=.. wall_p50_us=.. wall_p99_us=.. n=..
    local thr thr50 eff p50 p99 n
    thr=$(sed -n 's/.*thr_mibs=\([^ ]*\).*/\1/p'      <<<"$s")
    thr50=$(sed -n 's/.*thr_p50_mibs=\([^ ]*\).*/\1/p' <<<"$s")
    eff=$(sed -n 's/.*eff_workers=\([^ ]*\).*/\1/p'    <<<"$s")
    p50=$(sed -n 's/.*wall_p50_us=\([^ ]*\).*/\1/p'    <<<"$s")
    p99=$(sed -n 's/.*wall_p99_us=\([^ ]*\).*/\1/p'    <<<"$s")
    n=$(sed -n 's/.*n=\([^ ]*\).*/\1/p'                <<<"$s")

    echo "$arm,$w,$b,$chunk,$thr,$thr50,$eff,$p50,$p99,$n" >> "$SUMMARY_CSV"
    printf '  W=%-3s B=%-3s chunk=%-8s  %9s MiB/s   eff_workers=%-6s p50=%sus\n' \
        "$w" "$b" "$chunk" "$thr" "$eff" "$p50"
    return 0
}

# ---------------------------------------------------------------------------
log "output -> $OUTDIR"
printf '  arm      : %s\n  host     : %s\n  src      : %s\n  dst      : %s\n' \
    "$ARM_LABEL" "$STORAGE_HOST" "$SRC_FILE" "$DST_FILE"
printf '  workers  : %s\n  batches  : %s\n  chunks   : %s\n  target   : %s MiB/point\n' \
    "$WORKERS" "$BATCHES" "$CHUNKS" "$((TARGET_BYTES / 1024 / 1024))"
printf '  NOTE     : file-backed on ext4 (not the raw block + FIEMAP path production uses)\n'

if ! preflight; then
    fail "preflight failed -- nothing was run"
    exit 1
fi

FAILED=0
for chunk in $CHUNKS; do
    log "chunk = $chunk bytes"
    for w in $WORKERS; do
        for b in $BATCHES; do
            run_point "$w" "$b" "$chunk" "$SRC_FILE" "$DST_FILE" || FAILED=$((FAILED + 1))
        done
    done
done

# ---------------------------------------------------------------------------
# raw 대조점 -- 해결책이 아니라 진단이다. 파일 기반에서 W 스케일링이 안 나올
# 때, 그게 ext4 탓인지 blockcopy 구조 탓인지 가른다.
# ---------------------------------------------------------------------------
if [[ "$RAW_CONTROL" == "1" ]]; then
    log "raw control point ($RAW_SRC -> $RAW_DST)"
    warn "this needs the 'disk' group and OVERWRITES $RAW_DST"
    okperm=$(run_on "$STORAGE_HOST" \
        "dd if='$RAW_SRC' of=/dev/null bs=4096 count=1 iflag=direct status=none 2>/dev/null && echo yes || echo no")
    if [[ "$okperm" != "yes" ]]; then
        warn "cannot open $RAW_SRC -- skipping the raw control point."
        warn "  Grant access with a udev rule scoped to those two devices, NOT with"
        warn "  'usermod -aG disk': this host has Lustre MDT/OST disks and the disk"
        warn "  group would open all of them. See BLKCOPY_EXPERIMENT.md."
    else
        for w in 1 8; do
            run_point "$w" 16 1048576 "$RAW_SRC" "$RAW_DST" "-raw" || FAILED=$((FAILED + 1))
        done
    fi
fi

# ---------------------------------------------------------------------------
# W x B 행렬 요약 (chunk 별 한 장)
# ---------------------------------------------------------------------------
log "summary -- throughput MiB/s (rows = W, cols = B)"

# summary.csv 에서 값을 꺼내 행렬로 찍는다. gawk 전용 기능(asorti)을 쓰지
# 않는다 -- 이 스크립트를 mawk 만 있는 호스트에서 돌려도 같은 출력이 나와야
# 한다. 지점 수가 100 아래라 grep 반복으로 충분하다.
cell() {   # $1 chunk  $2 W  $3 B  $4 열이름(thr|eff)
    local col=5
    [[ "$4" == "eff" ]] && col=7
    awk -F, -v c="$1" -v w="$2" -v b="$3" -v col="$col" \
        'NR>1 && $4==c && $2==w && $3==b { print $col; found=1 }
         END { if (!found) print "-" }' "$SUMMARY_CSV" | head -1
}

for chunk in $CHUNKS; do
    printf '\n  chunk = %s bytes   (MiB/s, 괄호는 eff_workers)\n' "$chunk"
    printf '  %6s' "W\\B"
    for b in $BATCHES; do printf ' %16s' "B=$b"; done
    printf '\n'
    for w in $WORKERS; do
        printf '  %6s' "$w"
        for b in $BATCHES; do
            printf ' %10s(%4s)' "$(cell "$chunk" "$w" "$b" thr)" "$(cell "$chunk" "$w" "$b" eff)"
        done
        printf '\n'
    done
done

log "how to read this"
cat <<'NOTE'
  - W > B 구간은 W = B 와 같아야 한다 (w = min(copy_workers, batch)).
    다르면 W 가 서버에 안 먹은 것이다 -- server-*.log 의 copy-workers 를 볼 것.
  - eff_workers(= copy_ns/wall_ns)에서 min(W,B)는 **상한**이지 목표치가 아니다.
    wall 에는 RPC 코덱/전송이 함께 들어 있어 W=1 에서 1 미만이 나오는 것은
    정상이다. 판단은 상대적으로 한다 -- B 를 키울 때 이 값이 min(W,B)를 따라
    올라가면 병렬화가 먹는 것이고, min(W,B)를 올려도 제자리면 안 먹는 것이다.
    후자이고 파일 기반이면 ext4 직렬화를 먼저 의심할 것
    -> RAW_CONTROL=1 로 원인을 가른다.
  - 처리량이 매체의 물리 상한을 넘으면 계측 오류다 (오프셋 겹침이나 캐시 경유).
  - NVMe-oF/RDMA arm 에서 1.25 GB/s 근처 포화는 링크 한계다 (IB 4X SDR 10 Gb/s).
  - 수치는 file-backed on ext4 다. production 의 raw 블록 + FIEMAP 경로와
    같지 않으므로 보고할 때 병기할 것.
NOTE

printf '\n  summary csv : %s\n  raw csv/logs: %s\n' "$SUMMARY_CSV" "$OUTDIR"
if [[ $FAILED -gt 0 ]]; then
    fail "$FAILED point(s) failed"
    exit 1
fi
ok "all points completed"
exit 0
