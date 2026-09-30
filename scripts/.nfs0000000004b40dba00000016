#!/usr/bin/env bash
#
# cstate_probe.sh -- 원격/로컬 호스트의 CPU 유휴 상태(cpuidle) 카운터를 읽어
#                    CSV 로 남긴다
#
# 묻는 것: **AENet 의 ~130µs 고정 스톨이 코어 절전(C6) 복귀 지연인가.**
#
# apply_timed 스윕에서 AENet 평균이 페이로드에 따라 62.8 -> 215.7µs 로 커지는데,
# AE RPC 메시지는 엔트리당 16B 로 사실상 고정이다 (proto/rpcproto.proto:8-11).
# 원본 로그 10000건을 분해해 보면 AENet 분포는 **이봉**이고 두 봉우리의 위치는
# 거의 안 움직인다 -- 바뀌는 것은 혼합비뿐이다:
#
#     size   AENet평균   p(느린봉)   빠른봉   느린봉    AE_RT
#       1K       62.8       6.5%      55.4    168.4    329µs
#      16K       83.6      19.1%      67.3    152.6    522µs
#      32K      169.8      75.6%      73.9    200.8    676µs   <- 여기서 급변
#     128K      215.7      99.5%      90.1    216.3    958µs
#
# 즉 ~130µs 짜리 고정 스톨이 걸릴 **확률**만 커진다. 가설은 그 정체가 C6 복귀
# 지연이라는 것이다 -- net/src/raft_rdma_transport.cpp:174-209 의
# wait_completion 이 스핀 없이 곧바로 ::poll() 로 잠들기 때문에 왕복당 인터럽트
# 웨이크업을 4회 문다. AE_RT 가 C6 target residency 를 넘어서면 커널이 깊은
# 절전을 고르기 시작하고, 그때부터 매번 exit latency 를 지불한다.
#
# 이 스크립트는 **그 가설을 확인/기각하기 위한 관찰 도구**다. 아무것도 바꾸지
# 않고 sudo 도 쓰지 않는다 (cpuidle sysfs 는 world-readable).
#
# 사용법:
#   ./scripts/cstate_probe.sh info  <host>
#   ./scripts/cstate_probe.sh snap  <host>
#   ./scripts/cstate_probe.sh watch <host> <outcsv> [interval_s]
#
# ── 왜 별도 스크립트인가 ────────────────────────────────────────────────
#
# apply_latency_sweep.sh 는 "raft_client 를 돌려 stdout 을 파싱하는 것이 전부라
# sudo 도 ssh 도 필요 없다"는 것이 계약이다 (scripts/README.md:202). 원격 수집을
# 그 안에 넣으면 그 성질이 깨지므로 밖에 둔다. 스윕과 **병행 실행**하고 나중에
# 시각으로 맞춘다.
#
# ── 전형적인 사용 흐름 ──────────────────────────────────────────────────
#
#   # 1) 유휴 단계 구성 확인 (호스트마다 다를 수 있다)
#   for h in eternity4 eternity5 eternity6; do echo "== $h"; ./scripts/cstate_probe.sh info $h; done
#
#   # 2) 리더 확인 -- 스윕은 리더를 파싱하지 않는다 (apply_latency_sweep.sh:244)
#   #    클라이언트가 첫 줄에 찍는다 (apps/raft_client_main.cpp:246-249)
#   SIZES=1024 N=200 WARMUP=0 ./scripts/apply_latency_sweep.sh /tmp/leaderprobe
#   head -1 /tmp/leaderprobe/apply-timed-r1-1024.log     # -> leader: 10.0.0.5:6001
#   #    주소->호스트: 10.0.0.N:6001 -> eternityN  (N = 노드 id)
#
#   # 3) 프로브를 띄운 채 스윕
#   OUT=$HOME/raftof_clean/csv/cstate_$(date +%Y%m%d-%H%M%S); mkdir -p $OUT
#   ./scripts/cstate_probe.sh watch eternity5 $OUT/cstate.csv 1 &
#   PROBE=$!
#   ./scripts/apply_latency_sweep.sh $OUT
#   kill $PROBE
#
#   # 4) 리더가 중간에 안 바뀌었는지 확인 -- 두 줄 이상이면 그 런은 버린다
#   head -qn1 $OUT/apply-timed-r1-*.log | sort -u
#
# ── 4단계: 지점별로 잘라 붙이기 ─────────────────────────────────────────
#
# 스윕은 지점을 순차 실행하므로 각 지점 로그의 mtime 이 그 지점의 **종료 시각**
# 이다. 이전 지점의 mtime 을 시작으로 삼아 버킷팅한다. 아래를 그대로 붙여
# 쓰면 크기별 "C6 진입/명령" 과 "C6 체류율" 이 나온다:
#
#   python3 - "$OUT" <<'PY'
#   import csv, os, sys, glob, re
#   out = sys.argv[1]
#   # 지점 경계: 워밍업 로그 mtime 을 첫 시작으로, 각 점 로그 mtime 을 끝으로
#   pts = sorted(((int(re.search(r'-(\d+)\.log$', p).group(1)), os.path.getmtime(p))
#                 for p in glob.glob(f'{out}/apply-timed-r1-*.log')), key=lambda x: x[1])
#   w = f'{out}/warmup.log'
#   prev = os.path.getmtime(w) if os.path.exists(w) else pts[0][1] - 60
#   # 프로브 표본
#   rows = list(csv.DictReader(open(f'{out}/cstate.csv')))
#   print(f"{'size':>7} {'state':>6} {'entries/cmd':>12} {'residency%':>11} {'MHz':>7}")
#   for size, end in pts:
#       seg = [r for r in rows if prev <= float(r['epoch_s']) <= end]
#       prev = end
#       if len(seg) < 2: continue
#       ncpu = int(seg[0]['ncpu']); span = float(seg[-1]['epoch_s']) - float(seg[0]['epoch_s'])
#       mhz = sum(float(r['avg_mhz']) for r in seg) / len(seg)
#       for st in sorted({r['state'] for r in seg}):
#           a = [r for r in seg if r['state'] == st]
#           du = int(a[-1]['usage']) - int(a[0]['usage'])
#           dt = int(a[-1]['time_us']) - int(a[0]['time_us'])
#           print(f"{size:>7} {st:>6} {du/10000:>12.2f} "
#                 f"{dt/(span*1e6*ncpu)*100 if span else 0:>10.1f}% {mhz:>7.0f}")
#   PY
#
# mtime 정렬이 미덥지 않으면 더 견고한 대안: SIZES="1024" 와 SIZES="131072" 를
# 따로 돌리고 각각을 프로브로 감싼다. 판정에는 양 끝 두 지점이면 충분하다
# (p 가 6.5% <-> 99.5% 로 갈리는 구간이다).
#
# ── 판정 기준 ───────────────────────────────────────────────────────────
#
#   확인 : C6 진입/명령이 크기에 따라 뚜렷이 늘고, 16K->32K 에서 급변한다
#          (AENet 의 p 가 19% -> 76% 로 뛰는 지점과 일치) -> 스핀 도입으로
#   기각 : C6 진입이 평평하거나 거의 0 -> 다른 용의자로 (HCA 인터럽트
#          모더레이션 `ethtool -c`, 스케줄러 `/proc/schedstat`)
#
# 빠른 봉우리도 55 -> 90µs 로 1.6배 올랐는데 그건 주파수 쪽일 가능성이 있어서
# avg_mhz 를 같이 남긴다.

set -uo pipefail

SSH="ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=8"

log()  { printf '\n=== %s\n' "$*"; }
warn() { printf '  [warn] %s\n' "$*" >&2; }
fail() { printf '  [FAIL] %s\n' "$*" >&2; }

usage() {
    sed -n '/^# 사용법:/,/^#$/p' "$0" | sed 's/^# \{0,1\}//'
    exit 1
}

# 원격/로컬을 한 인터페이스로 (scripts/blkcopy_scaling.sh:128-136 과 같은 관용구).
# host 가 localhost 이거나 이 호스트면 ssh 를 타지 않는다.
run_on() {
    local host="$1"; shift
    if [[ "$host" == "localhost" || "$host" == "$(hostname)" ]]; then
        bash -c "$*"
    else
        # shellcheck disable=SC2086
        $SSH "$host" "$*"
    fi
}

# ---------------------------------------------------------------------------
# 원격에서 한 번의 셸 호출로 전부 읽는다 -- state 당 ssh 왕복을 돌면 40코어 x
# 4상태 = 160 회가 되어 표본 하나 뜨는 데 수십 초 걸린다.
#
# 출력 (공백 구분, 한 줄에 하나):
#   N <statedir> <name>        상태 이름          (cpu0 만)
#   L <statedir> <us>          exit latency       (cpu0 만)
#   R <statedir> <us>          target residency   (cpu0 만)
#   D <statedir> <0|1>         disable 플래그     (cpu0 만)
#   U <statedir> <sum>         진입 횟수 누적, 전 CPU 합
#   T <statedir> <sum_us>      체류 시간 누적(µs), 전 CPU 합
#   C <ncpu>                   CPU 수
#   F <avg_mhz>                평균 주파수
#
# statedir 는 "state3" 같은 디렉터리 이름이다. **번호를 의미로 쓰지 말 것** --
# 호스트마다 구성이 다르다. 이름(N)으로 C6 를 찾는다.
#
# awk 의 FILENAME 에서 경로를 쪼개 state 디렉터리를 뽑는다:
#   /sys/devices/system/cpu/cpu0/cpuidle/state3/usage
#    1    2      3      4   5     6        7      8     <- split(FILENAME,p,"/") 의 p[]
#   (p[1] 은 선행 "/" 때문에 빈 문자열)
# ---------------------------------------------------------------------------
read -r -d '' REMOTE_READ <<'REMOTE_EOF'
CPUIDLE=/sys/devices/system/cpu/cpu0/cpuidle
if [ ! -d "$CPUIDLE" ]; then echo "E no-cpuidle"; exit 0; fi
awk '{split(FILENAME,p,"/"); print "N", p[8], $0}' $CPUIDLE/state*/name 2>/dev/null
awk '{split(FILENAME,p,"/"); print "L", p[8], $0}' $CPUIDLE/state*/latency 2>/dev/null
awk '{split(FILENAME,p,"/"); print "R", p[8], $0}' $CPUIDLE/state*/residency 2>/dev/null
awk '{split(FILENAME,p,"/"); print "D", p[8], $0}' $CPUIDLE/state*/disable 2>/dev/null
awk '{split(FILENAME,p,"/"); u[p[8]]+=$1} END{for(k in u) print "U", k, u[k]}' \
    /sys/devices/system/cpu/cpu[0-9]*/cpuidle/state*/usage 2>/dev/null
awk '{split(FILENAME,p,"/"); t[p[8]]+=$1} END{for(k in t) print "T", k, t[k]}' \
    /sys/devices/system/cpu/cpu[0-9]*/cpuidle/state*/time 2>/dev/null
echo "C $(ls -d /sys/devices/system/cpu/cpu[0-9]* 2>/dev/null | wc -l)"
awk '/^cpu MHz/ {s+=$4; n++} END{printf "F %.0f\n", (n?s/n:0)}' /proc/cpuinfo
REMOTE_EOF
: "${REMOTE_READ:?}"   # read -r -d '' 는 EOF 에서 1 을 돌려준다 -- 값만 확인

# ---------------------------------------------------------------------------
# info -- 유휴 단계 구성을 사람이 읽는 형태로
# ---------------------------------------------------------------------------
cmd_info() {
    local host="$1"
    local raw
    raw="$(run_on "$host" "$REMOTE_READ")" || { fail "$host: 읽기 실패"; return 1; }
    if grep -q '^E no-cpuidle' <<<"$raw"; then
        fail "$host: cpuidle 이 없다 (sysfs 미노출). 이 호스트에서는 가설 확인 불가"
        return 1
    fi

    printf '  %-9s %-7s %9s %11s %9s\n' state name exit_us residency_us disabled
    local sd
    for sd in $(awk '$1=="N"{print $2}' <<<"$raw" | sort -V); do
        printf '  %-9s %-7s %9s %11s %9s\n' \
            "$sd" \
            "$(awk -v s="$sd" '$1=="N"&&$2==s{print $3}' <<<"$raw")" \
            "$(awk -v s="$sd" '$1=="L"&&$2==s{print $3}' <<<"$raw")" \
            "$(awk -v s="$sd" '$1=="R"&&$2==s{print $3}' <<<"$raw")" \
            "$(awk -v s="$sd" '$1=="D"&&$2==s{print $3}' <<<"$raw")"
    done
    printf '  ncpu=%s  avg_mhz=%s\n' \
        "$(awk '$1=="C"{print $2}' <<<"$raw")" \
        "$(awk '$1=="F"{print $2}' <<<"$raw")"

    # 가장 깊은(=exit latency 가 가장 큰) 상태를 짚어 준다. 이게 가설의 주인공이다.
    local deep
    deep="$(awk '$1=="L"{print $3, $2}' <<<"$raw" | sort -n | tail -1)"
    if [ -n "$deep" ]; then
        local dl="${deep%% *}" ds="${deep##* }"
        printf '  -> 가장 깊은 상태: %s (%s), exit %sus\n' \
            "$ds" "$(awk -v s="$ds" '$1=="N"&&$2==s{print $3}' <<<"$raw")" "$dl"
        if [ "$dl" -lt 30 ] 2>/dev/null; then
            warn "가장 깊은 상태의 복귀가 ${dl}us 뿐이다 -- 관측된 ~130us 스톨을 설명 못 한다"
        fi
    fi
}

# ---------------------------------------------------------------------------
# snap -- 표본 한 번. CSV 행들을 stdout 으로.
# ---------------------------------------------------------------------------
CSV_HEADER='epoch_s,host,state,usage,time_us,ncpu,avg_mhz'

cmd_snap() {
    local host="$1"
    local now raw
    now="$(date +%s.%N)"
    raw="$(run_on "$host" "$REMOTE_READ")" || { warn "$host: 표본 건너뜀 (ssh 실패)"; return 1; }
    grep -q '^E no-cpuidle' <<<"$raw" && { warn "$host: cpuidle 없음"; return 1; }

    awk -v ts="$now" -v h="$host" '
        $1=="N" { name[$2] = $3 }
        $1=="U" { usage[$2] = $3 }
        $1=="T" { tim[$2]   = $3 }
        $1=="C" { ncpu = $2 }
        $1=="F" { mhz  = $2 }
        END {
            for (s in usage) {
                n = (s in name) ? name[s] : s
                printf "%s,%s,%s,%d,%d,%d,%s\n", ts, h, n, usage[s], tim[s], ncpu, mhz
            }
        }' <<<"$raw" | sort -t, -k3,3
}

# ---------------------------------------------------------------------------
# watch -- SIGINT 까지 snap 반복
# ---------------------------------------------------------------------------
cmd_watch() {
    local host="$1" outcsv="$2" interval="${3:-1}"

    [ -n "$outcsv" ] || { fail "watch 는 출력 CSV 경로가 필요하다"; return 1; }
    mkdir -p "$(dirname "$outcsv")" || return 1
    echo "$CSV_HEADER" > "$outcsv" || return 1

    local stop=0
    trap 'stop=1' INT TERM

    log "watch $host -> $outcsv (interval ${interval}s, Ctrl-C 로 종료)"
    local n=0 miss=0
    while [ "$stop" -eq 0 ]; do
        if cmd_snap "$host" >> "$outcsv"; then
            n=$((n + 1))
        else
            miss=$((miss + 1))
            # 스윕을 방해하면 안 되므로 실패해도 계속 간다. 다만 연속 실패가
            # 쌓이면 표본이 통째로 비는 것이라 알려 준다.
            if [ "$miss" -eq 5 ]; then warn "$host: 연속 5회 실패 -- 호스트/ssh 확인"; fi
        fi
        sleep "$interval"
    done

    printf '\n  표본 %d 개 (실패 %d) -> %s\n' "$n" "$miss" "$outcsv"
}

# ---------------------------------------------------------------------------
main() {
    [ $# -ge 1 ] || usage
    local sub="$1"; shift
    case "$sub" in
        info)  [ $# -ge 1 ] || usage; cmd_info  "$1" ;;
        snap)  [ $# -ge 1 ] || usage; { echo "$CSV_HEADER"; cmd_snap "$1"; } ;;
        watch) [ $# -ge 2 ] || usage; cmd_watch "$1" "$2" "${3:-1}" ;;
        -h|--help|help) usage ;;
        *) fail "알 수 없는 서브커맨드: $sub"; usage ;;
    esac
}

main "$@"
