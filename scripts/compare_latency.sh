#!/usr/bin/env bash
#
# compare_latency.sh -- apply_latency_sweep.sh 결과 여러 개를 크기별·항별로
#                       나란히 놓는다 (pull(destination) vs push 지연 비교용)
#
# 묻는 것: **같은 조건에서 복제 정책만 바꿨을 때 어느 항이 얼마나 달라지는가.**
# 기준(첫 번째 디렉터리) 대비 Δ 와 Δ% 를 항마다 낸다. 절차는
# E2E_EXPERIMENT.md §7-6.
#
# 사용법:
#   ./scripts/compare_latency.sh [-v] DIR_A DIR_B [DIR_C ...]
#
#   DIR_*   apply_latency_sweep.sh 의 출력 디렉터리 (안의 latency.csv 를 읽는다).
#           첫 번째가 기준이다.
#   -v      p50 외에 mean / p99 열도 낸다 (Δ 는 늘 p50 기준)
#
# 환경변수:
#   LABELS    열 이름 (공백 구분, 디렉터리 수만큼). 기본은 디렉터리 이름에서
#             apply_latency_ / lat_ 접두어를 뗀 것
#   CSV_OUT   주면 같은 값을 long 형식 CSV 로도 쓴다
#             (size_bytes,term,label,reps,mean_us,p50_us,p99_us)
#
# ── 읽기 전에 알 것 ───────────────────────────────────────────────────────
#
# **REPS>1 이면 행을 평균한다.** latency.csv 에는 rep 열이 없어서 같은
# (size, term) 행이 반복 수만큼 들어 있다. mean/p50/p99 를 각각 산술평균한다 --
# p50 의 평균은 전체의 p50 이 아니지만, 반복 간 흔들림을 줄이는 용도로는 충분하다.
# 표 머리에 라벨별 반복 수를 찍는다.
#
# **`ReplNet+StorageIO` 행은 mean 의 합이다.** 팔로워 데이터 경로 전체 비용으로,
# destination 의 (blockcopy RPC + 스토리지 복사) 와 push 의 (push 대기, ReplNet=0)
# 를 한 줄에서 비교하려고 둔다. p50 끼리는 더할 수 없어서(합의 p50 이 아니다)
# 이 행만 mean 으로 계산하고 term 이름에 (mean) 을 붙인다.
#
# **busy_retries 가 0 이 아닌 크기는 경고한다.** 그 지점의 수치는 지연이 아니라
# 링 백프레셔다 (apply_latency_sweep.sh 머리 주석 (3)).

set -uo pipefail

VERBOSE=0
if [ "${1:-}" = "-v" ]; then
    VERBOSE=1
    shift
fi

fail() { printf '[FAIL] %s\n' "$*" >&2; }

if [ $# -lt 2 ]; then
    fail "usage: $0 [-v] DIR_A DIR_B [DIR_C ...]"
    exit 2
fi

FILES=()
DEFAULT_LABELS=()
for d in "$@"; do
    f="$d/latency.csv"
    if [ ! -s "$f" ]; then
        fail "$f 가 없거나 비어 있다"
        exit 1
    fi
    if ! head -1 "$f" | grep -q '^size_bytes,n_samples,term,mean_us,p50_us,p99_us,'; then
        fail "$f 의 헤더가 apply_latency_sweep.sh 형식이 아니다"
        exit 1
    fi
    FILES+=("$f")
    b="$(basename "${d%/}")"
    b="${b#apply_latency_}"
    b="${b#lat_}"
    DEFAULT_LABELS+=("$b")
done

if [ -n "${LABELS:-}" ]; then
    read -r -a LABEL_ARR <<< "$LABELS"
    if [ "${#LABEL_ARR[@]}" -ne "${#FILES[@]}" ]; then
        fail "LABELS 는 ${#FILES[@]} 개여야 한다 (지금 ${#LABEL_ARR[@]} 개)"
        exit 2
    fi
else
    LABEL_ARR=("${DEFAULT_LABELS[@]}")
fi

# gawk 전용 기능은 쓰지 않는다 (mawk 에서도 같아야 한다 -- sweep 스크립트와 같은 원칙).
awk -F, -v labels="${LABEL_ARR[*]}" -v verbose="$VERBOSE" -v csv_out="${CSV_OUT:-}" '
function sizelabel(b) {
    if (b % 1048576 == 0) return (b / 1048576) "M"
    if (b % 1024 == 0)    return (b / 1024) "K"
    return b
}
function fmt(v) { return (v == "") ? "-" : sprintf("%.1f", v) }
function avg(l, s, t, col,    k) {
    k = l SUBSEP s SUBSEP t
    if (!(k in cnt)) return ""
    return sum[k, col] / cnt[k]
}
BEGIN {
    nl = split(labels, lab, " ")
    nterm = split("Total LHandler LPersist AENet FHandler ReplNet StorageIO QuorumWait Mutex CommitWait SRead SWrite residual", terms, " ")
    nsz = 0
    fi = 0
    if (csv_out != "") print "size_bytes,term,label,reps,mean_us,p50_us,p99_us" > csv_out
}
FNR == 1 { fi++; next }
NF >= 8 {
    s = $1 + 0; t = $3; l = lab[fi]
    k = l SUBSEP s SUBSEP t
    cnt[k]++
    sum[k, "mean"] += $4; sum[k, "p50"] += $5; sum[k, "p99"] += $6
    if (t == "Total") { reps[l, s]++; busy[l, s] += $8 }
    if (!(s in seen)) { seen[s] = 1; sizes[++nsz] = s }
}
END {
    # 크기 정렬 (삽입 정렬 -- asort 는 gawk 전용)
    for (i = 2; i <= nsz; i++) {
        v = sizes[i]; j = i - 1
        while (j >= 1 && sizes[j] > v) { sizes[j + 1] = sizes[j]; j-- }
        sizes[j + 1] = v
    }
    base = lab[1]
    for (si = 1; si <= nsz; si++) {
        s = sizes[si]
        hdr = sprintf("\n== size %s (%s B)   reps:", sizelabel(s), s)
        for (li = 1; li <= nl; li++) hdr = hdr sprintf(" %s=%d", lab[li], reps[lab[li], s] + 0)
        print hdr
        for (li = 1; li <= nl; li++)
            if (busy[lab[li], s] > 0)
                printf "   [warn] %s: busy_retries=%d -- 이 크기는 지연이 아니라 백프레셔가 섞였다\n", lab[li], busy[lab[li], s]

        line = sprintf("%-24s", "term (us)")
        for (li = 1; li <= nl; li++) {
            if (verbose) line = line sprintf(" %10s %10s %10s", lab[li] "_mean", lab[li] "_p50", lab[li] "_p99")
            else         line = line sprintf(" %12s", lab[li] "_p50")
            if (li > 1)  line = line sprintf(" %10s %8s", "d_" lab[li], "d%")
        }
        print line

        for (ti = 1; ti <= nterm + 1; ti++) {
            synth = (ti > nterm)
            t = synth ? "ReplNet+StorageIO(mean)" : terms[ti]
            any = 0
            line = sprintf("%-24s", t)
            for (li = 1; li <= nl; li++) {
                l = lab[li]
                if (synth) {
                    a = avg(l, s, "ReplNet", "mean"); b2 = avg(l, s, "StorageIO", "mean")
                    m = (a == "" && b2 == "") ? "" : a + b2
                    p50 = m; p99 = ""
                } else {
                    m = avg(l, s, t, "mean"); p50 = avg(l, s, t, "p50"); p99 = avg(l, s, t, "p99")
                }
                if (p50 != "") any = 1
                val[li] = p50
                if (verbose) line = line sprintf(" %10s %10s %10s", fmt(m), fmt(p50), fmt(p99))
                else         line = line sprintf(" %12s", fmt(p50))
                if (li > 1) {
                    if (val[1] == "" || p50 == "") line = line sprintf(" %10s %8s", "-", "-")
                    else {
                        d = p50 - val[1]
                        pct = (val[1] == 0) ? "-" : sprintf("%+.1f%%", d / val[1] * 100)
                        line = line sprintf(" %+10.1f %8s", d, pct)
                    }
                }
                if (csv_out != "" && p50 != "")
                    printf "%s,%s,%s,%d,%s,%s,%s\n", s, t, l, reps[l, s] + 0,
                        (m == "" ? "" : sprintf("%.1f", m)), sprintf("%.1f", p50),
                        (p99 == "" ? "" : sprintf("%.1f", p99)) >> csv_out
            }
            if (any) print line
        }
    }
    printf "\nbase = %s (d_* 는 base 대비 p50 차이, ReplNet+StorageIO 행만 mean)\n", base
}
' "${FILES[@]}"
