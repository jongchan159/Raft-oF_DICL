#!/usr/bin/env bash
# 사용: ./ctxthreads.sh PID [초=10]
PID="$1"; SEC="${2:-10}"
snap() { for t in /proc/$PID/task/*; do
  [ -r "$t/status" ] && echo "$(basename $t) $(awk '/^voluntary_ctxt_switches/{print $2}' $t/status)"; done; }
snap > /tmp/ct0.$$; sleep "$SEC"; snap > /tmp/ct1.$$
awk -v sec="$SEC" 'NR==FNR{a[$1]=$2; next} ($1 in a){print ($2-a[$1])/sec} !($1 in a){print "NEW"}' \
    /tmp/ct0.$$ /tmp/ct1.$$ > /tmp/ct2.$$
newt=$(grep -c NEW /tmp/ct2.$$)
grep -v NEW /tmp/ct2.$$ | sort -n | awk -v sec="$SEC" -v newt="$newt" '
  { r[NR]=$1; tot+=$1 }
  END {
    n=NR
    printf "공통 스레드 %d개, 합계 %.0f/s, 새로 생긴 스레드 %d개(%.0f/s)\n", n, tot, newt, newt/sec
    printf "스레드별 속도(/s)  최대 %.0f  상위10%% %.0f  중앙 %.0f  최소 %.0f\n", r[n], r[int(n*0.9)], r[int(n/2)], r[1]
    for (i=1;i<=n;i++) if (r[i]>=100) {big++; bs+=r[i]}
    printf "100/s 이상 스레드 %d개, 그 합 %.0f/s (평균 %.0f/s)\n", big, bs, (big? bs/big:0)
  }'
rm -f /tmp/ct0.$$ /tmp/ct1.$$ /tmp/ct2.$$
