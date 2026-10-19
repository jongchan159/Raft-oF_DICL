#!/usr/bin/env bash
# 사용: ./ctxsw.sh [초=10] [이름 정규식 또는 PID=^raft_node] [처리량 ops/s(선택)]
#   예) ./ctxsw.sh                    -> ^raft_node 프로세스를 자동으로 찾아 10초 측정
#       ./ctxsw.sh 10 ^raft_node 21318 -> 처리량을 주면 요청당 스위치 수도 출력
SEC="${1:-10}"; TARGET="${2:-^raft_node}"; TPUT="${3:-}"

if [[ "$TARGET" =~ ^[0-9]+$ ]]; then
  PIDS="$TARGET"
else
  PIDS="$(pgrep "$TARGET" | tr '\n' ' ')"      # 프로세스 이름(comm) 기준: sudo 래퍼는 제외
fi
if [ -z "${PIDS// /}" ]; then
  echo "대상 프로세스를 찾지 못함 (패턴: $TARGET)"; exit 1
fi

sum() { cat /proc/$1/task/*/status 2>/dev/null | awk -v k="$2" '$1==k":"{s+=$2} END{print s+0}'; }

declare -A v0 n0
for p in $PIDS; do v0[$p]=$(sum $p voluntary_ctxt_switches); n0[$p]=$(sum $p nonvoluntary_ctxt_switches); done
sleep "$SEC"
for p in $PIDS; do
  if [ ! -d /proc/$p ]; then echo "pid=$p 측정 중 종료됨 (값 무효)"; continue; fi
  v1=$(sum $p voluntary_ctxt_switches); n1=$(sum $p nonvoluntary_ctxt_switches)
  vr=$(( (v1 - v0[$p]) / SEC )); nr=$(( (n1 - n0[$p]) / SEC ))
  line="pid=$p ($(cat /proc/$p/comm 2>/dev/null))  ${SEC}s  voluntary=${vr}/s  involuntary=${nr}/s  threads=$(ls /proc/$p/task 2>/dev/null | wc -l)"
  if [ -n "$TPUT" ]; then
    line+="  per_request: voluntary=$(awk -v a=$vr -v t=$TPUT 'BEGIN{printf "%.2f", a/t}') involuntary=$(awk -v a=$nr -v t=$TPUT 'BEGIN{printf "%.2f", a/t}')"
  fi
  echo "$line"
done