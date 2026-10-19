#!/usr/bin/env bash
# 사용: node_cfg.sh [이름 정규식]   기본 ^raft_node  (프로세스 이름이 raft_node로 시작하는 것 전부)
#   예) node_cfg.sh            -> raft_node, raft_node_xxx ...
#       node_cfg.sh '^block_'  -> block_server 등
PAT="${1:-^raft_node}"
echo "host=$(hostname)  date=$(date +%F_%T)"
pids="$(pgrep "$PAT" | tr '\n' ',' | sed 's/,$//')"      # 이름(comm)만 매칭: sudo/stdbuf 래퍼는 제외됨
if [ -z "$pids" ]; then
  echo "(이 머신에 '$PAT' 프로세스 없음)"
  exit 0
fi
ps -ww -o pid=,etimes=,lstart=,args= -p "$pids" | while IFS= read -r line; do
  line="${line#"${line%%[![:space:]]*}"}"
  pid="${line%% *}"; rest="${line#* }"
  rest="${rest#"${rest%%[![:space:]]*}"}"
  etimes="${rest%% *}"; rest="${rest#* }"
  rest="${rest#"${rest%%[![:space:]]*}"}"
  lstart="${rest:0:24}"; args="${rest:25}"
  echo "pid=$pid  node_started=$lstart  (uptime=${etimes}s)"
  read -r -a tok <<< "$args"
  echo "  exe                $(basename "${tok[0]}")"
  i=1; summary=""
  while [ $i -lt ${#tok[@]} ]; do
    t="${tok[$i]}"
    if [[ "$t" == -* ]]; then
      k="${t#-}"; v=""
      nx="${tok[$((i+1))]:-}"
      if [ -n "$nx" ] && [[ "$nx" != -* ]]; then v="$nx"; i=$((i+2)); else v="on"; i=$((i+1)); fi
      if [ "$k" = "cluster" ]; then v="$(echo "$v" | tr ',' '\n' | wc -l) members"; fi
      printf '  %-18s %s\n' "$k" "$v"
      summary+="$k=$v "
    else
      i=$((i+1))
    fi
  done
  echo "  CFG: $summary"
done