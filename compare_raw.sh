#!/usr/bin/env bash
# 스토리지 노드에서 sudo로 실행: 세 볼륨의 raw 구간이 바이트 단위로 같은지 비교한다.
# 사용: sudo ./compare_raw.sh <섹터수> <장치:skip> <장치:skip> <장치:skip>
#   예: sudo ./compare_raw.sh 14500  /dev/nvme1n1:80478209  /dev/nvme3n1:71024641  /dev/nvme5n1:80904193
#   skip = 컴퓨트 노드에서 filefrag로 구한 (첫 extent 물리 블록 x 8 + 1)
set -uo pipefail
COUNT="$1"; shift
EMPTY_MD5="d41d8cd98f00b204e9800998ecf8427e"          # 0바이트 입력의 md5 (읽기에 실패했다는 신호)
TMP=$(mktemp -d /dev/shm/rawcmp.XXXXXX); trap 'rm -rf "$TMP"' EXIT
want=$((COUNT * 512)); n=0; sums=(); fail=0
for spec in "$@"; do
  dev=${spec%%:*}; skip=${spec##*:}; n=$((n+1)); f="$TMP/v$n"; : > "$f"
  dd if="$dev" bs=512 skip="$skip" count="$COUNT" iflag=direct of="$f" 2>"$TMP/err$n"
  size=$(stat -c%s "$f" 2>/dev/null || echo 0)
  md5=$(md5sum < "$f" | cut -d' ' -f1)
  nz=$(tr -d '\0' < "$f" | wc -c)
  printf "%-16s skip=%-12s 읽은 바이트=%-9s md5=%s 0이 아닌 바이트=%s\n" "$dev" "$skip" "$size" "$md5" "$nz"
  if [[ "$size" -ne "$want" || "$md5" == "$EMPTY_MD5" ]]; then
    echo "  읽기 실패: 기대 ${want}바이트, 실제 ${size}바이트. dd 메시지: $(tr '\n' ' ' < "$TMP/err$n")"
    echo "  -> 장치 경로, skip 값, 권한(sudo), 구간이 장치 크기 안인지 확인"; fail=1
  elif [[ "$nz" -eq 0 ]]; then
    echo "  경고: 전부 0 -> 이 구간에 리더가 아직 안 썼거나 구간이 틀림"; fail=1
  fi
  sums+=("$md5")
done
[[ "$fail" -eq 0 ]] || { echo "무효: 위 문제를 해결하기 전에는 비교 결과를 믿을 수 없음"; exit 2; }
if [[ "$(printf '%s\n' "${sums[@]}" | sort -u | wc -l)" -eq 1 ]]; then
  echo "PASS: 세 볼륨의 raw 바이트가 같다 (복제 정확)"
else
  echo "FAIL: 해시가 다르다 -> 복제 결함 후보 (장치-노드 매핑 오기입 가능성부터 확인)"; exit 1
fi