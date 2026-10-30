**c-state off**

```bash
echo 1 | sudo tee /sys/devices/system/cpu/*/cpuidle/state3/disable    # state3 = C6
```

**polling mode on/off**

net/src/raft_rdma_transport.cpp:75-76 `constexpr int kSpinServeUs = 0;` (off, sleep) 또는 `= -1;` (polling, 무한 스핀)

**CLUSTER**

device 필드는 **그 컴퓨트 노드에서 본 자기 볼륨 경로**이고 각 노드는 자기 항목만 쓴다.
2026-10-27 현재 세 호스트 모두 자기 볼륨이 `nvme1n1` 이다. 번호는 attach 순서로 정해지므로
재부팅이나 재연결 뒤에는 아래 "연결 확인" 으로 다시 볼 것 (틀리면
`init_storage failed: OpenCachedFD: open device(wr) ...: No such file or directory`).

```bash
# IPoIB(RDMA)
CLUSTER="4@10.0.0.4:6001@/dev/nvme1n1@10.0.0.91:5050,\
5@10.0.0.5:6001@/dev/nvme1n1@10.0.0.91:5050,\
6@10.0.0.6:6001@/dev/nvme1n1@10.0.0.91:5050"

ADDRS=10.0.0.4:6001,10.0.0.5:6001,10.0.0.6:6001

# TCP
CLUSTER="4@115.145.173.123:6001@/dev/nvme1n1@115.145.173.243:5050,\
5@115.145.173.124:6001@/dev/nvme1n1@115.145.173.243:5050,\
6@115.145.173.125:6001@/dev/nvme1n1@115.145.173.243:5050"

ADDRS=115.145.173.123:6001,115.145.173.124:6001,115.145.173.125:6001

```

nvme device 오류 발생 시 (lsblk 의 TRAN 은 NVMe-oF 장치도 `nvme` 로 나온다 -- 원격 여부는 아래 transport 로 본다)

```bash
lsblk -d -o NAME,MODEL,SIZE,MOUNTPOINT | grep nvme
for c in /sys/class/nvme/nvme*; do echo "$(basename $c) $(cat $c/transport) $(cat $c/address 2>/dev/null)"; done
```

**PUSH MODE (다른 멤버 볼륨 attach)**

`-mode push` 리더는 팔로워 볼륨에 직접 쓴다 (DECISIONS.md D20). 그래서 컴퓨트 노드마다 **다른 멤버의 볼륨도 attach** 해야 한다. **절대 마운트하지 않는다** -- ext4 를 두 호스트가 마운트하면 깨진다.

```bash
# 1) 스토리지 노드: 각 서브시스템이 다른 컴퓨트 호스트의 연결도 받는지 확인
for s in /sys/kernel/config/nvmet/subsystems/*; do
  echo "$(basename $s) allow_any=$(cat $s/attr_allow_any_host) hosts=$(ls $s/allowed_hosts)"
done
#   막혀 있으면(allow_any=0) 그 호스트의 hostnqn(cat /etc/nvme/hostnqn) 을 allowed_hosts 에 링크하거나
#   allow_any=1 로 연다. allowed_hosts 에 항목이 하나라도 남아 있으면 allow_any=1 은 EINVAL 로 거부된다.
#   2026-10-27 현재 node1~6 전부 allow_any=1 (node4 는 원래 eternity4 ...ac1f6bb17902 만 허용이었다)
#   RDMA 포트로 export 안 된 서브시스템은 TCP 로 붙는다 (E2E_EXPERIMENT.md §1)

# 2) 컴퓨트 노드마다: 다른 멤버의 서브시스템에 연결
#    RDMA 포트(nvmet port 10) = 10.0.0.91:4420. 예: eternity4 는 node5, node6
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node5
sudo nvme list-subsys        # NQN -> /dev/nvmeXnY 대응을 기록한다 (이름은 호스트마다 다르다)
#   "Connect rejected: status 8 (invalid service ID)" (dmesg) 면 ARP 문제다 -- 아래 "ARP" 참고

# 2-1) 새로 붙인 컨트롤러에만 fast_io_fail_tmo=1 (자기 볼륨 컨트롤러는 건드리지 않는다)
#   nvme-cli 2.8 의 connect 에는 --fast_io_fail_tmo 옵션이 없어서 sysfs 로 준다.
#   끊긴 지 1 초 뒤 큐에 묶인 I/O 를 EIO 로 돌려준다 -- push 쓰기가 mu 를 잡고 멈추지 않게,
#   옛 리더의 늦은 push 쓰기 창을 줄이게 (DECISIONS.md D20 "한계").
#   자기 볼륨(마운트된 ext4)에 주면 짧은 끊김에도 ext4 가 I/O 에러를 받는다.
#   재연결(disconnect/connect)하면 기본값(off)으로 돌아가므로 다시 준다.
echo 1 | sudo tee /sys/class/nvme/nvmeX/fast_io_fail_tmo

# 3) raft_node 실행 사용자에게 그 장치들의 rw 권한 (E2E_EXPERIMENT.md §3-1 과 같은 방식)

# 4) hostid 가 호스트마다 다른지 (나중에 펜싱할 때 필요)
cat /etc/nvme/hostid
#   2026-10-27 현재 eternity5 와 eternity6 의 hostnqn/hostid 가 같다 (이미지 복제). 연결은 되지만
#   타깃이 두 호스트를 구분하지 못한다 -- 펜싱을 넣기 전에 재생성할 것
```

**연결 확인** -- 아무 호스트에서나 (root 불필요). 기동 전에 매번 돌린다.

```bash
for h in eternity4 eternity5 eternity6; do echo "== $h"; ssh $h '
  for s in /sys/class/nvme-subsystem/*; do n=$(cat $s/subsysnqn); case $n in node*)
    for d in $s/nvme*n*; do b=$(basename $d); c=$(ls -d $s/nvme[0-9]* | head -1 | xargs basename)
      echo "  $n -> /dev/$b state=$(cat /sys/class/nvme/$c/state) wwid=$(cat $d/wwid)" \
           "fio_tmo=$(cat /sys/class/nvme/$c/fast_io_fail_tmo) mnt=[$(findmnt -no TARGET /dev/$b)]"
    done;; esac; done'; done
```

통과 조건:
- 호스트마다 `node4`/`node5`/`node6` 세 줄, 전부 `state=live`
- **자기 볼륨만 `mnt=[/mnt/raftvol]`**, 나머지 둘은 `mnt=[]`. 비어 있지 않으면 즉시 umount
- 같은 node 의 `wwid` 가 세 호스트에서 같다 -- 이 줄의 `node5 -> /dev/nvmeXn1` 이 그 호스트의
  `-push-devices 5=/dev/nvmeXn1` 이다
- 새로 붙인 둘은 `fio_tmo=1`, 자기 볼륨은 `off`

**ARP -- `invalid service ID` 로 연결이 거절될 때**

eternitystorage 는 IB 포트 두 개가 같은 서브넷에 있다(`ibp59s0`=10.0.0.91, `ibp216s0`=10.0.0.90).
기본 `arp_ignore=0` 이라 `ibp216s0` 도 10.0.0.91 의 ARP 에 답하고, 컴퓨트 노드가 그 주소를 배우면
RDMA CM 연결 요청이 nvmet 리스너가 없는 포트로 가서 거절된다(dmesg `Connect rejected: status 8
(invalid service ID)`, connect 는 `Connection reset by peer`). 이미 맺은 연결은 영향이 없다.

```bash
ip neigh show 10.0.0.91     # 끝이 ...00:0e:00:4e 가 맞다. ...00:0d:ff:de 면 잘못 배운 것
# 맞는 값은 스토리지에서 직접 읽는다. 앞 4바이트(00:00:10:29)는 IPoIB QPN 이라
# 스토리지 재부팅 / 드라이버 재적재 뒤 바뀔 수 있다 -- 고정하기 전에 매번 확인
ssh eternitystorage ip -br link show ibp59s0
# 컴퓨트 노드에서 고정 (재부팅 / ip neigh del 하면 사라진다)
sudo ip neigh replace 10.0.0.91 dev ibp59s0 nud permanent \
  lladdr 00:00:10:29:fe:80:00:00:00:00:00:00:b8:3f:d2:03:00:0e:00:4e
# 근본 해결은 스토리지 노드의 sysctl net.ipv4.conf.all.arp_ignore=1 arp_announce=2 (공유 장비라 관리자와 상의)
```

**기동** -- 스토리지 노드의 blockcopy 서버가 먼저 떠 있어야 한다 (push 전의 no-op, 경계 배치는 pull 로 간다.
E2E_EXPERIMENT.md §5-1). 세 노드 모두 `-mode push`, `-ring-pages` 는 세 노드가 같아야 한다(다르면 맵 검증이 거절).

`/mnt/raftvol/node{4,5,6}` 에는 다른 사용자(ryudb00)의 링 파일이 있다. 같은 디렉터리로 띄우면 그 파일을
그대로 재사용한다(헤더의 term/votedFor 까지) -- 별도 디렉터리를 쓴다.

```bash
sudo install -d /mnt/raftvol/jongc-push/node4      # 호스트마다 자기 id
COMMON="-ring-pages 4096 -heartbeat-ms 300 -profile -mode push -statemachine hash -debug"
# -push-devices 경로는 위 "연결 확인" 의 **이 호스트** 줄에서 가져온다 (2026-10-27 기준 값)
# eternity4
sudo ./build/raft_node -id 4 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/jongc-push/node4 $COMMON -push-devices "5=/dev/nvme2n1,6=/dev/nvme3n1"
# eternity5
sudo ./build/raft_node -id 5 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/jongc-push/node5 $COMMON -push-devices "4=/dev/nvme2n1,6=/dev/nvme3n1"
# eternity6
sudo ./build/raft_node -id 6 -cluster "$CLUSTER" -metadata-dir /mnt/raftvol/jongc-push/node6 $COMMON -push-devices "4=/dev/nvme2n1,5=/dev/nvme3n1"
```

기동 배너의 `push device : 5 -> ... (wwid W)` 가 eternity5 배너의 `own volume : ... (wwid W)` 와 같아야 한다. 다르면 엉뚱한 볼륨을 매핑한 것이니 그대로 돌리지 말 것.

D21(커밋 판정을 AE 응답 경로에서도 함) 이전 바이너리로 지연을 잴 때는 `-loop-sleep-us 20` 을 더한다 (E2E_EXPERIMENT.md §7-6 -- 기본 200 은 커밋 폴링 지연이 QuorumWait 에 섞인다). D21 이후에는 필요 없다.

**MULTI-STORAGE (스토리지 노드 간 attach)**

스토리지 노드가 여러 대면 팔로워의 스토리지 노드에 리더 볼륨이 로컬에 없다. 그래서 다른 스토리지 노드가
export 하는 멤버 볼륨을 NVMe-oF 로 attach 해 두고, blockcopy 가 그걸 pread 해 자기 로컬 볼륨에 pwrite 한다.
**코드는 바뀌지 않는다** -- 서버는 `-devices` 인덱스로만 장치를 고르고 로컬/원격을 구분하지 않는다. 구조와
불변식은 E2E_EXPERIMENT.md §1 "스토리지 노드가 여러 대일 때".

두 번째 스토리지는 eternity1(IPoIB 10.0.0.1, ibp59s0 4X EDR)로 에뮬레이션할 예정이다. 멤버 볼륨 배정은 아직
정하지 않았으므로 아래 명령은 배정과 무관하게 썼다(상대가 export 하는 node4/5/6 만 골라 붙는다).
2026-10-30 실측 eternity1: nvmet 미적재(configfs 에 subsystems 없음), 로컬 SSD 는 부트 디스크
`nvme0n1`(970 EVO Plus 250GB) 하나, `nvme1n2`(NVMe-oF, `/mnt/raftvol` 마운트)가 이미 붙어 있다 --
export 할 볼륨을 정하기 전에 그게 누구 것인지 확인할 것.

```bash
# 1) 각 스토리지 노드의 nvmet: 상대 스토리지 노드도 받는지 (위 PUSH MODE 1) 과 같은 확인)
for s in /sys/kernel/config/nvmet/subsystems/node*; do
  echo "$(basename $s) allow_any=$(cat $s/attr_allow_any_host) hosts=$(ls $s/allowed_hosts) ports=$(ls /sys/kernel/config/nvmet/ports/*/subsystems | grep -c $(basename $s))"
done
```

eternitystorage -- eternity1 이 export 하는 node4/5/6 을 붙인다
```bash
sudo modprobe nvme-rdma
for n in node4 node5 node6; do sudo nvme discover -t rdma -a 10.0.0.1 -s 4420 | grep -qE "subnqn: *$n *$" && sudo nvme connect -t rdma -a 10.0.0.1 -s 4420 -n $n; done
for c in /sys/class/nvme/nvme*; do grep -q "traddr=10.0.0.1," $c/address 2>/dev/null && echo 1 | sudo tee $c/fast_io_fail_tmo; done
```

eternity1 -- eternitystorage 가 export 하는 node4/5/6 을 붙인다 (`invalid service ID` 면 위 "ARP" 의 고정을 eternity1 에서도 한다)
```bash
ip neigh show 10.0.0.91
for n in node4 node5 node6; do sudo nvme discover -t rdma -a 10.0.0.91 -s 4420 | grep -qE "subnqn: *$n *$" && sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n $n; done
for c in /sys/class/nvme/nvme*; do grep -q "traddr=10.0.0.91," $c/address 2>/dev/null && echo 1 | sudo tee $c/fast_io_fail_tmo; done
```

- **붙인 장치는 절대 마운트하지 않는다.** blockcopy 는 그 장치를 읽기만 한다
- `fast_io_fail_tmo=1` 은 새로 붙인 컨트롤러에만 준다. 상대 스토리지가 죽으면 pread 가 1 초 뒤 EIO 로 끝나고,
  do_pba_copy 가 그 배치를 fail-soft 로 건너뛴다(다음 라운드에 다시 pull)
- eternitystorage 는 IB 포트 두 개가 같은 /24 다. 거기서 나가는 connect 가 거절되면 ARP 를 먼저 의심한다

**스토리지 노드 연결 확인 + `-devices` 만들기** -- 스토리지 호스트마다. 로컬 멤버는 nvmet `device_path`(by-id),
원격 멤버는 attach 된 `/dev/nvmeXnY` 다. **루프의 node 순서는 `-cluster` 순서(지금 4,5,6)와 같아야 한다.**

eternitystorage
```bash
for n in node4 node5 node6; do p=$(cat /sys/kernel/config/nvmet/subsystems/$n/namespaces/*/device_path 2>/dev/null); k=local; if [ -z "$p" ]; then k=remote; for s in /sys/class/nvme-subsystem/*; do [ "$(cat $s/subsysnqn)" = $n ] && p=/dev/$(basename $(ls -d $s/nvme*n* | head -1)); done; fi; echo "$n $k ${p:-MISSING} wwid=$(cat /sys/class/block/$(basename $(readlink -f ${p:-/x}))/wwid 2>/dev/null) mnt=[$(findmnt -no TARGET ${p:-/x} 2>/dev/null)]"; done
```

eternity1
```bash
for n in node4 node5 node6; do p=$(cat /sys/kernel/config/nvmet/subsystems/$n/namespaces/*/device_path 2>/dev/null); k=local; if [ -z "$p" ]; then k=remote; for s in /sys/class/nvme-subsystem/*; do [ "$(cat $s/subsysnqn)" = $n ] && p=/dev/$(basename $(ls -d $s/nvme*n* | head -1)); done; fi; echo "$n $k ${p:-MISSING} wwid=$(cat /sys/class/block/$(basename $(readlink -f ${p:-/x}))/wwid 2>/dev/null) mnt=[$(findmnt -no TARGET ${p:-/x} 2>/dev/null)]"; done
```

통과 조건:
- 세 줄 전부 `MISSING` 이 아니고, 모든 줄이 `mnt=[]` 이다
- 한 멤버는 정확히 한 스토리지에서만 `local` 이다
- `remote` 줄의 wwid 는 컴퓨트 노드 "연결 확인" 의 같은 node wwid(nvmet uuid)와 같다. `local` 줄의 wwid 는
  SSD 자체의 것이라 **다른 게 정상**이다 -- local 은 by-id 시리얼로 대조한다(아래 "재부팅 시 복구" 의 표)

기동 -- 스토리지 호스트마다 위 출력의 세 번째 열을 그 순서대로 `-devices` 로 준다 (eternitystorage, eternity1 각각에서 같은 줄)
```bash
cd ~/raftof_clean
sudo ./build/raft_blockcopy_server -addr 0.0.0.0:5050 -copy-workers 8 -devices $(for n in node4 node5 node6; do p=$(cat /sys/kernel/config/nvmet/subsystems/$n/namespaces/*/device_path 2>/dev/null); if [ -z "$p" ]; then for s in /sys/class/nvme-subsystem/*; do [ "$(cat $s/subsysnqn)" = $n ] && p=/dev/$(basename $(ls -d $s/nvme*n* | head -1)); done; fi; echo ${p:-MISSING}; done | paste -sd,)
```
배너의 `device[0..2]` 가 확인 루프의 세 번째 열과 같은 순서인지 본다. `MISSING` 이 있으면 open 실패로 멈춘다.

`-cluster` 는 멤버마다 `storage_host` 를 **그 멤버 볼륨을 `local` 로 갖는 스토리지**의 IPoIB 로 준다
(eternitystorage = `10.0.0.91:5050`, eternity1 = `10.0.0.1:5050`). 팔로워는 자기 `storage_host` 에만
WritePBABatch 를 보내고, 그 서버가 dst(자기 볼륨)를 로컬로, src(리더 볼륨)를 로컬 또는 attach 로 연다.

**스토리지 노드(eternitystorage) 재부팅 시 복구**

2026-10-28 에 실제로 겪었다. 재부팅하면 세 가지가 한꺼번에 깨진다.
- **nvmet 설정이 통째로 사라진다.** configfs 는 휘발성이고 저장해 둔 설정(`/etc/nvmet`)이 없다.
- **스토리지 로컬 NVMe 번호가 다시 매겨진다.** 재부팅 전 node4/5/6 = nvme1n1/nvme3n1/nvme5n1 이었던 번호가
  파일시스템 없는 다른 디스크가 됐다. **예전 번호로 export 하면 남의 디스크를 내보낸다.**
- **컴퓨트 노드는 연결을 잃는다.** `ctrl_loss_tmo`(600 초) 뒤 컨트롤러가 삭제되고 다시 생기지 않는다.
  `/mnt/raftvol` 은 마운트된 채 `Input/output error`. 자동 재연결(autoconnect, fstab)이 없다.

증상 확인 (아무 호스트, root 불필요):
```bash
ssh eternitystorage uptime -s; ssh eternitystorage ls /sys/kernel/config/nvmet/subsystems
for h in eternity4 eternity5 eternity6; do echo "== $h $(ssh $h 'ls /sys/class/nvme | tr "\n" " "; ls /mnt/raftvol 2>&1 | head -1')"; done
```

**1) 스토리지 (root): 디스크를 내용으로 식별한다.** 번호를 믿지 말 것. ext4 가 있는 디스크에서 `/jongc-push`
아래의 `nodeN` 으로 가린다(그 디렉터리는 각 볼륨에 하나씩 만들었다). `debugfs` 는 기본이 읽기 전용이다.
```bash
for d in /dev/nvme*n1; do f=$(blkid -o value -s TYPE $d); [ "$f" = ext4 ] || continue; echo "$d $(debugfs -R 'ls /jongc-push' $d 2>/dev/null | grep -oE 'node[0-9]+' | sort -u | tr '\n' ' ') $(ls -l /dev/disk/by-id/ | grep -E "nvme-SAMSUNG.*_[A-Z0-9]+ -> ../../$(basename $d)\$" | awk '{print $9}')"; done
```
2026-10-28 식별 결과 (by-id 는 SSD 시리얼이라 재부팅해도 같다):

| 멤버 | by-id | 그때 번호 |
|---|---|---|
| node4 | `/dev/disk/by-id/nvme-SAMSUNG_MZQLB960HAJR-00007_S437NA0N509494` | nvme12n1 |
| node5 | `/dev/disk/by-id/nvme-SAMSUNG_MZQLB960HAJR-00007_S437NA0N509496` | nvme10n1 |
| node6 | `/dev/disk/by-id/nvme-SAMSUNG_MZQLB960HAJR-00007_S437NA0N509484` | nvme13n1 |

node1(nvme16n1)·node2(nvme17n1) 등 나머지는 다른 사용자 것이다 -- 건드리지 않는다.

**2) 스토리지 (root): nvmet 재생성 + blockcopy 서버.** device_path 와 `-devices` 모두 by-id 로 준다.
```bash
sudo modprobe nvmet nvmet-rdma
cd /sys/kernel/config/nvmet
ID=/dev/disk/by-id/nvme-SAMSUNG_MZQLB960HAJR-00007
for x in node4:S437NA0N509494 node5:S437NA0N509496 node6:S437NA0N509484; do n=${x%%:*}; sudo mkdir -p subsystems/$n/namespaces/10; echo 1 | sudo tee subsystems/$n/attr_allow_any_host; echo ${ID}_${x#*:} | sudo tee subsystems/$n/namespaces/10/device_path; echo 1 | sudo tee subsystems/$n/namespaces/10/enable; done
sudo mkdir -p ports/10; echo rdma | sudo tee ports/10/addr_trtype; echo ipv4 | sudo tee ports/10/addr_adrfam; echo 10.0.0.91 | sudo tee ports/10/addr_traddr; echo 4420 | sudo tee ports/10/addr_trsvcid
for n in node4 node5 node6; do sudo ln -s /sys/kernel/config/nvmet/subsystems/$n ports/10/subsystems/$n; done
cd ~/raftof_clean
sudo ./build/raft_blockcopy_server -addr 0.0.0.0:5050 -copy-workers 8 -devices ${ID}_S437NA0N509494,${ID}_S437NA0N509496,${ID}_S437NA0N509484
```
- 네임스페이스를 새로 만들므로 **wwid 가 바뀐다**(2026-10-28 이후 node4=`uuid.26bc4957…` node5=`uuid.5ab9ffcd…`
  node6=`uuid.1e2dd756…`). 배너는 "연결 확인" 의 새 값과 대조한다
- 원래 있던 TCP 포트(port 1, 115.145.173.243:4420)와 node1~3 은 주인이 복구한다
- 같은 IPoIB lladdr 이면 컴퓨트 노드의 ARP 고정값은 그대로 유효하다. 바뀌었으면 위 "ARP" 절

**3) 컴퓨트 노드: 자기 볼륨을 먼저 연결해야 `nvme1n1` 이 된다** (CLUSTER 가 그것을 가정한다).
첫 connect 뒤 `subsysnqn` 이 자기 노드가 아니면 e2fsck/mount 로 넘어가지 말 것.
`e2fsck -p` 가 "UNEXPECTED INCONSISTENCY" 로 멈추면 `-y` 를 주지 말고 멈춘다(etcd 등 남의 파일도 있다).

eternity4
```bash
sudo umount -l /mnt/raftvol
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node4
cat /sys/class/nvme/nvme1/subsysnqn
sudo e2fsck -p /dev/nvme1n1
sudo mount /dev/nvme1n1 /mnt/raftvol
ls /mnt/raftvol
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node5
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node6
```

eternity5
```bash
ip neigh show 10.0.0.91
sudo umount -l /mnt/raftvol
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node5
cat /sys/class/nvme/nvme1/subsysnqn
sudo e2fsck -p /dev/nvme1n1
sudo mount /dev/nvme1n1 /mnt/raftvol
ls /mnt/raftvol
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node4
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node6
```

eternity6
```bash
ip neigh show 10.0.0.91
sudo umount -l /mnt/raftvol
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node6
cat /sys/class/nvme/nvme1/subsysnqn
sudo e2fsck -p /dev/nvme1n1
sudo mount /dev/nvme1n1 /mnt/raftvol
ls /mnt/raftvol
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node4
sudo nvme connect -t rdma -a 10.0.0.91 -s 4420 -n node5
```

필요하면 각 호스트에서 `echo 1 | sudo tee /sys/class/nvme/nvme2/fast_io_fail_tmo /sys/class/nvme/nvme3/fast_io_fail_tmo` (위 2-1).

**4) 확인.** 위 "연결 확인" 루프. 호스트마다 node4/5/6 이 `live`, 자기 볼륨만 마운트, 같은 node 의 wwid 가 세
호스트에서 같아야 한다. 그다음 `-push-devices` 를 그 출력에서 다시 읽는다.
