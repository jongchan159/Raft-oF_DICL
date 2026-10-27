**c-state off**

'echo 1 | sudo tee /sys/devices/system/cpu/*/cpuidle/state3/disable'

**polling mode on/off**

raft_rdma_transport.cpp:242-250 constexpr int kSpinServeUs = 0; (off) or = -1; (polling)

**CLUSTER**

```bash
# IPoIB(RDMA)
CLUSTER="4@10.0.0.4:6001@/dev/nvme1n1@10.0.0.91:5050,\
5@10.0.0.5:6001@/dev/nvme2n1@10.0.0.91:5050,\
6@10.0.0.6:6001@/dev/nvme3n1@10.0.0.91:5050"

ADDRS=10.0.0.4:6001,10.0.0.5:6001,10.0.0.6:6001

# TCP
CLUSTER="4@115.145.173.123:6001@/dev/nvme1n1@115.145.173.243:5050,\
5@115.145.173.124:6001@/dev/nvme2n1@115.145.173.243:5050,\
6@115.145.173.125:6001@/dev/nvme3n1@115.145.173.243:5050"

ADDRS=115.145.173.123:6001,115.145.173.124:6001,115.145.173.125:6001

```

nvme device 오류 발생 시
lsblk -d -o NAME,MODEL,SIZE,TRAN,MOUNTPOINT | grep nvme; for c in /sys/class/nvme/nvme*; do echo "$(basename $c) $(cat $c/transport) $(cat $c/address
  2>/dev/null)"; done

**PUSH MODE (다른 멤버 볼륨 attach)**

`-mode push` 리더는 팔로워 볼륨에 직접 쓴다 (DECISIONS.md D20). 그래서 컴퓨트 노드마다 **다른 멤버의 볼륨도 attach** 해야 한다. **절대 마운트하지 않는다** -- ext4 를 두 호스트가 마운트하면 깨진다.

```bash
# 1) 스토리지 노드: 각 서브시스템이 다른 컴퓨트 호스트의 연결도 받는지 확인
for s in /sys/kernel/config/nvmet/subsystems/*; do
  echo "$(basename $s) allow_any=$(cat $s/attr_allow_any_host) hosts=$(ls $s/allowed_hosts)"
done
#   막혀 있으면 그 호스트의 hostnqn 을 allowed_hosts 에 링크한다
#   RDMA 포트로 export 안 된 서브시스템은 TCP 로 붙는다 (E2E_EXPERIMENT.md §1)

# 2) 컴퓨트 노드마다: 다른 멤버의 서브시스템에 연결 (trsvcid 는 nvmet ports/*/addr_trsvcid)
sudo nvme connect -t rdma -a 10.0.0.91 -s <trsvcid> -n node5
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
# 컴퓨트 노드에서 고정 (재부팅 / ip neigh del 하면 사라진다)
sudo ip neigh replace 10.0.0.91 dev ibp59s0 nud permanent \
  lladdr 00:00:10:29:fe:80:00:00:00:00:00:00:b8:3f:d2:03:00:0e:00:4e
# 근본 해결은 스토리지 노드의 sysctl net.ipv4.conf.all.arp_ignore=1 arp_announce=2 (공유 장비라 관리자와 상의)
```

```bash
# 예: eternity4 에서. 경로는 위 nvme list-subsys 로 확인한 **이 호스트의** 이름
./build/raft_node -id 4 -cluster "$CLUSTER" ... -mode push -push-devices "5=/dev/nvmeXn1,6=/dev/nvmeYn1"
```

기동 배너의 `push device : 5 -> ... (wwid W)` 가 eternity5 배너의 `own volume : ... (wwid W)` 와 같아야 한다. 다르면 엉뚱한 볼륨을 매핑한 것이니 그대로 돌리지 말 것.
