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
sudo nvme connect -t rdma -a 10.0.0.91 -s <trsvcid> -n node5 --fast_io_fail_tmo=1
sudo nvme list-subsys        # NQN -> /dev/nvmeXnY 대응을 기록한다 (이름은 호스트마다 다르다)

# 3) raft_node 실행 사용자에게 그 장치들의 rw 권한 (E2E_EXPERIMENT.md §3-1 과 같은 방식)

# 4) hostid 가 호스트마다 다른지 (나중에 펜싱할 때 필요)
cat /etc/nvme/hostid
```

```bash
# 예: eternity4 에서. 경로는 위 nvme list-subsys 로 확인한 **이 호스트의** 이름
./build/raft_node -id 4 -cluster "$CLUSTER" ... -mode push -push-devices "5=/dev/nvmeXn1,6=/dev/nvmeYn1"
```

기동 배너의 `push device : 5 -> ... (wwid W)` 가 eternity5 배너의 `own volume : ... (wwid W)` 와 같아야 한다. 다르면 엉뚱한 볼륨을 매핑한 것이니 그대로 돌리지 말 것.
