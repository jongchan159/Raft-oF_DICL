# `scripts/`

지금 이 디렉터리에는 실행 스크립트가 없다.

3노드 e2e 스크립트(`smoke_test.sh` / `restart_test.sh`)는 그 전에 제거되었다.
둘 다 `-identity-pba`(논리 오프셋 == 물리 오프셋)로 링 파일 자체를 볼륨처럼
취급해 블록 디바이스 없이 PBA 복사 경로를 돌리는 방식이었고, 그 모드가
사라지면서 성립하지 않는다.

측정 스크립트 두 개는 **레이턴시·시간 계측 제거와 함께 삭제했다.** 둘 다
지금은 존재하지 않는 기능을 구동하는 하네스였다:

- `apply_latency_sweep.sh` — 페이로드 크기별 apply 지연 스윕. `raft_client`의
  `-op apply-timed`와 노드의 `-profile` 플래그에 의존했고, 출력은 `ApplyTimings`
  11항(Total/LHandler/LPersist/AENet/FHandler/ReplNet/StorageIO/QuorumWait/
  Mutex/CommitWait/residual)이었다. 그 RPC도 플래그도 항목도 전부 없어졌다.
- `blkcopy_scaling.sh` — blockcopy 병렬도·배치 스케일링 실험. `raft_blkcopy_scale`
  바이너리를 구동했고, 그 바이너리를 삭제했다.

복제 경로를 끝까지 확인하려면 실클러스터에서 돌려야 한다 — 절차서는 저장소
루트의 `E2E_EXPERIMENT.md`.

계측을 되살릴 일이 생기면 두 스크립트는 git 이력에서 꺼낼 수 있다. 다만
서버 쪽(`ApplyTimings`, `ReplSink`, `ClientApplyTimed` RPC, 스토리지 노드의
`copy_nanos`)이 먼저 복원되어야 하며, proto 필드 번호는 `AppendEntriesResponse`
5..12와 `WritePBABatchResponse` 2..4가 `reserved`로 잡혀 있다.
