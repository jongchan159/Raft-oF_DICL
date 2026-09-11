# `apps/` — 실행 파일 진입점

**여기에 알고리즘은 없다.** 조립부(composition root)다.
프로토콜 본체는 `core/`, 그 읽기 시작점은 `core/src/raft_lifecycle.cpp` 의
`Server::main_loop` 다.

| 파일 | 바이너리 | 링크 |
|---|---|---|
| `raft_node_main.cpp` | `raft_node` | core + blockio + net + wire + proto |
| `raft_client_main.cpp` | `raft_client` | wire + proto (**core 없음**) |
| `raft_blockcopy_server_main.cpp` | `raft_blockcopy_server` | wire + storage + proto (**core 없음**) |
| `raft_blkcopy_scale_main.cpp` | `raft_blkcopy_scale` | wire + proto (**core 없음**) |

## `raft_blkcopy_scale_main.cpp`

`BlockCopyServer.HandleWritePBABatch` 를 직접 쏘는 처리량/포화점 측정 도구다.
Raft 를 거치지 않으므로 선거·quorum·persist 잡음 없이 pread+pwrite 만 남는다.
한 지점(W, B, chunk 고정)만 재고, 스윕은 `scripts/blkcopy_scaling.sh` 가 돈다 —
W(`-copy-workers`)가 **서버** 시작 플래그라 지점마다 서버를 다시 띄워야 한다.

지연 전용이던 `raft_blkcopy_bench` 를 2026-09-10 에 대체했다. 그쪽은
`-batch 1` / `-copy-workers 1` 을 계약으로 고정하고 있어 스케일링 스윕과
양립할 수 없었다 (DECISIONS.md D15). 지연이 필요하면 W=1,B=1 코너를 보면 된다.

**두 가지 함정이 usage 에도 적혀 있다**: `copy_nanos` 는 워커별 시간의 **합**이라
처리량 분모로 쓰면 안 되고(`raft_blockcopy_server.cpp:284`), `-workers` 는 이
도구가 검증할 수 없는 **기록 전용** 라벨이라 서버 기동 로그의
`copy-workers : N` 과 대조해야 한다.

## `raft_node_main.cpp` 가 하는 일의 전부

`Server` 에 대해 부르는 것은 **세 개뿐**이다: `init_storage()` / `start()` /
`stop()`. 나머지는 인자 파싱, 필드 채우기, 전송 구현체 주입, `-debug` 상태
덤프다. `raft.current_term` 등을 읽는 곳이 몇 군데 있지만 전부 덤프용
읽기 전용이고, 상태 전이는 한 줄도 하지 않는다.

**전송을 고르는 지점이 이 프로젝트에 여기 하나다:**

```cpp
server->transport  = std::make_shared<TcpRaftTransport>(peers);
server->blockcopy  = std::make_shared<TcpBlockCopyClient>(...);
```

RDMA로 바꿀 때 손댈 곳이 이 두 줄이고, `core/` 는 건드리지 않는다.

## 불변식

- **`apps/` 는 `-I` 경로에 없다.** 진입점은 아무 곳에서도 include되지
  않으므로 넣을 이유가 없다. 여기에 다른 곳에서 쓸 헤더를 만들지 말 것 —
  그건 `net/` 이나 `core/` 로 가야 한다.
- 파일을 추가하면 `CMakeLists.txt` 의 `add_executable` 과 `build.sh` 의
  `build_*` 함수 **양쪽**을 고쳐야 한다. 두 빌드 시스템은 병행 유지된다.
