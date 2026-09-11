# `net/` — 전송 계층 (TCP + protobuf)

`core/` 를 소켓에 붙이는 층. **나중에 RDMA로 교체될 부분이 여기다.**

`include/` 는 선언, `src/` 는 정의.

## 두 갈래로 나뉜다 (빌드가 이 구분에 의존한다)

| 그룹 | core 심볼 | 링크되는 바이너리 |
|---|---|---|
| `WIRE_SRCS` — 와이어 코덱 · HTTP CONNECT 리스너 · RPC 다이얼 · CLI 파서 · proto 변환 | 불필요 | **세 바이너리 전부** |
| `NET_SRCS` — `raft_tcp_server.cpp` 와 두 RPC 클라이언트 | 필요 (`server->apply()` 등을 부른다) | `raft_node` 전용 |

`raft_client` 와 `raft_blockcopy_server` 는 `core/` 를 링크하지 않는다.
그래서 core 심볼을 요구하는 파일이 `WIRE_SRCS` 로 새면 그 두 바이너리가
링크에 실패한다. **새 `.cpp` 를 추가할 때 어느 그룹인지 먼저 정할 것.**

## 불변식

- `raft_client` / `raft_blockcopy_server` 에 `nvmeof_raft::Server::` 심볼이
  하나도 없어야 한다. CTest `isolation_raft_client`,
  `isolation_raft_blockcopy_server` 가 강제한다.
- **`raft_statemachine_hash.h` 는 헤더-온리로 남아야 한다.** `.cpp` 를 만들면
  `raft_selftest` 가 `net/` 을 링크해야 하고, 그건 "selftest 는 core 만
  링크한다" 는 불변식을 깬다.
- `raft_proto_conv.h` 의 템플릿 2개(`commands_to_proto` /
  `commands_from_proto`)는 헤더에 남는다. 프로젝트에서 헤더 템플릿은 이 둘뿐이다.
- 전송 구현체(`TcpRaftTransport` / `TcpBlockCopyClient`)의 내부 락은
  **`Server::mu` 밖에서** 잡힌다 — DECISIONS.md D10.
- accept된 fd에서 `SO_RCVTIMEO` 를 반드시 지운다. Linux가 리슨 소켓의
  타임아웃을 상속시켜서, 안 지우면 롱리브드 RPC가 200ms마다 끊기고
  선거가 끝나지 않는다 — `[수정-3]`, DECISIONS.md D3.

## 알려진 결함

`TcpBlockCopyClient::write_pba_batch` 가 RPC 실패 시 커넥션 핸들을
무효화하지 않는다 → 스토리지 커넥션이 한 번 죽으면 이후 모든 PBA 복사가
영구히 실패한다. 고치는 방법까지 DECISIONS.md §U6 에 적혀 있다.
raft RPC 경로(`TcpRaftTransport::call_peer`)에는 이 무효화가 제대로 있다.
