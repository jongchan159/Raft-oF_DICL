# `proto/` — 와이어 메시지 정의

`rpcproto.proto` 가 노드 사이, 그리고 노드와 스토리지 노드 사이 메시지의
**단일 출처**다.

## 생성된 코드는 커밋되어 있다

`rpcproto.pb.h` / `rpcproto.pb.cc` 를 저장소에 두는 이유: **`.proto` 를
수정하지 않는 한 `protoc` 설치 자체가 필요 없다.** 빌드는 커밋된 파일을
그대로 쓴다.

`.proto` 를 고쳤을 때만 재생성한다:

```bash
./build.sh regen-proto
# 또는 CMake:  cmake --build build-cmake --target regen-proto
```

`protoc` 이 없으면 두 빌드 모두 **실패시키지 않고 경고만** 낸다.

## 불변식

- **와이어 포맷은 리팩토링으로 바뀌지 않는다.** 이 프로젝트의 리팩토링
  제약이 "기능·성능·와이어 포맷 불변" 이다. 필드 번호를 재사용하거나
  의미를 바꾸면 구버전 노드와 섞였을 때 조용히 깨진다.
- 런타임 protobuf는 **3.21.0 이상**이 필요하다. 생성된 `.pb.h` 가 그 버전의
  런타임을 요구한다. Ubuntu 18.04 apt에는 3.0.0뿐이라 소스 빌드가
  필요하다 — 절차는 최상위 `README.md` §1.

## AppendEntries에 명령 바이트가 없다는 점

`AppendEntriesRequest` 는 `leader_pba_src`, `log_block_length`, `start_slot`,
그리고 term+cmd_len 만 담은 `entry_metas` 를 보낸다. **실제 명령 바이트는
이 메시지에 실리지 않는다** — 스토리지 노드가 PBA 복사로 옮긴다.
메시지를 고칠 때 이 설계를 깨지 않도록 주의할 것.
