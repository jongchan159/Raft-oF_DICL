# `cmake/` — CTest 헬퍼

## `check_absent.cmake`

바이너리에 특정 심볼 패턴이 **없다는 것**을 단정하는 스크립트다.
링크 격리 불변식 4개를 CTest 테스트로 만든다:

| 테스트 | 단정 |
|---|---|
| `isolation_raft_unit_tests` | protobuf 링크 0 |
| `isolation_raft_client` | `nvmeof_raft::Server::` 심볼 0 |
| `isolation_raft_blockcopy_server` | `nvmeof_raft::Server::` 심볼 0 |

이 네 성질은 전송을 추상 인터페이스로 바꿔서 얻은 것이고, 실수로
헤더 하나만 include해도 조용히 무너진다. 그래서 문서가 아니라
테스트로 못박아 뒀다.

## 패턴을 고칠 때

**심볼 패턴은 이름공간까지 붙여 쓸 것.** `Server::` 로 쓰면
`blockcopy::BlockCopyServer::` 까지 잡혀서 거짓 양성이 난다 (실제로
겪었다). `nvmeof_raft::Server::` 가 맞다.

새 단정을 추가했으면 **역으로도 확인할 것** — `raft_node` 를 대상으로
돌려서 실제로 실패하는지 봐야 테스트가 무의미하지 않다는 걸 안다.
