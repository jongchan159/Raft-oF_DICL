# `storage/` — 스토리지(blockcopy) 노드 서버

**별도 프로세스다.** `raft_blockcopy_server` 바이너리의 몸통이고, Raft 노드와
같은 주소공간에서 돌지 않는다.

파일이 2개뿐이라 flat으로 둔다.

## 하는 일

Raft 노드가 보낸 `WritePBABatch` 요청을 받아, 지정된 물리 블록 주소 구간을
실제로 복사한다:

```
handle_write_pba_batch
  → 버퍼 풀 acquire (정렬된 버퍼 재사용)
  → copy_workers 개 std::thread
  → write_pba_copy_buf: pread(src_fd, ..., pba_src) + pwrite(dst_fd, ..., pba_dst)
  → join → copy_nanos (= read_nanos + write_nanos) 를 응답에 담아 보고
```

그 `copy_nanos` 가 리더의 `ApplyTimings` 에서 `StorageIO` 항이 된다. 현재
측정에서 Apply 지연의 대부분이 이 항이다 — DECISIONS.md §U7.

## 불변식

- **`core/` 를 링크하지 않고 include하지도 않는다.** `blockio/block_geometry.h`
  의 `kPageSize` 만 쓴다. 예전에는 `core/raft_constants.h` 를 include했는데
  실제로 필요한 건 그 상수 하나였다.
- `raft_blockcopy_server` 에 `nvmeof_raft::Server::` 심볼이 하나도 없어야
  한다. CTest `isolation_raft_blockcopy_server` 가 강제한다.
- 이 코드는 리팩토링 중 **의도적으로 손대지 않았다.** 성능 기준선 조사(§U7)의
  대조군이기 때문이다. 바꿀 때는 그 점을 감안할 것.
  **2026-09-07 이후로는 더 이상 순수 대조군이 아니다** — `AlignedBufPool::acquire`
  의 use-after-free 를 고쳤고(§U8), 응답에 `read_nanos`/`write_nanos` 를 추가했다.
  둘 다 복사 경로의 명령 수를 바꾸지 않지만, U7 비교를 할 때는 이 점을 밝힐 것.

## 이름공간 주의

`blockcopy::BlockCopyServer` 다. `nvmeof_raft::Server` 와 이름이 겹쳐서,
심볼 격리를 확인할 때 패턴을 `Server::` 로 쓰면 이쪽까지 잡힌다 —
반드시 `nvmeof_raft::Server::` 로 볼 것.
