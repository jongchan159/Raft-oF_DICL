#ifndef RAFT_BLOCKCOPY_SERVER_HPP
#define RAFT_BLOCKCOPY_SERVER_HPP

/* 페이지 크기만 쓴다. blockio/ 가 디바이스 기하값의 단일 출처이며,
 * 여기서 core/include/raft_constants.h 를 거치지 않으므로 **storage/ 는 core/ 를
 * 전혀 모른다** (의존 방향: net -> storage -> blockio). */
#include "block_geometry.h"

#include <cstdint>
#include <cstring>
#include <cstdio>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <atomic>
#include <stdexcept>
#include <algorithm>
#include <utility>
#include <climits>

#include <fcntl.h>
#include <unistd.h>
#include <time.h>

/* ============================================================
 * 스토리지(blockcopy) 노드의 서버 구현.
 *
 * 예전에는 core/ 에 있었는데, core/ 는 "Raft 알고리즘 자체"를 담는
 * 디렉터리이고 이 파일은 그것과 아무 관계가 없다 -- core/ 의 어느 파일도
 * 이걸 include하지 않으며(전수 확인), 쓰는 쪽은 net/ 와
 * raft_blockcopy_server 바이너리뿐이다. Go 원본 대응도 별도 프로그램
 * (~/RAFT/server_random)이다. 그래서 storage/ 로 분리했다.
 *
 * raft_constants.h에서 PAGE_SIZE만 쓴다 -- constexpr이므로
 * raft_constants.cpp 링크가 필요하지 않다 (이 바이너리는 core/를
 * 링크하지 않는다).
 * ============================================================ */

namespace nvmeof_raft {
namespace blockcopy {

/* O_DIRECT 버퍼 정렬. blockio가 소유하는 페이지 크기와 같은 값이다.
 * 예전에는 여기 4096 리터럴로 따로 정의돼 있었고, blockio 쪽 kAlign=512와
 * 이름은 같고 값은 다른 상태로 공존한 적이 있다. */
constexpr size_t kAlign = blockio::kPageSize;

int open_device(const std::string &path);

/* ============================================================
 * AlignedBufPool
 *
 * WritePBABatch 처리에 쓰는 O_DIRECT 정렬 버퍼 풀.
 *
 * 원래는 RPC마다 워커 수만큼 posix_memalign(max_size)를 새로 잡고 끝에
 * free했다. max_size는 MaxPBACopyChunkBytes(256MiB)까지 갈 수 있어서
 * -copy-workers 16이면 RPC 한 번에 최대 4GiB를 할당/해제하게 된다
 * (핫패스에서 page fault + munmap 폭풍). 풀에 넣어 재사용하면 정상
 * 상태에서는 할당이 0이 된다.
 *
 * acquire는 요청 크기보다 작은 버퍼를 받으면 키워서 돌려주므로,
 * 호출자는 항상 n바이트 이상을 보장받는다. 동시 RPC를 막지 않는다
 * (기존과 동일하게 여러 커넥션이 병렬로 처리될 수 있음).
 * ============================================================ */
class AlignedBufPool {
public:
    AlignedBufPool() = default;
    ~AlignedBufPool();
    AlignedBufPool(const AlignedBufPool &) = delete;
    AlignedBufPool &operator=(const AlignedBufPool &) = delete;

    /* 반환: {버퍼, 실제 capacity}. 실패 시 {nullptr, 0} */
    std::pair<void *, size_t> acquire(size_t n);

    void release(void *p, size_t cap);

private:
    std::mutex mu_;
    std::vector<std::pair<void *, size_t>> free_;
};

struct WritePBAReq { uint64_t pba_src, pba_dst, nbytes; int src_dev, dst_dev; };
struct WritePBARsp { std::string error; int64_t copy_nanos = 0; };

struct WritePBABatchReq {
    std::vector<uint64_t> pba_srcs, pba_dsts, nbytes;
    uint64_t block_size = 0;
    int src_dev = 0, dst_dev = 0;
};
/* copy_nanos 는 read_nanos + write_nanos 다. 방향별로도 따로 돌려주는 이유:
 * 로컬 PCIe 복사와 NVMe-oF attach 볼륨 복사의 지연 차이가 pread 쪽인지
 * pwrite 쪽인지는 합계만으로는 알 수 없다 (blkcopy 벤치가 묻는 질문이 정확히
 * 그것이다). 서버는 이미 total_read_ns / total_write_ns 를 따로 들고 있었고,
 * 합치기 전 값을 그대로 실어 보내는 것뿐이라 측정 비용은 0이다. */
struct WritePBABatchRsp {
    std::string error;
    int64_t copy_nanos = 0;
    int64_t read_nanos = 0, write_nanos = 0;
};

struct GetTimeRsp { uint64_t read_nanos = 0, write_nanos = 0, other_nanos = 0; };

/* ============================================================
 * BlockCopyServer
 *
 * pread/pwrite를 통해 데이터 블럭을 주소 단위로 복사하는 메서드가 담겨있는 클래스
 * ============================================================ */
class BlockCopyServer {
public:
    BlockCopyServer(std::vector<int> fds, std::vector<std::string> paths, int copy_workers);

    ~BlockCopyServer();

    WritePBARsp handle_write_pba(const WritePBAReq &req);

    WritePBABatchRsp handle_write_pba_batch(const WritePBABatchReq &req);

    GetTimeRsp handle_get_time();

    void handle_reset_time();

private:
    bool get_fd(int dev_idx, int &out_fd) const;

    std::vector<int> fds_;
    std::vector<std::string> paths_;
    int copy_workers_;

    AlignedBufPool buf_pool_;   /* WritePBABatch 워커 버퍼 재사용 풀 */

    std::mutex mu_;
    uint64_t read_ns_ = 0, write_ns_ = 0, other_ns_ = 0;
};

} /* namespace blockcopy */
} /* namespace nvmeof_raft */

#endif /* RAFT_BLOCKCOPY_SERVER_HPP */