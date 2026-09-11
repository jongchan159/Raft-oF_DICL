#include "raft_blockcopy_server.h"

#include <algorithm>
#include <climits>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <unistd.h>
#include <time.h>

/**
 *
 */

/* storage/raft_blockcopy_server.h 의 구현. 선언은 그 헤더를 볼 것.
 *
 * ns_diff / write_pba_copy_buf / write_pba_copy 는 이 파일 밖에서 쓰이지
 * 않으므로 익명 namespace 에 둔다 -- 예전에는 헤더에 inline 으로 노출돼 있어
 * 이 헤더를 include 하는 4개 TU 가 pread/pwrite 루프를 매번 컴파일했다. */

namespace nvmeof_raft {
namespace blockcopy {

namespace {

uint64_t ns_diff(const struct timespec &a, const struct timespec &b) {
    return static_cast<uint64_t>(b.tv_sec - a.tv_sec) * 1000000000ull +
           static_cast<uint64_t>(b.tv_nsec - a.tv_nsec);
}

int write_pba_copy_buf(int src_fd, int dst_fd,
                               int64_t pba_src, int64_t pba_dst,
                               int nbytes, void *buf,
                               uint64_t *read_ns, uint64_t *write_ns) {
    struct timespec t0{}, t1{};

    clock_gettime(CLOCK_MONOTONIC_RAW, &t0);
    ssize_t r = pread(src_fd, buf, static_cast<size_t>(nbytes), pba_src);
    clock_gettime(CLOCK_MONOTONIC_RAW, &t1);
    *read_ns = ns_diff(t0, t1);
    if (r != nbytes) {
        return -1;
    }

    clock_gettime(CLOCK_MONOTONIC_RAW, &t0);
    ssize_t w = pwrite(dst_fd, buf, static_cast<size_t>(nbytes), pba_dst);
    clock_gettime(CLOCK_MONOTONIC_RAW, &t1);
    *write_ns = ns_diff(t0, t1);
    if (w != nbytes) {
        return -1;
    }
    return 0;
}

int write_pba_copy(int src_fd, int dst_fd,
                           int64_t pba_src, int64_t pba_dst,
                           int nbytes, uint64_t *read_ns, uint64_t *write_ns) {
    void *buf = nullptr;
    if (posix_memalign(&buf, kAlign, static_cast<size_t>(nbytes)) != 0) {
        return -1;
    }
    int rc = write_pba_copy_buf(src_fd, dst_fd, pba_src, pba_dst, nbytes, buf,
                                 read_ns, write_ns);
    free(buf);
    return rc;
}

}  /* anonymous namespace */

int open_device(const std::string &path) {
    return ::open(path.c_str(), O_RDWR | O_DIRECT);
}

AlignedBufPool::~AlignedBufPool(){
        for (auto &b : free_) {
            free(b.first);
        }
    }

std::pair<void *, size_t> AlignedBufPool::acquire(size_t n){
        void *p = nullptr;
        size_t cap = 0;
        {
            std::lock_guard<std::mutex> lk(mu_);
            if (!free_.empty()) {
                p = free_.back().first;
                cap = free_.back().second;
                free_.pop_back();
            }
        }
        /* 풀에서 꺼낸 버퍼가 n바이트를 담을 수 있으면 그대로 쓰고, 작으면
         * 버린 뒤 아래에서 새로 잡는다. 이 else가 없던 시절에는 free(p) 후
         * 그 **해제된** 포인터를 옛 capacity와 함께 반환해서, 워커가 해제된
         * 메모리에 n바이트를 pread/pwrite했다 (use-after-free + heap overflow).
         * 청크 크기가 균일한 동안에는 cap < n 분기 자체가 안 잡혀 드러나지
         * 않고, 크기를 키워가며 호출하는 순간 터진다 -- blkcopy 벤치의
         * 청크 스윕이 정확히 그 순서다. */
        if (p != nullptr) {
            if (cap >= n) return {p, cap};
            else free(p);   /* 너무 작으면 버리고 새로 잡는다 */
        }

        void *np = nullptr;
        size_t ncap = (n + kAlign - 1) / kAlign * kAlign;
        if (posix_memalign(&np, kAlign, ncap) != 0) {
            return {nullptr, 0};
        }
        return {np, ncap};
    }

void AlignedBufPool::release(void *p, size_t cap){
        if (p == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lk(mu_);
        free_.push_back({p, cap});
    }

BlockCopyServer::BlockCopyServer(std::vector<int> fds, std::vector<std::string> paths,
                                 int copy_workers)
        : fds_(std::move(fds)), paths_(std::move(paths)),
          copy_workers_(copy_workers < 1 ? 1 : copy_workers) {}

BlockCopyServer::~BlockCopyServer(){
        for (int fd : fds_) {
            if (fd >= 0) {
                ::close(fd);
            }
        }
    }

WritePBARsp BlockCopyServer::handle_write_pba(const WritePBAReq &req){
        WritePBARsp rsp;
        int src_fd, dst_fd;
        if (!get_fd(req.src_dev, src_fd) || !get_fd(req.dst_dev, dst_fd)) {
            rsp.error = "device index out of range";
            return rsp;
        }

        uint64_t read_ns = 0, write_ns = 0;
        int rc = write_pba_copy(src_fd, dst_fd,
                                 static_cast<int64_t>(req.pba_src),
                                 static_cast<int64_t>(req.pba_dst),
                                 static_cast<int>(req.nbytes), &read_ns, &write_ns);
        if (rc != 0) {
            rsp.error = "write_pba_copy failed (src_dev=" + std::to_string(req.src_dev) +
                        " dst_dev=" + std::to_string(req.dst_dev) +
                        " nbytes=" + std::to_string(req.nbytes) + ")";
            return rsp;
        }

        rsp.copy_nanos = static_cast<int64_t>(read_ns + write_ns);
        std::lock_guard<std::mutex> lk(mu_);
        read_ns_ += read_ns;
        write_ns_ += write_ns;
        return rsp;
    }

WritePBABatchRsp BlockCopyServer::handle_write_pba_batch(const WritePBABatchReq &req){
        WritePBABatchRsp rsp;
        size_t count = req.pba_srcs.size();
        if (count == 0) {
            return rsp;
        }
        if (count != req.pba_dsts.size()) {
            rsp.error = "PbaSrcs and PbaDsts length mismatch";
            return rsp;
        }

        int src_fd, dst_fd;
        if (!get_fd(req.src_dev, src_fd) || !get_fd(req.dst_dev, dst_fd)) {
            rsp.error = "device index out of range";
            return rsp;
        }

        std::vector<uint64_t> sizes = req.nbytes;
        if (sizes.empty()) {
            if (req.block_size == 0) {
                rsp.error = "WritePBABatch: neither Nbytes nor BlockSize provided";
                return rsp;
            }
            sizes.assign(count, req.block_size);
        } else if (sizes.size() != count) {
            rsp.error = "Nbytes length " + std::to_string(sizes.size()) +
                        " != PbaSrcs length " + std::to_string(count);
            return rsp;
        }

        uint64_t max_size = 0;
        for (uint64_t sz : sizes) {
            max_size = std::max(max_size, sz);
        }
        if (max_size == 0) {
            rsp.error = "WritePBABatch: all entries have zero size";
            return rsp;
        }
        /* write_pba_copy_buf의 nbytes 파라미터가 int라서 2GiB 이상은
         * 조용히 음수로 잘린다. 현재 호출부(do_pba_copy)는
         * MaxPBACopyChunkBytes(256MiB)로 이미 clamp하므로 도달하지
         * 않지만, 그 상수를 올렸을 때 데이터가 조용히 깨지는 대신
         * 여기서 명시적으로 거절하게 한다. */
        if (max_size > static_cast<uint64_t>(INT_MAX)) {
            rsp.error = "WritePBABatch: chunk size " + std::to_string(max_size) +
                        " exceeds INT_MAX (lower MaxPBACopyChunkBytes)";
            return rsp;
        }

        int w = std::min(copy_workers_, static_cast<int>(count));
        if (w < 1) {
            w = 1;
        }

        /* 워커 버퍼는 풀에서 빌린다 (RPC마다 posix_memalign/free를 반복하면
         * max_size가 256MiB까지 갈 수 있어 -copy-workers 16에서 RPC당 최대
         * 4GiB를 할당/해제하게 된다). 정상 상태에서는 재사용되어 할당 0. */
        std::vector<void *> bufs(static_cast<size_t>(w), nullptr);
        std::vector<size_t> buf_caps(static_cast<size_t>(w), 0);
        bool alloc_failed = false;
        for (int i = 0; i < w; i++) {
            auto got = buf_pool_.acquire(static_cast<size_t>(max_size));
            if (got.first == nullptr) {
                alloc_failed = true;
                break;
            }
            bufs[static_cast<size_t>(i)] = got.first;
            buf_caps[static_cast<size_t>(i)] = got.second;
        }
        if (alloc_failed) {
            for (int i = 0; i < w; i++) {
                buf_pool_.release(bufs[static_cast<size_t>(i)], buf_caps[static_cast<size_t>(i)]);
            }
            rsp.error = "posix_memalign for worker buffer failed";
            return rsp;
        }

        std::mutex stats_mu;
        std::atomic<bool> has_error{false};
        std::string first_error;
        std::mutex err_mu;
        uint64_t total_read_ns = 0, total_write_ns = 0;

        std::vector<std::thread> workers;
        workers.reserve(static_cast<size_t>(w));
        for (int wid = 0; wid < w; wid++) {
            workers.emplace_back([&, wid]() {
                uint64_t local_read = 0, local_write = 0;
                for (size_t i = static_cast<size_t>(wid); i < count; i += static_cast<size_t>(w)) {
                    uint64_t r = 0, wr = 0;
                    int rc = write_pba_copy_buf(
                        src_fd, dst_fd,
                        static_cast<int64_t>(req.pba_srcs[i]),
                        static_cast<int64_t>(req.pba_dsts[i]),
                        static_cast<int>(sizes[i]), bufs[static_cast<size_t>(wid)],
                        &r, &wr);
                    if (rc != 0) {
                        bool expected = false;
                        if (has_error.compare_exchange_strong(expected, true)) {
                            std::lock_guard<std::mutex> lk(err_mu);
                            first_error = "worker " + std::to_string(wid) + " entry " +
                                std::to_string(i) + " failed";
                        }
                        return;
                    }
                    local_read += r;
                    local_write += wr;
                }
                std::lock_guard<std::mutex> lk(stats_mu);
                total_read_ns += local_read;
                total_write_ns += local_write;
            });
        }
        for (auto &t : workers) {
            t.join();
        }
        for (int i = 0; i < w; i++) {
            buf_pool_.release(bufs[static_cast<size_t>(i)], buf_caps[static_cast<size_t>(i)]);
        }

        if (has_error.load()) {
            rsp.error = first_error;
            return rsp;
        }

        rsp.read_nanos = static_cast<int64_t>(total_read_ns);
        rsp.write_nanos = static_cast<int64_t>(total_write_ns);
        rsp.copy_nanos = static_cast<int64_t>(total_read_ns + total_write_ns);
        std::lock_guard<std::mutex> lk(mu_);
        read_ns_ += total_read_ns;
        write_ns_ += total_write_ns;
        return rsp;
    }

GetTimeRsp BlockCopyServer::handle_get_time(){
        std::lock_guard<std::mutex> lk(mu_);
        return {read_ns_, write_ns_, other_ns_};
    }

void BlockCopyServer::handle_reset_time(){
        std::lock_guard<std::mutex> lk(mu_);
        read_ns_ = write_ns_ = other_ns_ = 0;
    }

bool BlockCopyServer::get_fd(int dev_idx, int &out_fd) const{
        if (dev_idx < 0 || static_cast<size_t>(dev_idx) >= fds_.size()) {
            return false;
        }
        out_fd = fds_[static_cast<size_t>(dev_idx)];
        return true;
    }

} /* namespace blockcopy */
} /* namespace nvmeof_raft */
