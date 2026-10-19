#include "cached_fd.h"
#include "block_geometry.h"

#include <algorithm>
#include <cstring>
#include <cerrno>
#include <stdexcept>
#include <cstdlib>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

/* FIEMAP ioctl 관련 -- 실서버 리눅스 헤더 필요. 이 파일이 링크되는 시점의
 * 환경에 <linux/fs.h>, <linux/fiemap.h>가 있어야 함 (일반 리눅스 서버엔
 * 기본 존재). 이 채팅 환경에도 있는지 아래 컴파일 검증에서 확인. */
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/fiemap.h>

namespace nvmeof_raft {

namespace {
constexpr size_t kAlign = blockio::kSectorSize;   /* 원본 C.ALIGN 대응, 섹터 정렬 */

/* 링 메타데이터 파일 생성 모드. */
constexpr int kRingFileMode = 0644;

/* raft.go의 c_get_pba를 대체하는 순수 C++ 구현.
 * FIEMAP ioctl로 논리 오프셋 -> 물리 오프셋 변환 (원본 c_get_pba와 동일 로직) */
bool native_get_pba(int fd, int64_t logical, size_t length,
                     uint64_t *out_pba, size_t *out_len) {
    /* fiemap 구조체는 가변 길이(fm_extents[])라 버퍼를 직접 잡음 */
    constexpr int kMaxExtents = 1;
    size_t fm_size = sizeof(struct fiemap) + kMaxExtents * sizeof(struct fiemap_extent);
    std::vector<uint8_t> buf(fm_size, 0);
    auto *fm = reinterpret_cast<struct fiemap *>(buf.data());

    fm->fm_start = static_cast<uint64_t>(logical);
    fm->fm_length = length;
    fm->fm_flags = 0;
    fm->fm_extent_count = kMaxExtents;

    if (ioctl(fd, FS_IOC_FIEMAP, fm) < 0) {
        return false;
    }
    if (fm->fm_mapped_extents == 0) {
        return false;   /* hole -- 원본은 이 경우도 에러 처리 */
    }

    const struct fiemap_extent &fe = fm->fm_extents[0];
    /* 논리 오프셋이 이 extent 안에서 얼마나 떨어져 있는지 반영 */
    uint64_t intra = static_cast<uint64_t>(logical) - fe.fe_logical;
    *out_pba = fe.fe_physical + intra;
    *out_len = static_cast<size_t>(fe.fe_length - intra);
    return true;
}
} /* anonymous namespace */

/* ============================================================
 * ExtentCache
 * ============================================================ */

ExtentCache ExtentCache::build(int fd, int64_t file_size) {
    ExtentCache cache;
    int64_t off = 0;
    while (off < file_size) {
        uint64_t remaining = static_cast<uint64_t>(file_size - off);
        uint64_t pba = 0;
        size_t len = 0;
        if (!native_get_pba(fd, off, remaining, &pba, &len)) {
            throw std::runtime_error(
                "BuildExtentCache: FIEMAP failed at offset " + std::to_string(off));
        }
        if (pba == 0) {
            throw std::runtime_error(
                "BuildExtentCache: PBA=0 at offset " + std::to_string(off) + " (hole in file)");
        }
        if (len == 0) {
            throw std::runtime_error(
                "BuildExtentCache: zero-length extent at offset " + std::to_string(off));
        }
        cache.extents_.push_back(Extent{off, pba, len});
        off += static_cast<int64_t>(len);
    }
    return cache;
}

ExtentCache ExtentCache::from_extents(std::vector<Extent> extents) {
    ExtentCache cache;
    std::sort(extents.begin(), extents.end(),
              [](const Extent &a, const Extent &b) { return a.logical < b.logical; });
    cache.extents_ = std::move(extents);
    return cache;
}

PBASegment ExtentCache::lookup(int64_t logical, uint64_t length) const {
    /* raft.go: sort.Search로 "logical <= 요청 오프셋"인 마지막 extent 탐색 */
    auto it = std::upper_bound(
        extents_.begin(), extents_.end(), logical,
        [](int64_t val, const Extent &e) { return val < e.logical; });
    if (it == extents_.begin()) {
        throw std::runtime_error(
            "ExtentCache::lookup: no extent covers offset " + std::to_string(logical));
    }
    --it;
    int64_t extent_end = it->logical + static_cast<int64_t>(it->length);
    if (logical >= extent_end) {
        throw std::runtime_error(
            "ExtentCache::lookup: offset past extent");
    }
    uint64_t intra_offset = static_cast<uint64_t>(logical - it->logical);
    uint64_t pba = it->physical + intra_offset;
    uint64_t available = it->length - intra_offset;
    if (available > length) {
        available = length;
    }
    return PBASegment{pba, available};
}

std::pair<uint64_t, int> ExtentCache::remaining_at(int64_t logical, int hint) const {
    /* Fast path: hint 먼저 확인 (순차 접근 패턴, persistCircular의 흔한 경우) */
    if (hint >= 0 && static_cast<size_t>(hint) < extents_.size()) {
        const Extent &e = extents_[static_cast<size_t>(hint)];
        if (logical >= e.logical && logical < e.logical + static_cast<int64_t>(e.length)) {
            return {e.length - static_cast<uint64_t>(logical - e.logical), hint};
        }
        if (static_cast<size_t>(hint + 1) < extents_.size()) {
            const Extent &e2 = extents_[static_cast<size_t>(hint + 1)];
            if (logical >= e2.logical && logical < e2.logical + static_cast<int64_t>(e2.length)) {
                return {e2.length - static_cast<uint64_t>(logical - e2.logical), hint + 1};
            }
        }
    }
    /* Slow path: binary search */
    auto it = std::upper_bound(
        extents_.begin(), extents_.end(), logical,
        [](int64_t val, const Extent &e) { return val < e.logical; });
    if (it == extents_.begin()) {
        return {0, 0};
    }
    --it;
    int idx = static_cast<int>(std::distance(extents_.begin(), it));
    int64_t ext_end = it->logical + static_cast<int64_t>(it->length);
    if (logical >= ext_end) {
        return {0, idx};
    }
    return {it->length - static_cast<uint64_t>(logical - it->logical), idx};
}

/* ============================================================
 * create_ring_file
 * ============================================================ */
 
int64_t create_ring_file(const std::string &path, uint64_t size_bytes) {
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT, kRingFileMode);
    if (fd < 0) {
        throw std::runtime_error("create_ring_file: open " + path + ": " + strerror(errno));
    }
 
    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        int e = errno;
        ::close(fd);
        throw std::runtime_error("create_ring_file: fstat " + path + ": " + strerror(e));
    }
 
    const uint64_t old_size = static_cast<uint64_t>(st.st_size);
    if (old_size < size_bytes) {
        /* fallocate: FIEMAP이 extent를 돌려주려면 블록이 실제로 할당돼
         * 있어야 한다 (sparse hole이면 BuildExtentCache가 던진다).
         * 기존 내용은 보존하면서 뒤쪽만 채운다. */
        if (::fallocate(fd, 0, static_cast<off_t>(st.st_size),
                        static_cast<off_t>(size_bytes - static_cast<uint64_t>(st.st_size))) != 0) {
            int e = errno;
            ::close(fd);
            throw std::runtime_error("create_ring_file: fallocate " + path + " to " +
                std::to_string(size_bytes) + " bytes: " + strerror(e));
        }
        if (::fdatasync(fd) != 0) {
            int e = errno;
            ::close(fd);
            throw std::runtime_error("create_ring_file: fdatasync " + path + ": " + strerror(e));
        }
    }
 
    /* ZERO_RANGE 는 **새로 늘린 구간에만** 건다.
     *
     * 원래는 파일 전체에 매 시작마다 걸었고 주석은 "extent 를 전부 written 으로
     * 만든다"고 했지만, ext4 에서 ZERO_RANGE 는 반대로 written extent 를
     * **unwritten 으로 되돌리고 데이터를 0 으로 만든다**(이 환경에서 확인).
     * 그러면 이미 precondition 을 마친 파일을 재사용해도 시작할 때마다 전부
     * 다시 써야 한다. written 상태로 만드는 일은 precondition_ring_file 이
     * 한다. [수정-9] 의 목적(첫 쓰기 때 extent 상태 변환 방지)은 그쪽이 맡는다. */
#ifndef FALLOC_FL_KEEP_SIZE
#define FALLOC_FL_KEEP_SIZE 0x01
#endif
#ifndef FALLOC_FL_ZERO_RANGE
#define FALLOC_FL_ZERO_RANGE 0x10
#endif
    if (old_size < size_bytes) {
        if (::fallocate(fd, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
                        static_cast<off_t>(old_size),
                        static_cast<off_t>(size_bytes - old_size)) != 0) {
            int e = errno;
            ::close(fd);
            throw std::runtime_error(
                "create_ring_file: fallocate(ZERO_RANGE) on " + path + ": " +
                strerror(e) + " -- the filesystem must support "
                "FALLOC_FL_ZERO_RANGE (ext4/xfs/btrfs)");
        } else if (::fdatasync(fd) != 0) {
            int e = errno;
            ::close(fd);
            throw std::runtime_error("create_ring_file: fdatasync after ZERO_RANGE " +
                                      path + ": " + strerror(e));
        }
    }
 
    /* 헤더(offset 0, 한 섹터)는 시작할 때마다 0 으로 되돌린다.
     * 예전에는 전체 ZERO_RANGE 가 헤더까지 0 으로 만들었으므로 그 동작을
     * 명시적으로 유지한다 (term/votedFor 는 항상 0 에서 시작). 이미 쓰인 링 본문은
     * 보존한다. */
    {
        static const char kZeroHeader[blockio::kSectorSize] = {0};
        ssize_t w;
        do {
            w = ::pwrite(fd, kZeroHeader, sizeof(kZeroHeader), 0);
        } while (w < 0 && errno == EINTR);
        if (w != static_cast<ssize_t>(sizeof(kZeroHeader))) {
            int e = errno;
            ::close(fd);
            throw std::runtime_error("create_ring_file: zero header " + path + ": " +
                                      strerror(e));
        }
        if (::fdatasync(fd) != 0) {
            int e = errno;
            ::close(fd);
            throw std::runtime_error("create_ring_file: fdatasync after header " +
                                      path + ": " + strerror(e));
        }
    }
 
    if (::fstat(fd, &st) != 0) {
        int e = errno;
        ::close(fd);
        throw std::runtime_error("create_ring_file: fstat(2) " + path + ": " + strerror(e));
    }
    ::close(fd);
    return st.st_size;
}

bool find_unwritten_ranges(int fd, uint64_t size_bytes,
                           std::vector<std::pair<uint64_t, uint64_t>> *out) {
    out->clear();
    constexpr uint32_t kBatch = 256;
    constexpr uint64_t kAlign = blockio::kPageSize;
 
    std::vector<uint8_t> buf(sizeof(struct fiemap) + kBatch * sizeof(struct fiemap_extent));
    auto *fm = reinterpret_cast<struct fiemap *>(buf.data());
 
    auto add = [&](uint64_t begin, uint64_t end) {
        if (end > size_bytes) end = size_bytes;
        begin = begin / kAlign * kAlign;
        end = (end + kAlign - 1) / kAlign * kAlign;
        if (end > size_bytes) end = size_bytes;
        if (begin >= end) return;
        if (!out->empty()) {
            auto &last = out->back();
            const uint64_t last_end = last.first + last.second;
            if (begin <= last_end) {                 /* 겹치거나 맞닿으면 합친다 */
                last.second = std::max<uint64_t>(last_end, end) - last.first;
                return;
            }
        }
        out->emplace_back(begin, end - begin);
    };
 
    uint64_t cursor = 0;                             /* 지금까지 확인한 논리 오프셋 */
    bool last_seen = false;
    while (cursor < size_bytes && !last_seen) {
        const uint64_t before = cursor;
        std::memset(buf.data(), 0, buf.size());
        fm->fm_start = cursor;
        fm->fm_length = size_bytes - cursor;
        fm->fm_flags = 0;
        fm->fm_extent_count = kBatch;
        if (::ioctl(fd, FS_IOC_FIEMAP, fm) < 0) {
            out->clear();
            return false;
        }
        if (fm->fm_mapped_extents == 0) {
            break;                                   /* 남은 구간은 hole -> 아래에서 추가 */
        }
        for (uint32_t i = 0; i < fm->fm_mapped_extents; i++) {
            const struct fiemap_extent &e = fm->fm_extents[i];
            if (e.fe_logical > cursor) {
                add(cursor, e.fe_logical);           /* extent 사이의 hole */
            }
            if (e.fe_flags & (FIEMAP_EXTENT_UNWRITTEN | FIEMAP_EXTENT_DELALLOC)) {
                add(e.fe_logical, e.fe_logical + e.fe_length);
            }
            cursor = std::max<uint64_t>(cursor, e.fe_logical + e.fe_length);
            if (e.fe_flags & FIEMAP_EXTENT_LAST) {
                last_seen = true;
                break;
            }
        }
        if (cursor == before) {
            break;                                   /* 진전이 없으면 나머지를 안 쓴 것으로 본다 */
        }
    }
    if (cursor < size_bytes) {
        add(cursor, size_bytes);                     /* 마지막 extent 뒤의 hole */
    }
    return true;
}

uint64_t precondition_ring_file(const std::string &path,
                                uint64_t size_bytes) {
    if (size_bytes == 0) {
        return 0;
    }
 
    constexpr size_t kIoAlign = blockio::kPageSize;
    constexpr size_t kChunkBytes = 16ull * 1024 * 1024;
 
    if ((size_bytes % kIoAlign) != 0) {
        throw std::runtime_error(
            "precondition_ring_file: size is not page-aligned: " +
            std::to_string(size_bytes));
    }
 
    int fd = ::open(path.c_str(), O_RDWR | O_DIRECT);
    if (fd < 0) {
        throw std::runtime_error(
            "precondition_ring_file: open " + path + ": " +
            strerror(errno));
    }
 
    /* 아직 쓰지 않은 구간만 쓴다. FIEMAP 을 쓸 수 없으면 전체를 쓴다. */
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    if (!find_unwritten_ranges(fd, size_bytes, &ranges)) {
        ranges.assign(1, std::pair<uint64_t, uint64_t>(0, size_bytes));
    }
    if (ranges.empty()) {
        ::close(fd);
        return 0;
    }
 
    void *buf = nullptr;
    if (::posix_memalign(&buf, kIoAlign, kChunkBytes) != 0) {
        ::close(fd);
        throw std::runtime_error(
            "precondition_ring_file: posix_memalign failed");
    }
 
    std::memset(buf, 0, kChunkBytes);
 
    uint64_t written = 0;
    for (const auto &r : ranges) {
        uint64_t off = r.first;
        const uint64_t end = r.first + r.second;
        while (off < end) {
            const size_t len = static_cast<size_t>(
                std::min<uint64_t>(kChunkBytes, end - off));
 
            ssize_t n;
            do {
                n = ::pwrite(fd, buf, len, static_cast<off_t>(off));
            } while (n < 0 && errno == EINTR);
 
            if (n < 0 || static_cast<size_t>(n) != len) {
                ::free(buf);
                ::close(fd);
                throw std::runtime_error(
                    "precondition_ring_file: pwrite failed");
            }
 
            off += len;
            written += len;
        }
    }
 
    if (::fdatasync(fd) != 0) {
        const int e = errno;
        ::free(buf);
        ::close(fd);
        throw std::runtime_error(
            "precondition_ring_file: fdatasync: " +
            std::string(strerror(e)));
    }
 
    ::free(buf);
    ::close(fd);
    return written;
}

/* ============================================================
 * CachedFD
 * ============================================================ */

CachedFD CachedFD::open(const std::string &meta_path, const std::string &device_path) {
    CachedFD c;

    c.meta_fd_ = ::open(meta_path.c_str(), O_RDONLY);
    if (c.meta_fd_ < 0) {
        throw std::runtime_error("OpenCachedFD: open meta " + meta_path + ": " + strerror(errno));
    }

    /* File-fd O_DIRECT: WriteAtFile이 이걸 써서 리더 persist가 goraft의
     * WriteAt(O_DIRECT) 형태와 정확히 같아지도록 함 (원본 주석 그대로) */
    c.meta_wr_fd_ = ::open(meta_path.c_str(), O_RDWR | O_DIRECT);
    if (c.meta_wr_fd_ < 0) {
        ::close(c.meta_fd_);
        throw std::runtime_error("OpenCachedFD: open meta(wr) " + meta_path + ": " + strerror(errno));
    }

    c.dev_wr_fd_ = ::open(device_path.c_str(), O_RDWR | O_DIRECT);
    if (c.dev_wr_fd_ < 0) {
        ::close(c.meta_fd_);
        ::close(c.meta_wr_fd_);
        throw std::runtime_error("OpenCachedFD: open device(wr) " + device_path + ": " + strerror(errno));
    }

    c.dev_rd_fd_ = ::open(device_path.c_str(), O_RDONLY | O_DIRECT);
    if (c.dev_rd_fd_ < 0) {
        ::close(c.meta_fd_);
        ::close(c.meta_wr_fd_);
        ::close(c.dev_wr_fd_);
        throw std::runtime_error("OpenCachedFD: open device(rd) " + device_path + ": " + strerror(errno));
    }

    /* 2MB 정렬 write buffer 미리 할당 (원본: "covers most batch sizes") */
    constexpr size_t kDefaultWrBufSize = 2 * 1024 * 1024;
    if (posix_memalign(&c.wr_buf_, kAlign, kDefaultWrBufSize) != 0) {
        c.close_all();
        throw std::runtime_error("OpenCachedFD: posix_memalign write buffer failed");
    }
    c.wr_buf_size_ = kDefaultWrBufSize;

    return c;
}

CachedFD::~CachedFD() {
    close_all();
}

void CachedFD::close_all() {
    if (meta_fd_ >= 0) { ::close(meta_fd_); meta_fd_ = -1; }
    if (meta_wr_fd_ >= 0) { ::close(meta_wr_fd_); meta_wr_fd_ = -1; }
    if (dev_wr_fd_ >= 0) { ::close(dev_wr_fd_); dev_wr_fd_ = -1; }
    if (dev_rd_fd_ >= 0) { ::close(dev_rd_fd_); dev_rd_fd_ = -1; }
    if (wr_buf_ != nullptr) { free(wr_buf_); wr_buf_ = nullptr; }
}

CachedFD::CachedFD(CachedFD &&other) noexcept
    : meta_fd_(other.meta_fd_), meta_wr_fd_(other.meta_wr_fd_),
      dev_wr_fd_(other.dev_wr_fd_), dev_rd_fd_(other.dev_rd_fd_),
      has_extent_cache_(other.has_extent_cache_),
      extent_cache_(std::move(other.extent_cache_)),
      wr_buf_(other.wr_buf_), wr_buf_size_(other.wr_buf_size_) {
    other.meta_fd_ = other.meta_wr_fd_ = other.dev_wr_fd_ = other.dev_rd_fd_ = -1;
    other.wr_buf_ = nullptr;
}

CachedFD &CachedFD::operator=(CachedFD &&other) noexcept {
    if (this != &other) {
        close_all();
        meta_fd_ = other.meta_fd_;
        meta_wr_fd_ = other.meta_wr_fd_;
        dev_wr_fd_ = other.dev_wr_fd_;
        dev_rd_fd_ = other.dev_rd_fd_;
        has_extent_cache_ = other.has_extent_cache_;
        extent_cache_ = std::move(other.extent_cache_);
        wr_buf_ = other.wr_buf_;
        wr_buf_size_ = other.wr_buf_size_;
        other.meta_fd_ = other.meta_wr_fd_ = other.dev_wr_fd_ = other.dev_rd_fd_ = -1;
        other.wr_buf_ = nullptr;
    }
    return *this;
}

void CachedFD::write_at_file(const void *data, size_t len, int64_t offset) {
    ssize_t n = ::pwrite(meta_wr_fd_, data, len, offset);
    if (n < 0) {
        throw std::runtime_error("CachedFD::write_at_file: pwrite at " +
            std::to_string(offset) + " (" + std::to_string(len) +
            " bytes): " + strerror(errno));
    }
    if (static_cast<size_t>(n) != len) {
        throw std::runtime_error("CachedFD::write_at_file: short pwrite at " +
            std::to_string(offset) + ": " + std::to_string(n) + "/" +
            std::to_string(len));
    }
}

std::vector<uint8_t> CachedFD::read_at_file(size_t len, int64_t offset) const {
    std::vector<uint8_t> buf(len, 0);
    ssize_t n = ::pread(meta_fd_, buf.data(), len, offset);
    if (n < 0) {
        throw std::runtime_error("CachedFD::read_at_file: pread at " +
            std::to_string(offset) + ": " + strerror(errno));
    }
    if (static_cast<size_t>(n) != len) {
        throw std::runtime_error("CachedFD::read_at_file: short pread at " +
            std::to_string(offset) + ": " + std::to_string(n) + "/" + std::to_string(len));
    }
    return buf;
}

void CachedFD::fdatasync() {
    if (::fdatasync(meta_wr_fd_) != 0) {
        throw std::runtime_error(std::string("CachedFD::fdatasync: ") + strerror(errno));
    }
}

void CachedFD::cache_extents(int64_t file_size) {
    extent_cache_ = ExtentCache::build(meta_fd_, file_size);
    has_extent_cache_ = true;
}

PBASegment CachedFD::get_pba(int64_t logical, uint64_t length) const {
    if (has_extent_cache_) {
        return extent_cache_.lookup(logical, length);
    }
    /* Fallback: FIEMAP ioctl (CacheExtents 호출 전) */
    uint64_t pba = 0;
    size_t len = 0;
    if (!native_get_pba(meta_fd_, logical, length, &pba, &len)) {
        throw std::runtime_error("CachedFD::get_pba: FIEMAP failed (logical=" +
            std::to_string(logical) + ", len=" + std::to_string(length) + ")");
    }
    return PBASegment{pba, len};
}

void CachedFD::write(uint64_t pba, const std::vector<uint8_t> &data) {
    /* 정렬된 경우만 지원한다. pba와 길이가 모두 kAlign(512B) 배수면 미리
     * 잡아둔 wr_buf_로 O_DIRECT pwrite를 한 번 한다.
     *
     * 정렬되지 않은 경우는 **구현하지 않고 예외를 던진다.** 원본 raft.go는
     * c_direct_write로 별도 버퍼를 잡아 read-modify-write를 하지만, 현재
     * 호출부(persist_circular)는 항상 섹터 정렬된 범위만 쓰기 때문에 그
     * 경로에 도달하지 않는다. 조용히 잘못 쓰는 것보다 던지는 편이 낫다. */
    if (data.empty()) return;

    size_t n = data.size();
    bool aligned = (pba % kAlign == 0) && (n % kAlign == 0);

    if (aligned && n <= wr_buf_size_ && wr_buf_ != nullptr) {
        std::memcpy(wr_buf_, data.data(), n);
        ssize_t written = ::pwrite(dev_wr_fd_, wr_buf_, n, static_cast<off_t>(pba));
        if (written < 0 || static_cast<size_t>(written) != n) {
            throw std::runtime_error("CachedFD::write: pwrite failed (pba=0x" +
                std::to_string(pba) + ", nbytes=" + std::to_string(n) + ")");
        }
        return;
    }

    throw std::runtime_error(
        "CachedFD::write: unaligned/oversized slow path not yet ported "
        "(pba=" + std::to_string(pba) + ", nbytes=" + std::to_string(n) + ")");
}

std::vector<uint8_t> CachedFD::read(uint64_t pba, uint64_t nbytes) const {
    size_t aligned_len = (nbytes + kAlign - 1) / kAlign * kAlign;
    void *buf = nullptr;
    if (posix_memalign(&buf, kAlign, aligned_len) != 0) {
        throw std::runtime_error("CachedFD::read: posix_memalign failed");
    }
    ssize_t r = ::pread(dev_rd_fd_, buf, aligned_len, static_cast<off_t>(pba));
    if (r < 0 || static_cast<size_t>(r) != aligned_len) {
        free(buf);
        throw std::runtime_error("CachedFD::read: pread at 0x" +
            std::to_string(pba) + " returned " + std::to_string(r));
    }
    std::vector<uint8_t> result(static_cast<uint8_t *>(buf),
                                 static_cast<uint8_t *>(buf) + aligned_len);
    free(buf);
    return result;
}

} /* namespace nvmeof_raft */