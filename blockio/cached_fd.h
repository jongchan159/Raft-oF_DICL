#ifndef BLOCKIO_CACHED_FD_H
#define BLOCKIO_CACHED_FD_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <stdexcept>

namespace nvmeof_raft {

/* ============================================================
 * PBASegment (raft.go 원본 blockcopy 패키지)
 * "represents a contiguous physical block address region."
 * ============================================================ */
struct PBASegment {
    uint64_t pba = 0;   /* physical byte address from device start */
    uint64_t len = 0;   /* length in bytes */
};

/* ============================================================
 * create_ring_file: 링 메타데이터 파일을 만들고 fallocate로 블록을
 * 실제 할당한다. FIEMAP은 hole에 대해 extent를 돌려주지 않으므로
 * (BuildExtentCache가 "PBA=0 ... hole in file"로 던진다) sparse
 * ftruncate로는 안 되고 반드시 fallocate가 필요하다.
 *
 * 이미 존재하고 크기가 충분하면 그대로 둔다 (재시작 시 헤더/링 보존).
 *
 * require_zero_range: 원본 restoreCircular은 매 기동마다 파일 전체에
 * FALLOC_FL_ZERO_RANGE를 걸어 모든 extent를 "written" 상태로 뒤집는다.
 * 원본 주석: "unwritten extents cause FIEMAP to report
 * FIEMAP_EXTENT_UNWRITTEN and serialize later extent-state conversions
 * through the filesystem journal. Metadata-only on ext4/xfs/btrfs,
 * idempotent, so safe to run on every startup." FIEMAP 경로에서는 이게
 * 성능/정확성에 직결되므로 true로 주고 실패 시 예외를 낸다.
 * identity_pba 테스트 모드는 FIEMAP을 안 쓰므로 false를 주면 실패해도
 * 경고 없이 넘어간다 (tmpfs 등 ZERO_RANGE 미지원 환경 대비).
 *
 * 반환: 파일 크기(바이트).
 * ============================================================ */
int64_t create_ring_file(const std::string &path, uint64_t size_bytes,
                          bool require_zero_range = true);

/* ============================================================
 * extent (raft.go 원본, 소문자 비공개 타입)
 * ============================================================ */
struct Extent {
    int64_t logical = 0;     /* logical byte offset in the file */
    uint64_t physical = 0;   /* physical byte offset on the device */
    uint64_t length = 0;     /* bytes */
};

/* ============================================================
 * ExtentCache (raft.go 원본)
 * "holds all extents for a file, sorted by logical offset. Built once
 *  after fallocate; replaces per-call FIEMAP ioctls with a binary
 *  search (O(log n) where n = number of extents, typically 1-3)."
 * ============================================================ */
class ExtentCache {
public:
    /* BuildExtentCache (raft.go 원본, 팩토리 함수를 정적 멤버로) */
    static ExtentCache build(int fd, int64_t file_size);

    /* identity: "논리 오프셋 == 물리 오프셋"인 단일 extent 맵.
     * 링 메타데이터 파일 자체를 볼륨으로 취급하는 테스트 모드용
     * (블록 디바이스 + root 없이 PBA 복사 경로를 e2e로 돌리기 위함).
     * is_contiguous()가 true가 되므로 persist의 extent-skip 검사도
     * 실서버의 단일 extent 케이스와 같은 fast path를 탄다. */
    static ExtentCache identity(int64_t file_size);

    /* from_extents: extent 목록을 직접 주어 맵을 만든다.
     *
     * 원본에는 없다. FIEMAP 없이 **단편화된(다중 extent) 파일**을 재현할
     * 유일한 방법이어서 추가했다 -- identity()는 항상 단일 extent라
     * is_contiguous()가 true가 되고, 그러면 persist_circular의
     * extent-skip 경로와 append_entries의 extent clamp 경로가 한 번도
     * 실행되지 않는다 (HANDOFF §7-10이 테스트를 요구하는 지점).
     * 입력은 logical 오름차순으로 정렬해 보관한다. */
    static ExtentCache from_extents(std::vector<Extent> extents);

    /* Lookup (raft.go 원본)
     * "resolves a logical file offset to a PBASegment using the cached
     *  extent map. Returns the PBA and the number of contiguous bytes
     *  available from that PBA (clamped to the requested length)." */
    PBASegment lookup(int64_t logical, uint64_t length) const;

    /* NumExtents (raft.go 원본) */
    int num_extents() const { return static_cast<int>(extents_.size()); }

    /* IsContiguous (raft.go 원본)
     * "true if the file is a single extent (no fragmentation). When
     *  true, callers can skip per-entry extent boundary checks." */
    bool is_contiguous() const { return extents_.size() <= 1; }

    /* RemainingAt (raft.go 원본)
     * "returns how many bytes remain in the extent that covers the
     *  given logical offset, and the index of that extent. Pass the
     *  returned extIdx as hint to the next call for O(1) fast path
     *  when offsets advance sequentially." */
    std::pair<uint64_t, int> remaining_at(int64_t logical, int hint) const;

private:
    std::vector<Extent> extents_;   /* logical 오름차순 정렬됨 */
};

/* ============================================================
 * CachedFD (raft.go 원본 필드/함수 그대로)
 *
 * 링 메타데이터 파일과 대상 디바이스의 fd를 O_DIRECT로 열어 들고 있고,
 * FIEMAP으로 만든 extent 맵을 캐시해 논리 오프셋 -> 물리 주소(PBA) 변환을
 * O(log n) 이진탐색으로 처리한다 (원본은 호출마다 FIEMAP ioctl).
 *
 * 원본은 cgo로 c_get_pba / c_direct_pwrite 같은 C 헬퍼를 호출한다. 이
 * 포팅은 그 헬퍼들을 raft_cached_fd.cpp의 익명 namespace에 순수 C++로
 * 두었다 (별도 .c 파일로 분리하지 않았다 -- 링크 단위를 늘릴 이유가 없었다).
 * ============================================================ */
class CachedFD {
public:
    static CachedFD open(const std::string &meta_path, const std::string &device_path);
    ~CachedFD();

    CachedFD(const CachedFD &) = delete;
    CachedFD &operator=(const CachedFD &) = delete;
    CachedFD(CachedFD &&other) noexcept;
    CachedFD &operator=(CachedFD &&other) noexcept;

    /* WriteAtFile (raft.go 원본)
     * "writes data to the cached metadata file at the given logical
     *  byte offset using O_DIRECT (no page cache). Mirrors goraft's
     *  persist call shape... data and offset must be ALIGN-aligned."
     *
     * 정렬 보장을 위해 raw pointer+len을 받는다. std::vector 버전을
     * 쓰면 vector의 내부 malloc 버퍼가 posix_memalign 정렬을 못 지켜서
     * O_DIRECT pwrite가 EINVAL로 실패한다 (오늘 실제로 재현된 버그) */
    void write_at_file(const void *data, size_t len, int64_t offset);

    /* read_at_file: 메타데이터 파일의 논리 오프셋에서 읽는다.
     * write_at_file과 대칭인 복구용 경로 -- 헤더(offset 0)를 device PBA로
     * 읽으면, fallocate만 된 새 링 파일에서 device의 stale 바이트를
     * 헤더로 오인할 수 있다(파일 fd로 읽으면 unwritten extent가 0으로
     * 보인다). 시작 시 한 번만 쓰므로 O_DIRECT가 아닌 버퍼드 fd를 쓴다. */
    std::vector<uint8_t> read_at_file(size_t len, int64_t offset) const;

    /* Fdatasync (raft.go 원본)
     * "flushes outstanding writes on the metadata file fd... One
     *  fdatasync per persistCircular call provides the group-commit
     *  durability barrier that mirrors goraft." */
    void fdatasync();

    /* CacheExtents (raft.go 원본)
     * "walks the file's FIEMAP extents and caches them... Must be
     *  called after fallocate ensures all blocks are allocated." */
    void cache_extents(int64_t file_size);

    /* cache_identity_extents: FIEMAP 대신 identity 맵을 설치 (테스트 모드) */
    void cache_identity_extents(int64_t file_size);

    int extent_cache_info() const {
        return has_extent_cache_ ? extent_cache_.num_extents() : 0;
    }
    bool is_contiguous() const {
        return has_extent_cache_ && extent_cache_.is_contiguous();
    }
    std::pair<uint64_t, int> remaining_at(int64_t logical, int hint) const {
        if (!has_extent_cache_) {
            return {~uint64_t(0), 0};   /* "no cache, assume unlimited" (원본 그대로) */
        }
        return extent_cache_.remaining_at(logical, hint);
    }

    /* GetPBA (raft.go 원본)
     * "retrieves the physical block address for a logical offset. Uses
     *  the cached extent map if available (O(log n) binary search),
     *  otherwise falls back to a FIEMAP ioctl." */
    PBASegment get_pba(int64_t logical, uint64_t length) const;

    /* 디바이스에 O_DIRECT로 직접 읽고 쓴다 (pba는 디바이스 시작 기준
     * 물리 바이트 오프셋).
     * write: pba와 길이가 모두 512B 정렬이면 미리 잡아둔 정렬 버퍼로
     *   fast path를 탄다. 정렬되지 않은 경우는 **구현되어 있지 않고
     *   예외를 던진다** -- 현재 호출부(persist_circular)가 항상 정렬된
     *   범위만 쓰기 때문이다. */
    void write(uint64_t pba, const std::vector<uint8_t> &data);
    std::vector<uint8_t> read(uint64_t pba, uint64_t nbytes) const;

private:
    CachedFD() = default;

    int meta_fd_ = -1;      /* metadata file fd (O_RDONLY), FIEMAP ioctl용 */
    int meta_wr_fd_ = -1;   /* metadata file fd (O_RDWR|O_DIRECT), WriteAtFile용 */
    int dev_wr_fd_ = -1;    /* device fd (O_RDWR|O_DIRECT), legacy direct-PBA write */
    int dev_rd_fd_ = -1;    /* device fd (O_RDONLY|O_DIRECT) */

    bool has_extent_cache_ = false;
    ExtentCache extent_cache_;

    void *wr_buf_ = nullptr;   /* 미리 할당된 ALIGN-정렬 write buffer */
    size_t wr_buf_size_ = 0;

    void close_all();
};

} /* namespace nvmeof_raft */

#endif /* BLOCKIO_CACHED_FD_H */