#ifndef BLOCKIO_CACHED_FD_H
#define BLOCKIO_CACHED_FD_H

#include <cstdint>
#include <cstddef>
#include <vector>
#include <string>
#include <stdexcept>
#include <utility>

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
 * 이미 존재하면 파일을 재사용하고 이미 있던 구간은 **건드리지 않는다**
 * (헤더, 링 내용, extent 위치와 written/unwritten 상태가 그대로 남는다).
 * 크기가 모자라면 모자란 뒤쪽만 fallocate로 늘린다.
 *
 * 원본 restoreCircular은 매 기동마다 파일 전체에 FALLOC_FL_ZERO_RANGE를
 * 걸어 모든 extent를 "written" 상태로 뒤집는다. 원본 주석: "unwritten
 * extents cause FIEMAP to report FIEMAP_EXTENT_UNWRITTEN and serialize
 * later extent-state conversions through the filesystem journal.
 * Metadata-only on ext4/xfs/btrfs, idempotent, so safe to run on every
 * startup." 실패하면 예외를 낸다 -- FIEMAP 경로에서 성능/정확성에 직결된다.
 *
 * [수정-18] 이 포팅은 **방금 늘린 구간에만** 손댄다 (새 파일이면 전체).
 * ext4에서 ZERO_RANGE는 extent를 written이 아니라 unwritten으로 만들기
 * 때문에(DECISIONS.md U11), 매 기동마다 전체에 걸면 헤더를 지우고 이전
 * 실행이 써서 written이 된 extent도 되돌린다 -- DECISIONS.md D18
 *
 * [수정-19] 늘린 구간은 먼저 FALLOC_FL_WRITE_ZEROES로 할당한다. 0이 쓰인
 * **written** extent가 되므로 원본이 ZERO_RANGE로 얻으려던 상태를 실제로
 * 얻는다. 파일시스템이나 디바이스가 지원하지 않으면(EOPNOTSUPP/EINVAL)
 * 예전 경로(fallocate + ZERO_RANGE)로 폴백한다 -- DECISIONS.md D19
 *
 * 반환: RingFileResult (파일 크기와, 늘린 구간을 어떻게 할당했는지).
 * ============================================================ */

/* create_ring_file이 이번 호출에서 늘린 구간을 어떻게 할당했는가 */
enum class RingAlloc {
    None,          /* 크기가 충분한 기존 파일 -- 아무것도 하지 않았다 */
    WriteZeroes,   /* FALLOC_FL_WRITE_ZEROES -- written extent */
    ZeroRange,     /* WRITE_ZEROES 미지원 -> fallocate + ZERO_RANGE (ext4에선 unwritten, U11) */
};

struct RingFileResult {
    int64_t size = 0;                  /* 파일 크기(바이트) */
    RingAlloc alloc = RingAlloc::None;
    int write_zeroes_errno = 0;        /* alloc == ZeroRange일 때 WRITE_ZEROES가 돌려준 errno */
};

RingFileResult create_ring_file(const std::string &path, uint64_t size_bytes);

/* ============================================================
 * extent (raft.go 원본, 소문자 비공개 타입)
 *
 * 링 파일의 논리 구간 하나가 디바이스의 어느 물리 구간에 있는가.
 *
 * push 모드에서 팔로워가 자기 링의 extent 목록을 AE 응답에 그대로 싣는다
 * (core/include/raft_entry.h, DECISIONS.md D20). 그래서 raft_entry.h가 이
 * 헤더를 include한다. 예전에는 core TU가 이 헤더 전체를 파싱하지 않도록 이
 * 구조체만 blockio/extent.h로 떼어 두었는데, 2026-10-27에 여기로 합쳤다.
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

    /* from_extents: extent 목록을 직접 주어 맵을 만든다.
     *
     * 원본에는 없다. 실제 FIEMAP 없이 **단편화된(다중 extent) 파일**을
     * 재현할 유일한 방법이어서 추가했다 -- persist_circular의 extent-skip
     * 경로와 append_entries의 extent clamp 경로를 유닛 테스트로 덮는 데 쓴다.
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

    /* mapping_hash: "논리 오프셋 -> 물리 주소" 매핑 자체의 해시 (FNV-1a 64).
     *
     * 원본에는 없다. push 모드에서 리더가 받아 둔 팔로워 맵이 아직 유효한지
     * 확인하는 데 쓴다 (DECISIONS.md D20). 논리·물리 주소가 둘 다 이어지는
     * 인접 extent를 합친 뒤 해시하므로, extent 상태 변환 등으로 레코드만
     * 쪼개지고 매핑이 같으면 해시도 같다. */
    uint64_t mapping_hash() const;

    /* 담고 있는 extent 목록 (logical 오름차순). push 모드 팔로워가 자기
     * 맵을 리더에게 보낼 때 쓴다. */
    const std::vector<Extent> &extents() const { return extents_; }

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

    /* open_device: 링 파일 없이 **장치만** 연다 (push 모드, DECISIONS.md D20).
     *
     * 리더가 push 대상 팔로워의 볼륨에 쓸 때 쓴다. 팔로워의 링 파일은 팔로워
     * 호스트의 ext4 위에 있고, 리더 호스트가 그 ext4를 같이 마운트할 수는
     * 없다. 그래서 파일이 아니라 장치에 팔로워 맵으로 구한 PBA로 쓴다.
     *
     * dev_wr_fd_ 하나만 연다. 메타 파일 fd 둘과 dev_rd_fd_는 -1로 남으므로
     * write_at_file / read_at_file / fdatasync / cache_extents / read는
     * 예외를 던진다. get_pba는 팔로워 맵을 넣은 뒤에만 쓸 수 있다.
     * 블록 장치가 아니면(S_ISBLK) 거절한다 -- 경로를 잘못 주면 다른 파일을
     * 조용히 덮어쓰게 되기 때문이다. */
    static CachedFD open_device(const std::string &device_path);

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
    /* 캐시한 extent 맵의 mapping_hash. 캐시가 없으면 0. */
    uint64_t extent_map_hash() const {
        return has_extent_cache_ ? extent_cache_.mapping_hash() : 0;
    }
    /* 캐시한 extent 목록의 사본. 캐시가 없으면 빈 목록. */
    std::vector<Extent> extents() const {
        return has_extent_cache_ ? extent_cache_.extents() : std::vector<Extent>{};
    }
    /* set_extents: FIEMAP 대신 주어진 맵을 쓴다. push 모드 리더가 팔로워에게서
     * 받아 검증한 맵을 그 팔로워의 open_device 인스턴스에 넣는다 -- 이후
     * get_pba와 write_at_logical이 이 맵을 쓴다 (DECISIONS.md D20). */
    void set_extents(ExtentCache cache) {
        extent_cache_ = std::move(cache);
        has_extent_cache_ = true;
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

    /* write_at_logical: 링 파일의 논리 오프셋을 이 인스턴스의 extent 맵으로
     * PBA로 바꿔 장치(dev_wr_fd_)에 직접 쓴다. extent 경계에서 쪼갠다.
     * push 모드 리더가 팔로워 링에 쓰는 경로다 -- 자기 링에 쓰는
     * write_at_file과 같은 (buf, len, 논리 오프셋) 모양이다 (DECISIONS.md D20).
     * O_DIRECT라 data/offset/len이 512B 정렬이어야 한다. extent 맵이 없거나
     * 범위가 맵 밖이면 예외를 던진다. */
    void write_at_logical(const void *data, size_t len, int64_t offset);
    /* fdatasync_device: dev_wr_fd_를 flush한다 (블록 장치의 캐시 flush). */
    void fdatasync_device();

private:
    CachedFD() = default;

    int meta_fd_ = -1;      /* metadata file fd (O_RDONLY), FIEMAP ioctl용 */
    int meta_wr_fd_ = -1;   /* metadata file fd (O_RDWR|O_DIRECT), WriteAtFile용 */
    int dev_wr_fd_ = -1;    /* device fd (O_RDWR|O_DIRECT), direct-PBA write
                               (open_device 인스턴스는 이것만 연다) */
    int dev_rd_fd_ = -1;    /* device fd (O_RDONLY|O_DIRECT) */

    bool has_extent_cache_ = false;
    ExtentCache extent_cache_;

    void *wr_buf_ = nullptr;   /* 미리 할당된 ALIGN-정렬 write buffer */
    size_t wr_buf_size_ = 0;

    void close_all();
};

/* ============================================================
 * block_device_wwid: 블록 장치의 namespace 식별자
 * (/sys/class/block/<name>/wwid). 읽을 수 없으면 빈 문자열.
 *
 * 같은 NVMe-oF namespace는 어느 호스트에서 attach해도 wwid가 같다.
 * 그래서 리더 호스트에서 "멤버 5의 볼륨"으로 매핑한 장치가 정말 그
 * 볼륨인지, 호스트 5가 자기 볼륨에 대해 보고하는 wwid와 비교해 확인할
 * 수 있다 (push 모드, DECISIONS.md D20). 장치 이름(/dev/nvmeXnY)은
 * 호스트마다 달라서 그걸로는 확인할 수 없다.
 * ============================================================ */
std::string block_device_wwid(const std::string &dev_path);

} /* namespace nvmeof_raft */

#endif /* BLOCKIO_CACHED_FD_H */