/* ============================================================
 * ExtentCache 단위 테스트 -- extent 경계 clamp
 *
 * HANDOFF §7-10이 요구한 두 번째 지점이다. 지금까지 이 경로에 테스트가
 * 없었던 이유는 구조적이다: e2e에서 쓰던 extent 맵은
 * **항상 단일 extent**라 is_contiguous()가 true가 되고, 그러면
 * persist_circular의 extent-skip 검사와 append_entries_worker의
 * extent clamp 루프가 한 번도 실행되지 않는다. 즉 e2e 스모크를 몇 번
 * 돌려도 이 코드는 검증되지 않는다.
 *
 * ExtentCache::from_extents로 단편화된 파일을 합성해 그 경계 산술을
 * 직접 고정한다.
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_server.h"      /* slot_offset 선언이 여기 있다 */
#include "cached_fd.h"
#include "raft_constants.h"

using namespace nvmeof_raft;

TEST_CASE("단편화된 맵은 is_contiguous()가 false다") {
    ExtentCache c = ExtentCache::from_extents({
        Extent{0,     100000, 4096},
        Extent{4096,  900000, 4096},
    });
    CHECK(c.num_extents() == 2);
    CHECK(c.is_contiguous() == false);
}

TEST_CASE("remaining_at은 extent 경계에서 정확히 끊는다") {
    /* 논리 0..4095 -> 물리 100000.., 논리 4096..12287 -> 물리 900000..,
     * 논리 12288..16383 -> 물리 500000.. */
    ExtentCache c = ExtentCache::from_extents({
        Extent{0,     100000, 4096},
        Extent{4096,  900000, 8192},
        Extent{12288, 500000, 4096},
    });

    SUBCASE("extent 시작에서는 그 extent 전체가 남는다") {
        CHECK(c.remaining_at(0, -1).first == 4096);
        CHECK(c.remaining_at(4096, -1).first == 8192);
        CHECK(c.remaining_at(12288, -1).first == 4096);
    }

    SUBCASE("extent 중간에서는 그 extent의 끝까지만 남는다") {
        CHECK(c.remaining_at(512, -1).first == 4096 - 512);
        CHECK(c.remaining_at(4096 + 1024, -1).first == 8192 - 1024);
    }

    SUBCASE("경계 직전 1바이트") {
        CHECK(c.remaining_at(4095, -1).first == 1);
        CHECK(c.remaining_at(12287, -1).first == 1);
    }

    SUBCASE("extent 인덱스를 함께 돌려준다") {
        CHECK(c.remaining_at(0, -1).second == 0);
        CHECK(c.remaining_at(4096, -1).second == 1);
        CHECK(c.remaining_at(12288, -1).second == 2);
    }

    SUBCASE("파일 끝을 넘으면 0") {
        CHECK(c.remaining_at(16384, -1).first == 0);
        CHECK(c.remaining_at(99999, -1).first == 0);
    }
}

TEST_CASE("remaining_at의 hint fast path가 slow path와 같은 답을 준다") {
    ExtentCache c = ExtentCache::from_extents({
        Extent{0,     100000, 4096},
        Extent{4096,  900000, 8192},
        Extent{12288, 500000, 4096},
    });

    /* persist_circular는 오프셋이 순차로 전진하므로 직전 extIdx를 hint로
     * 넘겨 O(1) 경로를 탄다. hint가 맞을 때/한 칸 뒤일 때/틀릴 때 모두
     * binary search와 같은 답이어야 한다. */
    const int64_t offsets[] = {0, 512, 4095, 4096, 8000, 12287, 12288, 16000};
    for (int64_t off : offsets) {
        auto truth = c.remaining_at(off, /*hint=*/-1);
        for (int hint = -1; hint <= 3; hint++) {
            auto got = c.remaining_at(off, hint);
            CHECK(got.first == truth.first);
            /* 값이 맞으면 인덱스도 같아야 한다 (0바이트 케이스 제외) */
            if (truth.first != 0) {
                CHECK(got.second == truth.second);
            }
        }
    }
}

TEST_CASE("lookup은 요청 길이를 extent 경계로 clamp한다") {
    ExtentCache c = ExtentCache::from_extents({
        Extent{0,     100000, 4096},
        Extent{4096,  900000, 8192},
    });

    SUBCASE("extent 안에 들어가면 요청 그대로") {
        PBASegment seg = c.lookup(0, 1024);
        CHECK(seg.pba == 100000);
        CHECK(seg.len == 1024);
    }

    SUBCASE("extent를 넘어가면 경계까지만") {
        PBASegment seg = c.lookup(2048, 8192);
        CHECK(seg.pba == 100000 + 2048);
        CHECK(seg.len == 4096 - 2048);   /* 다음 extent로 넘어가지 않는다 */
    }

    SUBCASE("두 번째 extent의 물리 주소로 정확히 매핑한다") {
        PBASegment seg = c.lookup(4096 + 512, 1024);
        CHECK(seg.pba == 900000 + 512);
        CHECK(seg.len == 1024);
    }

    SUBCASE("파일 끝을 넘으면 예외") {
        CHECK_THROWS_AS(c.lookup(4096 + 8192, 512), std::runtime_error);
    }
}

TEST_CASE("from_extents는 입력을 logical 순으로 정렬한다") {
    /* 역순으로 넣어도 lookup이 올바르게 동작해야 한다 */
    ExtentCache c = ExtentCache::from_extents({
        Extent{8192, 300000, 4096},
        Extent{0,    100000, 4096},
        Extent{4096, 200000, 4096},
    });
    CHECK(c.lookup(0, 512).pba == 100000);
    CHECK(c.lookup(4096, 512).pba == 200000);
    CHECK(c.lookup(8192, 512).pba == 300000);
}

TEST_CASE("링 슬롯 오프셋이 실제 extent 경계와 맞물린다") {
    /* 링 파일은 헤더 512B(RING_OFFSET) 뒤부터 슬롯 0이다. 4KiB extent로
     * 단편화된 파일에서 슬롯 8(=오프셋 512+4096)은 두 번째 extent에 걸친다.
     * 이 계산이 어긋나면 PBA 복사가 남의 데이터를 읽는다. */
    ExtentCache c = ExtentCache::from_extents({
        Extent{0,    100000, 4096},
        Extent{4096, 900000, 4096},
    });

    CHECK(slot_offset(0) == static_cast<int64_t>(RING_OFFSET));

    /* 슬롯 0은 첫 extent 안. 헤더 512B를 뺀 만큼만 남는다. */
    auto r0 = c.remaining_at(slot_offset(0), -1);
    CHECK(r0.first == 4096 - RING_OFFSET);
    CHECK(r0.second == 0);

    /* 슬롯 7의 끝이 첫 extent 끝(4096)과 정확히 일치한다:
     * 512 + 7*512 = 4096 -> 이미 두 번째 extent */
    CHECK(slot_offset(7) == 4096);
    auto r7 = c.remaining_at(slot_offset(7), -1);
    CHECK(r7.second == 1);
    CHECK(r7.first == 4096);
}

/* push 모드에서 리더가 받아 둔 팔로워 맵이 아직 유효한지 이 해시로 본다
 * (DECISIONS.md D20). extent 레코드가 쪼개지기만 하고 매핑이 같으면 같은
 * 해시여야 하고 -- 아니면 멀쩡한 맵을 매번 다시 받는다 -- 물리 주소가
 * 하나라도 다르면 달라야 한다 -- 아니면 낡은 맵으로 남의 블록에 쓴다. */
TEST_CASE("mapping_hash: 쪼개진 레코드와 합친 레코드는 같은 매핑이면 같은 해시다") {
    ExtentCache merged = ExtentCache::from_extents({
        Extent{0,    100000, 8192},
        Extent{8192, 900000, 4096},
    });
    ExtentCache split = ExtentCache::from_extents({
        Extent{0,    100000, 4096},
        Extent{4096, 104096, 4096},   /* 논리·물리 둘 다 이어진다 -> 합쳐진다 */
        Extent{8192, 900000, 4096},
    });
    CHECK(merged.mapping_hash() == split.mapping_hash());
}

TEST_CASE("mapping_hash: 물리 주소가 다르면 해시가 다르다") {
    ExtentCache a = ExtentCache::from_extents({
        Extent{0,    100000, 4096},
        Extent{4096, 900000, 4096},
    });
    ExtentCache b = ExtentCache::from_extents({
        Extent{0,    100000, 4096},
        Extent{4096, 900512, 4096},
    });
    CHECK(a.mapping_hash() != b.mapping_hash());
}

TEST_CASE("mapping_hash: 논리로만 이어지고 물리로 떨어진 extent는 합치지 않는다") {
    /* 논리 0..8191 이 한 덩어리처럼 보여도 물리로는 떨어져 있다 --
     * 하나의 연속 extent{0, 100000, 8192} 와는 다른 매핑이다. */
    ExtentCache gap = ExtentCache::from_extents({
        Extent{0,    100000, 4096},
        Extent{4096, 500000, 4096},
    });
    ExtentCache contiguous = ExtentCache::from_extents({
        Extent{0, 100000, 8192},
    });
    CHECK(gap.mapping_hash() != contiguous.mapping_hash());
}
