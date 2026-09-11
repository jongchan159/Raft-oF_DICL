/* ============================================================
 * ExtentCache 단위 테스트 -- extent 경계 clamp
 *
 * HANDOFF §7-10이 요구한 두 번째 지점이다. 지금까지 이 경로에 테스트가
 * 없었던 이유는 구조적이다: 테스트 모드(identity_pba)의 extent 맵은
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

TEST_CASE("identity 맵은 단일 extent이고 경계 검사를 건너뛴다") {
    const int64_t kSize = 1 << 20;   /* 1MiB */
    ExtentCache c = ExtentCache::identity(kSize);

    CHECK(c.num_extents() == 1);
    CHECK(c.is_contiguous() == true);

    /* 논리 오프셋 == 물리 오프셋 */
    PBASegment seg = c.lookup(4096, 8192);
    CHECK(seg.pba == 4096);
    CHECK(seg.len == 8192);

    /* 파일 끝까지 남은 바이트 */
    auto [rem, idx] = c.remaining_at(kSize - 512, -1);
    CHECK(rem == 512);
    CHECK(idx == 0);
}

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
