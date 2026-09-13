/* ============================================================
 * 링 인덱스/슬롯 산술 단위 테스트
 *
 * 여기 있는 함수들은 온-디스크 레이아웃 계약 그 자체다. DECISIONS.md
 * D13(슬롯 0에 메타 32B + 명령 480B가 함께 들어간다)과 Q1(실제 엔트리는
 * 절대 인덱스 1부터, log[0]은 sentinel)을 산술 수준에서 고정한다.
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_server.h"
#include "raft_constants.h"

using namespace nvmeof_raft;

TEST_CASE("slots_for_entry: 슬롯 0에 메타 32B가 함께 들어간다 (D13)") {
    /* 슬롯 하나는 512B이고 그 앞 32B가 메타다 -> 명령 480B까지 1슬롯 */
    CHECK(slots_for_entry(0) == 1);
    CHECK(slots_for_entry(1) == 1);
    CHECK(slots_for_entry(479) == 1);
    CHECK(slots_for_entry(480) == 1);   /* 32 + 480 = 512, 정확히 한 슬롯 */
    CHECK(slots_for_entry(481) == 2);   /* 1바이트 넘으면 두 슬롯 */

    /* 4064B가 정확히 8슬롯 = 4096B = 4KiB 한 페이지인 것이 이 설계의 핵심 */
    CHECK(slots_for_entry(4064) == 8);
    CHECK(slots_for_entry(4064) * SECTOR_SIZE == PAGE_SIZE);
    CHECK(slots_for_entry(4065) == 9);

    /* 일반식: ceil((32 + cmd_len) / 512) */
    for (size_t n : {0u, 1u, 100u, 480u, 481u, 1000u, 4064u, 4065u, 9000u, 65536u}) {
        CHECK(slots_for_entry(n) ==
              (ENTRY_META_SIZE + n + SECTOR_SIZE - 1) / SECTOR_SIZE);
    }
}

TEST_CASE("entry_slots: deferred 엔트리는 cmd_len으로 계산한다") {
    Entry loaded;
    loaded.command.assign(1000, 0xAB);
    CHECK(entry_slots(loaded) == slots_for_entry(1000));

    /* 팔로워의 lazy loading 상태: command가 비어 있고 cmd_len만 있다 */
    Entry deferred;
    deferred.cmd_len = 1000;
    deferred.ring_slot = 42;
    CHECK(deferred.command.empty());
    CHECK(entry_slots(deferred) == slots_for_entry(1000));

    /* 둘이 같은 슬롯 수여야 한다 -- 다르면 팔로워의 tail_slot이 리더와 어긋난다 */
    CHECK(entry_slots(loaded) == entry_slots(deferred));
}

TEST_CASE("slot_offset: 슬롯 0은 헤더 뒤에서 시작한다") {
    CHECK(slot_offset(0) == static_cast<int64_t>(RING_OFFSET));
    CHECK(RING_OFFSET == HEADER_SIZE);
    CHECK(HEADER_SIZE == SECTOR_SIZE);   /* 헤더가 정확히 섹터 하나 */

    CHECK(slot_offset(1) == static_cast<int64_t>(RING_OFFSET + SECTOR_SIZE));
    CHECK(slot_offset(8) == static_cast<int64_t>(RING_OFFSET + PAGE_SIZE));

    /* 큰 슬롯 번호에서 int64 산술이 넘치지 않는다 (32GiB 링 = 64Mi 슬롯) */
    const uint64_t big = 64ull * 1024 * 1024;
    CHECK(slot_offset(big) ==
          static_cast<int64_t>(RING_OFFSET) + static_cast<int64_t>(big * SECTOR_SIZE));
}

namespace {

/* log[0] = sentinel + n개 엔트리, tail_log_index = 1 + n */
std::unique_ptr<Server> make_server_with_log(size_t n) {
    auto s = std::make_unique<Server>();
    /* 링 크기는 이제 인스턴스 필드다 -- 테스트마다 다르게 줄 수 있다.
     * 2048 페이지 = 8MiB (selftest와 동일). */
    s->ring.configure(2048);
    s->raft.log.clear();
    s->raft.log.push_back(Entry{});           /* sentinel */
    for (size_t i = 0; i < n; i++) {
        Entry e;
        e.term = 1;
        e.cmd_len = 100;
        s->raft.log.push_back(std::move(e));
    }
    s->ring.tail_log_index = 1 + n;
    return s;
}

}  /* anonymous namespace */

TEST_CASE("oldest_log_index / log_slice: 절대 인덱스 <-> 벡터 인덱스 (Q1)") {
    SUBCASE("빈 로그(sentinel만)") {
        auto s = make_server_with_log(0);
        /* 실제 엔트리가 없으면 oldest == tail */
        CHECK(s->oldest_log_index() == 1);
        CHECK(s->ring.tail_log_index == 1);
    }

    SUBCASE("트리밍 없는 로그: 절대 인덱스 1이 벡터 인덱스 1") {
        auto s = make_server_with_log(5);   /* 절대 1..5, tail = 6 */
        CHECK(s->oldest_log_index() == 1);
        CHECK(s->log_slice(1) == 1);
        CHECK(s->log_slice(5) == 5);
        /* sentinel은 벡터 인덱스 0에 남아 있다 */
        CHECK(s->raft.log.size() == 6);
        CHECK(s->raft.log[0].term == 0);
    }

    SUBCASE("앞을 잘라낸 뒤에도 같은 절대 인덱스가 같은 엔트리를 가리킨다") {
        auto s = make_server_with_log(5);
        /* 절대 인덱스 4를 식별할 수 있게 표시해 둔다 */
        s->raft.log[s->log_slice(4)].term = 77;

        /* 앞의 2개(절대 1,2)를 잘라낸 상황을 손으로 재현:
         * sentinel은 유지하고 벡터에서 2개를 지운다 */
        s->raft.log.erase(s->raft.log.begin() + 1, s->raft.log.begin() + 3);
        /* tail_log_index는 그대로 6, 남은 실제 엔트리는 3개 -> oldest = 3 */
        CHECK(s->oldest_log_index() == 3);
        CHECK(s->log_slice(3) == 1);
        CHECK(s->log_slice(4) == 2);
        CHECK(s->raft.log[s->log_slice(4)].term == 77);   /* 같은 엔트리를 가리킨다 */
        CHECK(s->raft.log[0].term == 0);                  /* sentinel 보존 */
    }
}

TEST_CASE("log_slice는 oldest_log_index의 역함수다") {
    auto s = make_server_with_log(10);
    s->raft.log.erase(s->raft.log.begin() + 1, s->raft.log.begin() + 4);   /* 앞 3개 트림 */
    const uint64_t oldest = s->oldest_log_index();
    for (uint64_t abs_idx = oldest; abs_idx < s->ring.tail_log_index; abs_idx++) {
        uint64_t slice = s->log_slice(abs_idx);
        CHECK(slice >= 1);                  /* sentinel을 침범하지 않는다 */
        CHECK(slice < s->raft.log.size());       /* 벡터 범위 안 */
        CHECK(oldest + (slice - 1) == abs_idx);
    }
}

TEST_CASE("can_write_all: 살아 있는 엔트리를 덮어쓰게 되면 거절한다") {
    /* num_pages/RING_SLOTS는 아직 프로세스 전역이고 test_main이 한 번
     * configure_ring(2048)로 설정한다 (Phase 4에서 Server 멤버로 옮길 대상). */
    auto s = make_server_with_log(0);

    SUBCASE("log_slot_map이 비어 있으면 무조건 받아들인다 (원본 동작)") {
        /* 링에 살아 있는 엔트리가 없으면 덮어쓸 것도 없다는 논리다.
         * 링 전체보다 큰 명령도 통과한다는 뜻이므로 놀랍지만, 원본
         * raft.go의 canWriteAll이 정확히 이렇게 동작한다. 백프레셔는
         * 실제로는 apply_internal의 대기 루프가 담당한다. */
        s->ring.log_slot_map.clear();
        std::vector<std::vector<uint8_t>> huge{
            std::vector<uint8_t>(static_cast<size_t>(s->ring.ring_slots + 16) * SECTOR_SIZE, 0)};
        CHECK(s->can_write_all(huge) == true);
    }

    SUBCASE("wrap 후 head_slot을 침범하면 거절한다") {
        /* 링 끝 50슬롯만 남았고, 살아 있는 엔트리가 슬롯 100에서 시작한다 */
        s->ring.log_slot_map[1] = SlotRecord{/*start=*/100, /*num_slots=*/8,
                                        LogEntryState::Using};
        s->ring.head_slot = 100;
        s->ring.tail_slot = s->ring.ring_slots - 50;

        /* 101슬롯이 필요한 명령: 32 + 51680 = 51712 = 101 * 512 */
        const size_t kNeeds101 = 101 * SECTOR_SIZE - ENTRY_META_SIZE;
        REQUIRE(slots_for_entry(kNeeds101) == 101);
        std::vector<std::vector<uint8_t>> big{std::vector<uint8_t>(kNeeds101, 0)};
        /* 링 끝에 안 들어가니 슬롯 0으로 wrap -> 0+101 > head_slot(100) */
        CHECK(s->can_write_all(big) == false);
    }

    SUBCASE("wrap 없이 들어가면 받아들인다") {
        s->ring.log_slot_map[1] = SlotRecord{100, 8, LogEntryState::Using};
        s->ring.head_slot = 100;
        s->ring.tail_slot = s->ring.ring_slots - 50;

        /* 30슬롯이면 링 끝(ring_slots)을 넘지 않으므로 wrap이 없다 */
        const size_t kNeeds30 = 30 * SECTOR_SIZE - ENTRY_META_SIZE;
        REQUIRE(slots_for_entry(kNeeds30) == 30);
        std::vector<std::vector<uint8_t>> small{std::vector<uint8_t>(kNeeds30, 0)};
        CHECK(s->can_write_all(small) == true);
    }
}

TEST_CASE("slots_for_log_index: slot_map이 있으면 그 값, 없으면 엔트리에서 계산") {
    auto s = make_server_with_log(3);   /* 절대 1..3, cmd_len=100 -> 1슬롯씩 */

    SUBCASE("slot_map에 없으면 in-memory 엔트리로 계산한다") {
        s->ring.log_slot_map.clear();
        CHECK(s->slots_for_log_index(1) == slots_for_entry(100));
        CHECK(s->slots_for_log_index(3) == slots_for_entry(100));
    }

    SUBCASE("slot_map에 있으면 그 num_slots가 정답이다") {
        /* 리더가 실제로 배치한 슬롯 수가 엔트리 계산값과 다를 수 있다
         * (예: wrap 정렬). 그때는 배치값이 우선이어야 한다 -- 이걸 틀리면
         * 배치 clamp가 남의 슬롯을 침범한다. */
        s->ring.log_slot_map[2] = SlotRecord{/*start=*/40, /*num_slots=*/7,
                                        LogEntryState::Using};
        CHECK(s->slots_for_log_index(2) == 7);
        CHECK(s->slots_for_log_index(1) == slots_for_entry(100));   /* 없는 인덱스는 폴백 */
    }

    SUBCASE("deferred 엔트리(command 없음)도 cmd_len으로 같은 답을 준다") {
        s->ring.log_slot_map.clear();
        Entry &e = s->raft.log[s->log_slice(2)];
        e.command.clear();
        e.cmd_len = 4064;
        e.ring_slot = 99;
        CHECK(s->slots_for_log_index(2) == slots_for_entry(4064));
        CHECK(s->slots_for_log_index(2) == 8);
    }
}

TEST_CASE("한 프로세스에 링 크기가 다른 Server를 여러 개 만들 수 있다") {
    /* 리팩토링 전에는 불가능했다. num_pages/total_slots/ring_slots가
     * core/raft_constants.h의 프로세스 전역 가변 변수였고 configure_ring()이
     * 그걸 갱신했기 때문이다 -- 두 번째 Server를 다른 크기로 만들면 첫 번째
     * Server의 슬롯 인덱스 해석이 조용히 깨진다. 그래서 유닛 테스트가
     * 케이스마다 다른 링 구성을 쓸 수 없었다(HANDOFF §7-10의 전제조건).
     *
     * 이제 링 크기는 Server::ring의 인스턴스 필드이므로 서로 간섭하지 않는다. */
    auto small = std::make_unique<Server>();
    auto large = std::make_unique<Server>();

    small->ring.configure(64);      /* 64 페이지 = 256KiB */
    large->ring.configure(65536);   /* 65536 페이지 = 256MiB */

    CHECK(small->ring.num_pages == 64);
    CHECK(large->ring.num_pages == 65536);
    CHECK(small->ring.total_slots == 64 * SLOTS_PER_PAGE);
    CHECK(large->ring.total_slots == 65536 * SLOTS_PER_PAGE);
    CHECK(small->ring.ring_slots == small->ring.total_slots - 1);
    CHECK(large->ring.ring_slots == large->ring.total_slots - 1);
    CHECK(small->ring.file_size_bytes() == 64 * PAGE_SIZE);
    CHECK(large->ring.file_size_bytes() == 65536 * PAGE_SIZE);

    /* small을 다시 설정해도 large는 그대로다 */
    small->ring.configure(128);
    CHECK(small->ring.num_pages == 128);
    CHECK(large->ring.num_pages == 65536);

    /* 최소 크기 클램프: 헤더 1섹터 + 최소한의 링 */
    auto tiny = std::make_unique<Server>();
    tiny->ring.configure(0);
    CHECK(tiny->ring.num_pages == 2);
    tiny->ring.configure(1);
    CHECK(tiny->ring.num_pages == 2);

    /* 기본값은 원본과 동일한 8Mi 페이지(32GiB) */
    auto def = std::make_unique<Server>();
    CHECK(def->ring.num_pages == DEFAULT_NUM_PAGES);
}
