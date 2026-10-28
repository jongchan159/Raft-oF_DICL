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
     * 2048 페이지 = 8MiB. */
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
    CHECK(def->max_ae_batch == DEFAULT_MAX_AE_BATCH);
    CHECK(def->max_ae_batch_bytes == DEFAULT_MAX_AE_BATCH_BYTES);
}

/* ============================================================
 * AlignedBuffer::grow_uninit: 0 채우기 없이 늘리되 정렬과 기존 데이터는 보존
 * ============================================================ */
TEST_CASE("AlignedBuffer::grow_uninit: 재할당 시 기존 데이터를 보존하고 정렬을 유지한다") {
    AlignedBuffer b;
    b.grow_uninit(1024);
    CHECK(b.size() == 1024);
    CHECK(reinterpret_cast<uintptr_t>(b.data()) % SECTOR_SIZE == 0);
    for (size_t i = 0; i < 1024; i++) b.data()[i] = static_cast<uint8_t>(i * 7 + 3);

    b.grow_uninit(1024 * 64);               // 용량 초과 -> 재할당
    CHECK(b.size() == 1024 * 64);
    CHECK(reinterpret_cast<uintptr_t>(b.data()) % SECTOR_SIZE == 0);
    bool same = true;
    for (size_t i = 0; i < 1024; i++) same = same && (b.data()[i] == static_cast<uint8_t>(i * 7 + 3));
    CHECK(same);                             // 앞 1024바이트는 그대로

    size_t cap = b.capacity();
    b.clear();
    b.grow_uninit(4608);                     // 용량 안: 재할당 없이 size만 변경
    CHECK(b.capacity() == cap);
    CHECK(b.size() == 4608);
}

TEST_CASE("AlignedBuffer::resize: 늘어난 구간은 0으로 채우고 grow_uninit은 채우지 않는다") {
    AlignedBuffer b;
    b.grow_uninit(4096);
    for (size_t i = 0; i < 4096; i++) b.data()[i] = 0xAB;

    // grow_uninit: 용량 안에서 늘릴 때 기존 바이트를 건드리지 않음 (memset 회귀 방지)
    b.clear();
    b.grow_uninit(4096);
    CHECK(b.data()[0] == 0xAB);
    CHECK(b.data()[4095] == 0xAB);

    // resize: 늘어난 구간 [1024, 4096)은 반드시 0
    b.clear();
    b.resize(1024);                  // 이 구간은 0으로 채워져야 함
    for (size_t i = 0; i < 1024; i++) b.data()[i] = 0xCD;
    b.resize(4096);                  // 용량 안: 더러운 메모리 위에서 늘림
    bool tail_zero = true;
    for (size_t i = 1024; i < 4096; i++) tail_zero = tail_zero && (b.data()[i] == 0);
    CHECK(tail_zero);
    CHECK(b.data()[0] == 0xCD);      // 앞부분은 보존
}

/* ============================================================
 * [B'] persist_prepare_locked + persist_io 가 persist_circular와 같은 결과를 내는가
 *   - 같은 입력 -> 링 파일 전체 바이트, ring.tail_slot, log_slot_map 이 동일
 *   - 링 끝을 넘어가는 배치(구간이 둘로 갈림)도 동일
 * 실제 링 파일(O_DIRECT)이 필요해서 임시 디렉터리에 만든다.
 * ============================================================ */
#include "cached_fd.h"
#include <unistd.h>
#include <cstdlib>
#include <cstdio>

namespace {
struct PersistFixture {
    std::string dir, meta, dev;
    std::unique_ptr<Server> s;

    explicit PersistFixture(const char *tag) {
        char tmpl[] = "/tmp/persist_split_XXXXXX";
        REQUIRE(::mkdtemp(tmpl) != nullptr);
        dir = tmpl;
        meta = dir + "/" + tag + ".ring";
        dev = dir + "/" + tag + ".dev";

        s = std::make_unique<Server>();
        s->ring.configure(64);                       /* 64 페이지: 작게 해서 감김을 쉽게 만든다 */
        uint64_t want = s->ring.file_size_bytes();
        create_ring_file(meta, want);
        {
            FILE *f = std::fopen(dev.c_str(), "wb");
            REQUIRE(f != nullptr);
            std::fclose(f);
            REQUIRE(::truncate(dev.c_str(), static_cast<off_t>(want)) == 0);
        }
        auto cfd = std::make_shared<CachedFD>(CachedFD::open(meta, dev));
        cfd->cache_extents(static_cast<int64_t>(want));
        s->io.cached_fd = cfd;

        s->raft.id = 1;
        s->raft.cluster.resize(3);                   /* get_voted_for()가 cluster[cluster_index]를 읽는다 */
        for (size_t i = 0; i < 3; i++) s->raft.cluster[i].id = static_cast<uint64_t>(i + 1);
        s->raft.cluster_index = 0;
        s->raft.state = ServerState::Leader;
        s->raft.current_term = 3;
        s->ring.persisted_init = true;               /* 헤더 쓰기가 필요 없는 상태 */
        s->ring.persisted_term = 3;
        s->ring.persisted_voted_for = s->get_voted_for();
        s->raft.log.clear();
        s->raft.log.push_back(Entry{});              /* sentinel */
        s->ring.tail_log_index = 1;
    }
    ~PersistFixture() {
        s.reset();
        std::remove(meta.c_str());
        std::remove(dev.c_str());
        ::rmdir(dir.c_str());
    }
    /* run_buf를 0xAB로 채워 둔다. 실제 서버에서는 run_buf가 배치마다 재사용되어 이전 바이트가 남아 있다.
     * 패딩을 0으로 지우는 코드가 빠지면 이 0xAB가 디스크로 새어 나가므로 회귀를 잡는다. */
    void dirty_run_buf() {
        const size_t bytes = 1 << 20;
        s->ring.run_buf.resize(bytes, SECTOR_SIZE);
        std::memset(s->ring.run_buf.data(), 0xAB, bytes);
        s->ring.run_buf.clear();
    }
    /* 크기가 제각각인 엔트리 n개를 로그에 추가 (tail_log_index도 같이 전진) */
    void append_entries(size_t n, size_t seed) {
        for (size_t i = 0; i < n; i++) {
            Entry e;
            e.term = 3;
            size_t sz = (i % 7 == 0) ? 0 : (100 + (i * 977 + seed) % 6000);   /* 0은 no-op 엔트리 */
            e.command.resize(sz);
            for (size_t k = 0; k < sz; k++) e.command[k] = static_cast<uint8_t>(k * 31 + i + seed);
            s->raft.log.push_back(std::move(e));
            s->ring.tail_log_index++;
        }
    }
};

void check_same(PersistFixture &a, PersistFixture &b) {
    CHECK(a.s->ring.tail_slot == b.s->ring.tail_slot);
    REQUIRE(a.s->ring.log_slot_map.size() == b.s->ring.log_slot_map.size());
    bool slots_same = true;
    for (const auto &kv : a.s->ring.log_slot_map) {
        auto it = b.s->ring.log_slot_map.find(kv.first);
        if (it == b.s->ring.log_slot_map.end() || it->second.start != kv.second.start ||
            it->second.num_slots != kv.second.num_slots) {
            slots_same = false;
        }
    }
    CHECK(slots_same);
    const size_t sz = static_cast<size_t>(a.s->ring.file_size_bytes());
    auto ba = a.s->io.cached_fd->read_at_file(sz, 0);
    auto bb = b.s->io.cached_fd->read_at_file(sz, 0);
    CHECK(ba == bb);                                  /* 링 파일 전체가 바이트 단위로 같다 */
}
}  /* namespace */

TEST_CASE("persist_prepare_locked + persist_io: persist_circular와 같은 바이트/슬롯 상태를 만든다") {
    PersistFixture old_path("old"), new_path("new");
    old_path.dirty_run_buf(); new_path.dirty_run_buf();
    old_path.append_entries(40, 1);
    new_path.append_entries(40, 1);

    CHECK_FALSE(new_path.s->persist_needs_header());
    old_path.s->persist_circular(true, 40);
    auto plan = new_path.s->persist_prepare_locked(40);
    /* 구간 개수는 링 파일의 extent 배치에 따라 달라진다(extent 경계에서 끊김). 개수 대신 성질을 본다:
     * 모든 구간이 O_DIRECT 정렬을 지키고, 구간 길이의 합이 인코딩된 전체 바이트와 같다. */
    CHECK_FALSE(plan.writes.empty());
    size_t bytes_sum = 0;
    bool aligned = true;
    for (const auto &g : plan.writes) {
        bytes_sum += g.len;
        aligned = aligned && g.dev_off % SECTOR_SIZE == 0 && g.buf_off % SECTOR_SIZE == 0 &&
                  g.len % SECTOR_SIZE == 0;
    }
    CHECK(aligned);
    CHECK(bytes_sum == new_path.s->ring.run_buf.size());
    CHECK(new_path.s->persist_io(plan) > 0);

    check_same(old_path, new_path);
}

TEST_CASE("persist_prepare_locked: 링 끝을 넘어가는 배치도 같고 구간이 둘로 갈린다") {
    PersistFixture old_path("old2"), new_path("new2");
    old_path.dirty_run_buf(); new_path.dirty_run_buf();
    const uint64_t near_end = old_path.s->ring.ring_slots - 20;   /* 20슬롯 남기고 시작 -> 중간에 감김 */
    old_path.s->ring.tail_slot = near_end;
    new_path.s->ring.tail_slot = near_end;
    old_path.append_entries(30, 5);
    new_path.append_entries(30, 5);

    old_path.s->persist_circular(true, 30);
    auto plan = new_path.s->persist_prepare_locked(30);
    CHECK(plan.writes.size() >= 2);
    new_path.s->persist_io(plan);

    check_same(old_path, new_path);
}

TEST_CASE("persist_prepare_locked: 여러 배치를 연달아 해도 매번 같다") {
    PersistFixture old_path("old3"), new_path("new3");
    old_path.dirty_run_buf(); new_path.dirty_run_buf();
    for (size_t round = 0; round < 6; round++) {
        old_path.append_entries(25, round);
        new_path.append_entries(25, round);
        old_path.s->persist_circular(true, 25);
        new_path.s->persist_io(new_path.s->persist_prepare_locked(25));
    }
    check_same(old_path, new_path);
}

TEST_CASE("persist_needs_header: term이 바뀌면 true") {
    PersistFixture f("hdr");
    CHECK_FALSE(f.s->persist_needs_header());
    f.s->raft.current_term = 4;
    CHECK(f.s->persist_needs_header());
}


/* ============================================================
 * [B'] persist I/O가 mu 밖에서 진행되는 동안의 규칙
 *   1) durable_last_index_locked: 진행 중이면 durable_floor, 아니면 tail_log_index - 1
 *   2) 리더 자신의 표는 durable 인덱스까지만 센다 (advance_commit_index_locked)
 *   3) wait_persist_idle_locked: 진행 중이면 기다리고, 끝나 notify되면 mu를 쥔 채 돌아온다
 * ============================================================ */
#include <atomic>
#include <chrono>
#include <thread>

namespace {
/* 리더, 3노드, 현재 term 엔트리 10개 (인덱스 1..10), tail_log_index = 11 */
std::unique_ptr<Server> make_leader_with_log() {
    auto s = std::make_unique<Server>();
    s->ring.configure(64);
    s->raft.id = 1;
    s->raft.cluster.resize(3);
    for (size_t i = 0; i < 3; i++) s->raft.cluster[i].id = static_cast<uint64_t>(i + 1);
    s->raft.cluster_index = 0;
    s->raft.state = ServerState::Leader;
    s->raft.current_term = 3;
    s->raft.log.clear();
    s->raft.log.push_back(Entry{});
    for (size_t i = 0; i < 10; i++) {
        Entry e;
        e.term = 3;
        e.command.assign(8, 0x11);
        s->raft.log.push_back(std::move(e));
    }
    s->ring.tail_log_index = 11;
    s->raft.commit_index = 0;
    return s;
}
}  // namespace

TEST_CASE("durable_last_index_locked: 진행 중이면 floor, 아니면 tail - 1") {
    auto s = make_leader_with_log();
    CHECK(s->durable_last_index_locked() == 10);
    s->persist_inflight = true;
    s->ring.durable_floor = 5;
    CHECK(s->durable_last_index_locked() == 5);
    s->persist_inflight = false;
    CHECK(s->durable_last_index_locked() == 10);
}

TEST_CASE("advance_commit_index_locked: persist 진행 중이면 리더 자신의 표는 durable까지만") {
    {   /* 진행 중이 아님: 리더 10, 팔로워 8 -> 과반 8 */
        auto s = make_leader_with_log();
        s->raft.cluster[1].match_index = 8;
        std::vector<std::shared_ptr<EntryCommitSignal>> sig;
        s->advance_commit_index_locked(&sig);
        CHECK(s->raft.commit_index == 8);
    }
    {   /* 진행 중: 리더는 floor 5까지만 -> 과반 5. 아직 안 쓴 6..10을 리더 표로 세면 8이 되어 틀린다 */
        auto s = make_leader_with_log();
        s->raft.cluster[1].match_index = 8;
        s->persist_inflight = true;
        s->ring.durable_floor = 5;
        std::vector<std::shared_ptr<EntryCommitSignal>> sig;
        s->advance_commit_index_locked(&sig);
        CHECK(s->raft.commit_index == 5);
    }
}

TEST_CASE("wait_persist_idle_locked: 진행 중이면 기다리고 notify 후 mu를 쥔 채 돌아온다") {
    auto s = std::make_unique<Server>();
    s->persist_inflight = true;
    std::atomic<bool> done{false};
    std::thread waiter([&] {
        s->mu.lock();
        s->wait_persist_idle_locked();      /* 기다린다 */
        done = true;
        s->mu.unlock();
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    CHECK_FALSE(done.load());               /* 아직 진행 중이라 기다리는 중 */

    {
        std::lock_guard<std::mutex> lk(s->mu);     /* 기다리는 동안 mu는 비어 있어야 한다(락을 쥔 채 기다리면 여기서 막힘) */
        s->persist_inflight = false;
    }
    s->persist_cv.notify_all();
    waiter.join();
    CHECK(done.load());
}

TEST_CASE("wait_persist_idle_locked: 진행 중이 아니면 즉시 돌아온다") {
    auto s = std::make_unique<Server>();
    s->mu.lock();
    s->wait_persist_idle_locked();
    bool other_got_lock = true;
    std::thread t([&] { other_got_lock = s->mu.try_lock(); if (other_got_lock) s->mu.unlock(); });
    t.join();
    CHECK_FALSE(other_got_lock);            /* 돌아온 뒤에도 mu는 이 스레드가 쥐고 있다 */
    s->mu.unlock();
}