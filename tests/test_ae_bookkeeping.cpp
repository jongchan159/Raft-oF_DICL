/* ============================================================
 * apply_ae_success 단위 테스트
 *
 * HANDOFF §7-10이 "최소한 fast log backoff 경로에 테스트가 필요하다"고
 * 지목한 지점이다. 두 함수는 파일 I/O도 네트워크도 만지지 않고
 * cluster[fi]와 log만 보는 순수 산술이므로, Server를 스택에 만들고
 * 필드를 직접 세팅해 검증할 수 있다 (init_storage / start 불필요).
 *
 * 특히 DECISIONS.md D12(prev == 0 언더플로 가드)와 floor guard
 * (matchIndex+1 밑으로 되감지 않는다)를 회귀로 고정한다.
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_server.h"
#include "raft_constants.h"

using namespace nvmeof_raft;

namespace {

/* 리더 1명 + 팔로워 2명, 로그는 sentinel + n_entries개.
 * 엔트리 i(절대 인덱스 i)의 term은 terms[i-1]. */
std::unique_ptr<Server> make_leader_with_log(const std::vector<uint64_t> &terms) {
    auto s = std::make_unique<Server>();
    s->raft.id = 1;
    s->raft.cluster.resize(3);
    for (size_t i = 0; i < 3; i++) {
        s->raft.cluster[i].id = static_cast<uint64_t>(i + 1);
    }
    s->raft.cluster_index = 0;
    s->raft.state = ServerState::Leader;
    s->raft.heartbeat_ms = 100;

    /* log[0]은 sentinel (term 0). 실제 엔트리는 절대 인덱스 1부터
     * (DECISIONS.md Q1) */
    s->raft.log.clear();
    s->raft.log.push_back(Entry{});
    for (uint64_t t : terms) {
        Entry e;
        e.term = t;
        s->raft.log.push_back(std::move(e));
    }
    s->ring.tail_log_index = 1 + terms.size();
    s->raft.current_term = terms.empty() ? 1 : terms.back();
    return s;
}

}  /* anonymous namespace */

TEST_CASE("apply_ae_success: next_index/match_index를 전진시킨다") {
    auto s = make_leader_with_log({1, 1, 1});   /* 절대 인덱스 1,2,3 */
    s->raft.cluster[1].next_index = 1;
    s->raft.cluster[1].match_index = 0;

    /* 인덱스 1부터 2개를 보냈고 성공 -> next=3, match=2 */
    s->apply_ae_success(/*fi=*/1, /*next=*/1, /*len_entries=*/2, /*has_entries=*/true);
    CHECK(s->raft.cluster[1].next_index == 3);
    CHECK(s->raft.cluster[1].match_index == 2);
}

TEST_CASE("apply_ae_success: next_index를 뒤로 되돌리지 않는다") {
    auto s = make_leader_with_log({1, 1, 1});
    s->raft.cluster[1].next_index = 4;   /* 다른 워커가 이미 전진시켜 둔 상태 */
    s->raft.cluster[1].match_index = 3;

    /* 뒤늦게 도착한 "인덱스 1에서 1개 성공" 응답은 무시돼야 한다 */
    s->apply_ae_success(1, /*next=*/1, /*len_entries=*/1, /*has_entries=*/true);
    CHECK(s->raft.cluster[1].next_index == 4);
    CHECK(s->raft.cluster[1].match_index == 3);
}

TEST_CASE("apply_ae_success: 보낼 게 남으면 다음 하트비트를 즉시 만료시킨다") {
    auto s = make_leader_with_log({1, 1, 1, 1, 1});   /* tail_log_index = 6 */
    s->raft.cluster[1].next_index = 1;
    s->raft.heartbeat_timeout = std::chrono::steady_clock::now() + std::chrono::hours(1);

    /* next=1에서 2개만 보냈으므로 tail-1(=5) >= next(3) -> 즉시 재전송 */
    s->apply_ae_success(1, 1, 2, /*has_entries=*/true);
    CHECK(s->raft.cluster[1].next_index == 3);
    CHECK(s->raft.heartbeat_timeout == std::chrono::steady_clock::time_point{});

    SUBCASE("밀린 게 없으면 건드리지 않는다") {
        auto s2 = make_leader_with_log({1, 1});   /* tail_log_index = 3 */
        s2->raft.cluster[1].next_index = 1;
        auto far = std::chrono::steady_clock::now() + std::chrono::hours(1);
        s2->raft.heartbeat_timeout = far;
        s2->apply_ae_success(1, 1, 2, true);      /* next -> 3, tail-1 = 2 < 3 */
        CHECK(s2->raft.cluster[1].next_index == 3);
        CHECK(s2->raft.heartbeat_timeout == far);
    }

    SUBCASE("순수 하트비트(has_entries=false)는 건드리지 않는다") {
        auto s3 = make_leader_with_log({1, 1, 1, 1, 1});
        s3->raft.cluster[1].next_index = 1;
        auto far = std::chrono::steady_clock::now() + std::chrono::hours(1);
        s3->raft.heartbeat_timeout = far;
        s3->apply_ae_success(1, 1, 0, /*has_entries=*/false);
        CHECK(s3->raft.heartbeat_timeout == far);
    }
}
