/* ============================================================
 * apply_ae_success / apply_ae_failure_backoff 단위 테스트
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

TEST_CASE("apply_ae_failure_backoff: stale 응답은 되감지 않는다") {
    auto s = make_leader_with_log({1, 1, 1});
    s->raft.cluster[1].next_index = 3;   /* 다른 워커가 이미 전진시켰다 */
    s->raft.cluster[1].match_index = 2;

    /* sent_next=1 로 보낸 요청의 실패 응답 -> 지금 next(3)와 다르다 */
    bool applied = s->apply_ae_failure_backoff(1, /*prev_log_index=*/0,
                                               /*conflict_term=*/0, /*conflict_index=*/0,
                                               /*sent_next=*/1);
    CHECK(applied == false);
    CHECK(s->raft.cluster[1].next_index == 3);   /* 진전이 보존됐다 */
}

TEST_CASE("apply_ae_failure_backoff: 힌트가 없으면 1씩 되감는다") {
    auto s = make_leader_with_log({1, 1, 1});
    s->raft.cluster[1].next_index = 3;
    s->raft.cluster[1].match_index = 0;

    bool applied = s->apply_ae_failure_backoff(1, 2, 0, 0, /*sent_next=*/3);
    CHECK(applied == true);
    CHECK(s->raft.cluster[1].next_index == 2);
}

TEST_CASE("apply_ae_failure_backoff: conflict_term==0 이면 conflict_index로 점프한다") {
    auto s = make_leader_with_log({1, 1, 1, 1, 1});
    s->raft.cluster[1].next_index = 5;
    s->raft.cluster[1].match_index = 0;

    /* 팔로워 로그가 짧다: "인덱스 2부터 없다" */
    bool applied = s->apply_ae_failure_backoff(1, 4, /*conflict_term=*/0,
                                               /*conflict_index=*/2, /*sent_next=*/5);
    CHECK(applied == true);
    CHECK(s->raft.cluster[1].next_index == 2);
}

TEST_CASE("apply_ae_failure_backoff: 항상 전진(=되감기)을 보장한다") {
    auto s = make_leader_with_log({1, 1, 1, 1, 1});
    s->raft.cluster[1].next_index = 3;
    s->raft.cluster[1].match_index = 0;

    /* 힌트가 현재 next보다 크면(>= prev) 그대로 두면 무한 루프가 된다.
     * "Ensure we always make progress" -> prev-1 로 강제 */
    bool applied = s->apply_ae_failure_backoff(1, 2, 0, /*conflict_index=*/9,
                                               /*sent_next=*/3);
    CHECK(applied == true);
    CHECK(s->raft.cluster[1].next_index == 2);
}

TEST_CASE("apply_ae_failure_backoff: prev==0 에서 uint64 언더플로가 없다 (D12)") {
    auto s = make_leader_with_log({1});
    s->raft.cluster[1].next_index = 0;   /* prev = 0 */
    s->raft.cluster[1].match_index = 0;

    bool applied = s->apply_ae_failure_backoff(1, /*prev_log_index=*/0,
                                               /*conflict_term=*/0, /*conflict_index=*/0,
                                               /*sent_next=*/0);
    CHECK(applied == true);
    /* 언더플로가 있었다면 UINT64_MAX가 그대로 통과했다.
     * floor = match_index + 1 = 1 이므로 결과는 1이어야 한다. */
    CHECK(s->raft.cluster[1].next_index == 1);
    CHECK(s->raft.cluster[1].next_index != UINT64_MAX);
}

TEST_CASE("apply_ae_failure_backoff: match_index+1 밑으로는 절대 되감지 않는다") {
    auto s = make_leader_with_log({1, 1, 1, 1, 1});
    s->raft.cluster[1].next_index = 5;
    s->raft.cluster[1].match_index = 3;   /* 팔로워가 3까지 확인해줬다 */

    /* 팔로워가 "인덱스 1부터 없다"고 우겨도 floor가 막는다.
     * 이 guard는 GC된 슬롯을 PBA로 읽어 stale 데이터를 복제하지 않는다는
     * 안전 불변식이다 (DECISIONS.md D12). */
    bool applied = s->apply_ae_failure_backoff(1, 4, 0, /*conflict_index=*/1,
                                               /*sent_next=*/5);
    CHECK(applied == true);
    CHECK(s->raft.cluster[1].next_index == 4);   /* = match_index + 1 */
}

TEST_CASE("apply_ae_failure_backoff: conflict_term을 리더 로그에서 찾아 그 term의 끝으로 간다") {
    /* 절대 인덱스: 1(term1) 2(term1) 3(term2) 4(term2) 5(term3) */
    auto s = make_leader_with_log({1, 1, 2, 2, 3});
    s->raft.cluster[1].next_index = 6;
    s->raft.cluster[1].match_index = 0;

    /* 팔로워가 "내 conflict_term은 1, conflict_index는 2"라고 알려줬다.
     * 리더 로그에서 term 1의 마지막 엔트리는 인덱스 2 -> next = 3 */
    bool applied = s->apply_ae_failure_backoff(1, /*prev_log_index=*/5,
                                               /*conflict_term=*/1, /*conflict_index=*/2,
                                               /*sent_next=*/6);
    CHECK(applied == true);
    CHECK(s->raft.cluster[1].next_index == 3);
}

TEST_CASE("apply_ae_failure_backoff: conflict_term이 리더 로그에 없으면 conflict_index로 간다") {
    auto s = make_leader_with_log({5, 5, 5});   /* 리더 로그에는 term 5뿐 */
    s->raft.cluster[1].next_index = 4;
    s->raft.cluster[1].match_index = 0;

    /* term 2는 리더 로그에 없다. 루프는 log[j].term < conflict_term 에서
     * 탈출하고 new_next는 conflict_index(2)로 남는다. */
    bool applied = s->apply_ae_failure_backoff(1, 3, /*conflict_term=*/2,
                                               /*conflict_index=*/2, /*sent_next=*/4);
    CHECK(applied == true);
    CHECK(s->raft.cluster[1].next_index == 2);
}
