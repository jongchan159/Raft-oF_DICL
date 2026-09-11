/* ============================================================
 * 선거 경로 단위 테스트.
 *
 * 리팩토링 전에는 이 영역의 테스트가 **0건**이었다. 이유는 구조적이다:
 * core/ 가 전송을 링크타임 자유 함수로 가져갔기 때문에 응답을 조작할
 * 방법이 없었고, 선거를 돌려보려면 3노드 e2e 스크립트를 띄우는 수밖에
 * 없었다. 전송이 추상 인터페이스가 되면서 fake를 주입할 수 있게 됐다.
 *
 * 여기서 다루지 않는 것: become_leader와 "표를 주는" 경로는
 * persist_circular를 호출하므로 실제 링 파일(cached_fd)이 필요하다.
 * 그건 selftest와 3노드 e2e가 담당한다. 이 파일은 I/O 없이 검증할 수
 * 있는 판정 로직만 본다.
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_server.h"
#include "raft_constants.h"
#include "fake_transport.h"

using namespace nvmeof_raft;

namespace {

/* term=T, 엔트리 n개(전부 term T)를 가진 3노드 클러스터의 candidate */
std::unique_ptr<Server> make_candidate(uint64_t term, size_t n_entries) {
    auto s = std::make_unique<Server>();
    s->ring.configure(2048);
    s->raft.id = 1;
    s->raft.cluster.resize(3);
    for (size_t i = 0; i < 3; i++) {
        s->raft.cluster[i].id = static_cast<uint64_t>(i + 1);
    }
    s->raft.cluster_index = 0;
    s->raft.state = ServerState::Candidate;
    s->raft.current_term = term;
    s->raft.heartbeat_ms = 100;

    s->raft.log.clear();
    s->raft.log.push_back(Entry{});          /* sentinel */
    for (size_t i = 0; i < n_entries; i++) {
        Entry e;
        e.term = term;
        e.command.assign(16, 0x5A);
        s->raft.log.push_back(std::move(e));
    }
    s->ring.tail_log_index = 1 + n_entries;
    return s;
}

}  /* anonymous namespace */

TEST_CASE("request_vote_worker: transport가 없으면 아무 일도 일어나지 않는다") {
    auto s = make_candidate(3, 2);
    REQUIRE(!s->transport);   /* shared_ptr을 직접 비교하면 doctest가 문자열화를 못 한다 */

    s->request_vote_worker(1);   /* 크래시하지 않아야 한다 */
    CHECK(s->raft.cluster[1].voted_for == 0);
}

TEST_CASE("request_vote_worker: 전송이 실패하면 표를 기록하지 않는다") {
    auto s = make_candidate(3, 2);
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    /* on_request_vote를 비워 두면 false를 돌려준다 = dial 실패와 같다 */
    s->transport = ft;

    s->request_vote_worker(1);
    CHECK(ft->vote_calls == 1);
    CHECK(s->raft.cluster[1].voted_for == 0);
}

TEST_CASE("request_vote_worker: 후보의 last_log_index/term을 정확히 보낸다") {
    auto s = make_candidate(/*term=*/7, /*n_entries=*/4);   /* tail = 5 */
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    ft->on_request_vote = [](int, const RequestVoteRequest &,
                             RequestVoteResponse &) { return false; };
    s->transport = ft;

    s->request_vote_worker(2);

    CHECK(ft->last_vote_peer == 2);
    CHECK(ft->last_vote_req.rpc.term == 7);
    CHECK(ft->last_vote_req.candidate_id == 1);
    /* last_log_index = tail_log_index - 1 = 4 (마지막 엔트리의 절대 인덱스) */
    CHECK(ft->last_vote_req.last_log_index == 4);
    CHECK(ft->last_vote_req.last_log_term == 7);
}

TEST_CASE("request_vote_worker: 같은 term의 grant는 표로 기록된다") {
    auto s = make_candidate(3, 2);
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    ft->on_request_vote = [](int, const RequestVoteRequest &req,
                             RequestVoteResponse &rsp) {
        rsp.rpc.term = req.rpc.term;     /* 같은 term */
        rsp.vote_granted = true;
        return true;
    };
    s->transport = ft;

    s->request_vote_worker(1);
    /* 표는 "그 피어가 나에게 투표했다"는 뜻으로 cluster[peer].voted_for에
     * 기록된다 -- become_leader가 이걸 세어 쿼럼을 판정한다 (D2 참고). */
    CHECK(s->raft.cluster[1].voted_for == s->raft.id);
    CHECK(s->raft.cluster[2].voted_for == 0);   /* 부른 적 없는 피어는 그대로 */
}

TEST_CASE("request_vote_worker: 거절은 기록하지 않는다") {
    auto s = make_candidate(3, 2);
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    ft->on_request_vote = [](int, const RequestVoteRequest &req,
                             RequestVoteResponse &rsp) {
        rsp.rpc.term = req.rpc.term;
        rsp.vote_granted = false;
        return true;
    };
    s->transport = ft;

    s->request_vote_worker(1);
    CHECK(s->raft.cluster[1].voted_for == 0);
}

TEST_CASE("request_vote_worker: term이 어긋난 stale 응답은 버린다") {
    auto s = make_candidate(5, 2);
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    ft->on_request_vote = [](int, const RequestVoteRequest &,
                             RequestVoteResponse &rsp) {
        rsp.rpc.term = 4;          /* 우리 term(5)보다 낮다 = 지난 라운드의 응답 */
        rsp.vote_granted = true;
        return true;
    };
    s->transport = ft;

    s->request_vote_worker(1);
    CHECK(s->raft.cluster[1].voted_for == 0);
    CHECK(s->raft.current_term == 5);   /* 강등되지도 않는다 */
}

TEST_CASE("handle_request_vote_request: 낮은 term의 요청은 거절한다") {
    auto s = make_candidate(9, 3);

    RequestVoteRequest req;
    req.rpc.term = 8;            /* 우리보다 낮다 */
    req.candidate_id = 2;
    req.last_log_index = 100;    /* 로그가 길어도 무관 */
    req.last_log_term = 9;
    RequestVoteResponse rsp;

    s->handle_request_vote_request(req, rsp);

    CHECK(rsp.vote_granted == false);
    CHECK(rsp.rpc.term == 9);    /* 우리 term을 알려준다 */
    CHECK(s->get_voted_for() == 0);
}

TEST_CASE("handle_request_vote_request: 로그가 뒤처진 후보는 거절한다") {
    /* 우리 로그: term 9 엔트리 3개 -> last_log_term=9, log_len=3 */
    auto s = make_candidate(9, 3);

    SUBCASE("last_log_term이 낮으면 거절") {
        RequestVoteRequest req;
        req.rpc.term = 9;
        req.candidate_id = 2;
        req.last_log_term = 8;
        req.last_log_index = 100;
        RequestVoteResponse rsp;
        s->handle_request_vote_request(req, rsp);
        CHECK(rsp.vote_granted == false);
    }

    SUBCASE("term이 같고 인덱스가 짧으면 거절") {
        RequestVoteRequest req;
        req.rpc.term = 9;
        req.candidate_id = 2;
        req.last_log_term = 9;
        req.last_log_index = 2;   /* 우리 log_len(3)보다 짧다 */
        RequestVoteResponse rsp;
        s->handle_request_vote_request(req, rsp);
        CHECK(rsp.vote_granted == false);
    }
}

TEST_CASE("handle_request_vote_request: 이미 다른 후보에게 투표했으면 거절한다") {
    auto s = make_candidate(9, 3);
    /* 자기 투표 상태는 cluster[cluster_index].voted_for 한 자리뿐이다 (D2) */
    s->set_voted_for(3);
    REQUIRE(s->get_voted_for() == 3);

    RequestVoteRequest req;
    req.rpc.term = 9;
    req.candidate_id = 2;        /* 3이 아닌 다른 후보 */
    req.last_log_term = 9;
    req.last_log_index = 3;      /* 로그 조건은 만족 */
    RequestVoteResponse rsp;

    s->handle_request_vote_request(req, rsp);
    CHECK(rsp.vote_granted == false);
    CHECK(s->get_voted_for() == 3);   /* 바뀌지 않는다 */
}
