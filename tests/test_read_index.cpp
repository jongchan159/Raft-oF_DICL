/* ============================================================
 * ReadIndex (core/src/raft_read.cpp) 단위 테스트 [수정-17]
 *
 * Server 를 스택에 만들고 필드를 직접 세팅한다 (init_storage / start 불필요).
 * Server::read 를 끝까지 돌리는 테스트는 start() 대신
 * start_read_hb_workers() 로 heartbeat 워커만 띄운다 -- main_loop 를 돌리면
 * heartbeat / slot GC 가 링 파일을 건드린다.
 *
 * 여기서 다루지 않는 것: 더 높은 term 을 보는 경로(update_term ->
 * persist_circular)는 실제 링 파일이 필요하다 (test_election.cpp 와 같은
 * 제약). 실클러스터 e2e 가 담당한다.
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_server.h"
#include "raft_constants.h"
#include "fake_transport.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

using namespace nvmeof_raft;

namespace {

/* 상태머신 호출을 기록한다. read 경로는 mu 밖에서 부르므로 스스로 잠근다. */
class RecordingStateMachine : public StateMachine {
public:
    ApplyResult apply(const std::vector<uint8_t> &cmd) override {
        std::lock_guard<std::mutex> lk(mu);
        calls.push_back(cmd);
        ApplyResult r;
        r.result = cmd;   /* echo */
        return r;
    }
    std::mutex mu;
    std::vector<std::vector<uint8_t>> calls;
};

/* term T 의 리더, n_nodes 노드, 엔트리 n_entries 개 (전부 커밋·apply 완료).
 * term_start_index 는 마지막 엔트리 (그 term 의 no-op 이라고 본다). */
std::unique_ptr<Server> make_leader(size_t n_nodes, uint64_t term, size_t n_entries) {
    auto s = std::make_unique<Server>();
    s->ring.configure(2048);
    s->raft.id = 1;
    s->raft.cluster.resize(n_nodes);
    for (size_t i = 0; i < n_nodes; i++) {
        s->raft.cluster[i].id = static_cast<uint64_t>(i + 1);
    }
    s->raft.cluster_index = 0;
    s->raft.state = ServerState::Leader;
    s->raft.current_term = term;
    s->raft.heartbeat_ms = 100;

    s->raft.log.clear();
    s->raft.log.push_back(Entry{});   /* sentinel */
    for (size_t i = 0; i < n_entries; i++) {
        Entry e;
        e.term = term;
        s->raft.log.push_back(std::move(e));
    }
    s->ring.tail_log_index = 1 + n_entries;
    s->raft.commit_index = n_entries;
    s->raft.last_applied = n_entries + 1;   /* "다음에 apply 할 인덱스" */
    s->raft.term_start_index = n_entries;
    s->read_timeout_ms = 300;
    s->statemachine = std::make_shared<RecordingStateMachine>();
    return s;
}

/* 같은 term 이면 ok 를 돌려주는 팔로워 */
bool ack_same_term(const ReadIndexHeartbeatRequest &req, ReadIndexHeartbeatResponse &rsp) {
    rsp.rpc.term = req.rpc.term;
    rsp.ok = true;
    rsp.round = req.round;
    return true;
}

std::vector<uint8_t> bytes(const char *s) {
    return std::vector<uint8_t>(s, s + std::strlen(s));
}

}  /* anonymous namespace */

/* ---------------- 팔로워 측 핸들러 ---------------- */

TEST_CASE("handle_read_index_heartbeat: 같은 term 이면 ok + election timer 리셋") {
    auto s = make_leader(3, 5, 1);
    s->raft.state = ServerState::Follower;
    s->raft.election_timeout = std::chrono::steady_clock::time_point{};

    ReadIndexHeartbeatRequest req;
    req.rpc.term = 5;
    req.round = 42;
    ReadIndexHeartbeatResponse rsp;
    s->handle_read_index_heartbeat(req, rsp);

    CHECK(rsp.ok);
    CHECK(rsp.rpc.term == 5);
    CHECK(rsp.round == 42);
    CHECK(s->raft.election_timeout > std::chrono::steady_clock::now());
}

TEST_CASE("handle_read_index_heartbeat: 낮은 term 은 거절하고 우리 term 을 알려준다") {
    auto s = make_leader(3, 5, 1);
    s->raft.state = ServerState::Follower;
    const auto before = s->raft.election_timeout;

    ReadIndexHeartbeatRequest req;
    req.rpc.term = 4;
    ReadIndexHeartbeatResponse rsp;
    s->handle_read_index_heartbeat(req, rsp);

    CHECK_FALSE(rsp.ok);
    CHECK(rsp.rpc.term == 5);
    CHECK(s->raft.election_timeout == before);
}

TEST_CASE("handle_read_index_heartbeat: 같은 term 의 Candidate 는 Follower 가 된다") {
    auto s = make_leader(3, 5, 1);
    s->raft.state = ServerState::Candidate;

    ReadIndexHeartbeatRequest req;
    req.rpc.term = 5;
    ReadIndexHeartbeatResponse rsp;
    s->handle_read_index_heartbeat(req, rsp);

    CHECK(rsp.ok);
    CHECK(s->raft.state == ServerState::Follower);
}

TEST_CASE("handle_read_index_heartbeat: 리더는 같은 term heartbeat 를 인정하지 않는다") {
    auto s = make_leader(3, 5, 1);
    ReadIndexHeartbeatRequest req;
    req.rpc.term = 5;
    ReadIndexHeartbeatResponse rsp;
    s->handle_read_index_heartbeat(req, rsp);
    CHECK_FALSE(rsp.ok);
    CHECK(s->raft.state == ServerState::Leader);
}

/* ---------------- 리더 측 부기 ---------------- */

TEST_CASE("apply_read_hb_response: 3노드는 팔로워 1명의 ack 로 과반") {
    auto s = make_leader(3, 5, 1);
    s->raft.read_round = 3;

    ReadIndexHeartbeatRequest req;
    req.rpc.term = 5;
    req.round = 3;
    ReadIndexHeartbeatResponse rsp;
    ack_same_term(req, rsp);

    std::lock_guard<std::mutex> lk(s->mu);
    s->apply_read_hb_response(1, req, rsp);
    CHECK(s->raft.cluster[1].read_ack_round == 3);
    CHECK(s->raft.read_confirmed_round == 3);
}

TEST_CASE("apply_read_hb_response: 5노드는 팔로워 2명이 ack 해야 과반") {
    auto s = make_leader(5, 5, 1);
    s->raft.read_round = 7;
    std::lock_guard<std::mutex> lk(s->mu);

    ReadIndexHeartbeatRequest req;
    req.rpc.term = 5;
    ReadIndexHeartbeatResponse rsp;

    req.round = 7;
    ack_same_term(req, rsp);
    s->apply_read_hb_response(1, req, rsp);
    CHECK(s->raft.read_confirmed_round == 0);

    /* 두 번째 팔로워는 더 이른 라운드까지만 확인 -> 과반은 그 라운드 */
    req.round = 6;
    ack_same_term(req, rsp);
    s->apply_read_hb_response(2, req, rsp);
    CHECK(s->raft.read_confirmed_round == 6);

    req.round = 7;
    ack_same_term(req, rsp);
    s->apply_read_hb_response(3, req, rsp);
    CHECK(s->raft.read_confirmed_round == 7);
}

TEST_CASE("apply_read_hb_response: ok=false / 보낸 뒤 term 변경 / 리더 아님 은 무시") {
    auto s = make_leader(3, 5, 1);
    s->raft.read_round = 2;
    std::lock_guard<std::mutex> lk(s->mu);

    ReadIndexHeartbeatRequest req;
    req.rpc.term = 5;
    req.round = 2;
    ReadIndexHeartbeatResponse rsp;
    rsp.rpc.term = 5;
    rsp.ok = false;
    s->apply_read_hb_response(1, req, rsp);
    CHECK(s->raft.read_confirmed_round == 0);

    /* 이전 term 에 보낸 요청에 대한 늦은 응답 */
    ReadIndexHeartbeatRequest old_req = req;
    old_req.rpc.term = 4;
    ReadIndexHeartbeatResponse old_rsp;
    old_rsp.rpc.term = 4;
    old_rsp.ok = true;
    s->apply_read_hb_response(1, old_req, old_rsp);
    CHECK(s->raft.read_confirmed_round == 0);

    s->raft.state = ServerState::Follower;
    ack_same_term(req, rsp);
    s->apply_read_hb_response(1, req, rsp);
    CHECK(s->raft.read_confirmed_round == 0);
}

/* ---------------- Server::read ---------------- */

TEST_CASE("read: 리더가 아니면 not leader") {
    auto s = make_leader(3, 5, 1);
    s->raft.state = ServerState::Follower;
    ReadResult r = s->read({bytes("q")});
    CHECK(r.error == "not leader");
}

TEST_CASE("read: 빈 queries 는 리더 확인만 하고 라운드를 쓰지 않는다") {
    auto s = make_leader(3, 5, 1);
    ReadResult r = s->read({});
    CHECK(r.error.empty());
    CHECK(r.results.empty());
    CHECK(s->raft.read_round == 0);
}

TEST_CASE("read: 단일 노드는 heartbeat 없이 상태머신에서 읽는다") {
    auto s = make_leader(1, 5, 1);
    ReadResult r = s->read({bytes("a"), bytes("b")});
    REQUIRE(r.error.empty());
    REQUIRE(r.results.size() == 2);
    CHECK(r.results[0].result == bytes("a"));
    CHECK(r.results[1].result == bytes("b"));
}

TEST_CASE("read: 3노드, 팔로워 ack 로 확인 후 결과를 돌려준다") {
    auto s = make_leader(3, 5, 1);
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    ft->on_read_index_heartbeat = [](int, const ReadIndexHeartbeatRequest &req,
                                     ReadIndexHeartbeatResponse &rsp) {
        return ack_same_term(req, rsp);
    };
    s->transport = ft;
    s->start_read_hb_workers();

    ReadTimings t;
    ReadResult r = s->read({bytes("q")}, &t);
    REQUIRE(r.error.empty());
    CHECK(r.results.at(0).result == bytes("q"));
    CHECK(s->raft.read_confirmed_round >= 1);
    CHECK(t.total_ns >= t.confirm_ns);
}

TEST_CASE("read: 과반이 응답하지 않으면 busy (read quorum timeout)") {
    auto s = make_leader(3, 5, 1);
    s->read_timeout_ms = 60;
    auto ft = std::make_shared<testing::FakeRaftTransport>();   /* 훅 없음 = 전송 실패 */
    s->transport = ft;
    s->start_read_hb_workers();

    ReadResult r = s->read({bytes("q")});
    CHECK(r.busy);
    CHECK(r.error == "read quorum timeout");
    /* 대기 중 kReadRekickMs 마다 다시 찔렀다 */
    std::lock_guard<std::mutex> lk(ft->read_mu);
    CHECK(ft->read_hb_calls >= 2);
}

TEST_CASE("read: read_index 까지 apply 될 때까지 기다린 뒤 읽는다") {
    auto s = make_leader(3, 5, 3);
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    ft->on_read_index_heartbeat = [](int, const ReadIndexHeartbeatRequest &req,
                                     ReadIndexHeartbeatResponse &rsp) {
        return ack_same_term(req, rsp);
    };
    s->transport = ft;
    s->start_read_hb_workers();

    /* 인덱스 3 까지 커밋됐지만 2 까지만 apply 됨 */
    s->raft.last_applied = 3;
    s->raft.log[3].command = bytes("put");

    std::atomic<bool> read_done{false};
    ReadResult r;
    std::thread reader([&]() {
        r = s->read({bytes("get")});
        read_done.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CHECK_FALSE(read_done.load());

    s->apply_pending();   /* 인덱스 3 apply -> 대기자를 깨운다 */
    reader.join();

    REQUIRE(r.error.empty());
    auto *sm = static_cast<RecordingStateMachine *>(s->statemachine.get());
    std::lock_guard<std::mutex> lk(sm->mu);
    REQUIRE(sm->calls.size() == 2);
    CHECK(sm->calls[0] == bytes("put"));   /* apply 가 먼저, */
    CHECK(sm->calls[1] == bytes("get"));   /* 읽기가 나중 */
}

TEST_CASE("read: term_start_index 가 커밋되기 전에는 읽지 않는다") {
    auto s = make_leader(3, 5, 2);
    s->read_timeout_ms = 60;
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    ft->on_read_index_heartbeat = [](int, const ReadIndexHeartbeatRequest &req,
                                     ReadIndexHeartbeatResponse &rsp) {
        return ack_same_term(req, rsp);
    };
    s->transport = ft;
    s->start_read_hb_workers();

    /* 새 리더: no-op(인덱스 2) 이 아직 커밋 전 */
    s->raft.commit_index = 1;
    s->raft.last_applied = 2;
    s->raft.term_start_index = 2;

    ReadResult r = s->read({bytes("q")});
    CHECK(r.busy);
    CHECK(r.error == "read apply timeout");
}

TEST_CASE("read: AE 가 막혀 있어도 heartbeat 는 지나간다 (AE 와 독립)") {
    auto s = make_leader(3, 5, 1);
    auto ft = std::make_shared<testing::FakeRaftTransport>();

    std::mutex gate_mu;
    std::condition_variable gate_cv;
    bool release = false;
    ft->on_append_entries = [&](int, const AppendEntriesRequest &, AppendEntriesResponse &) {
        std::unique_lock<std::mutex> g(gate_mu);
        gate_cv.wait(g, [&] { return release; });   /* 진행 중인 데이터 AE 흉내 */
        return false;
    };
    ft->on_read_index_heartbeat = [](int, const ReadIndexHeartbeatRequest &req,
                                     ReadIndexHeartbeatResponse &rsp) {
        return ack_same_term(req, rsp);
    };
    s->transport = ft;
    s->start_read_hb_workers();

    /* 팔로워 1 로 AE 를 보내 막아 둔다 (FakeRaftTransport::mu 를 쥔 채 블록 --
     * 실제 전송에서 AE 커넥션의 call_mu 를 쥔 채 blockcopy 를 기다리는 것과 같다) */
    std::thread ae([&]() {
        AppendEntriesRequest req;
        AppendEntriesResponse rsp;
        ft->append_entries(1, req, rsp);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    ReadResult r = s->read({bytes("q")});
    CHECK(r.error.empty());

    {
        std::lock_guard<std::mutex> g(gate_mu);
        release = true;
    }
    gate_cv.notify_all();
    ae.join();
}

TEST_CASE("read: 동시에 들어온 읽기들은 적은 수의 heartbeat 라운드로 확인된다") {
    auto s = make_leader(3, 5, 1);
    auto ft = std::make_shared<testing::FakeRaftTransport>();
    ft->on_read_index_heartbeat = [](int, const ReadIndexHeartbeatRequest &req,
                                     ReadIndexHeartbeatResponse &rsp) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));   /* 네트워크 RTT */
        return ack_same_term(req, rsp);
    };
    s->transport = ft;
    s->start_read_hb_workers();

    constexpr int kReaders = 16;
    std::vector<std::thread> readers;
    std::atomic<int> ok{0};
    for (int i = 0; i < kReaders; i++) {
        readers.emplace_back([&]() {
            if (s->read({bytes("q")}).error.empty()) {
                ok++;
            }
        });
    }
    for (auto &t : readers) {
        t.join();
    }
    CHECK(ok.load() == kReaders);
    /* 팔로워 2명 x 라운드 수. 라운드당 하나씩이면 2*16 = 32 */
    std::lock_guard<std::mutex> lk(ft->read_mu);
    CHECK(ft->read_hb_calls < 2 * kReaders);
}
