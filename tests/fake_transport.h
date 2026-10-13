#ifndef RAFT_TESTS_FAKE_TRANSPORT_H
#define RAFT_TESTS_FAKE_TRANSPORT_H

#include "raft_transport.h"

#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

/* ============================================================
 * 테스트용 전송 구현체.
 *
 * 리팩토링 전에는 이런 것을 만들 수 없었다. core/ 가 전송을 **링크타임
 * 자유 함수 5개**로 가져갔기 때문에, 테스트는 그 다섯 심볼을 대신 정의하는
 * .cpp를 링크하는 방법밖에 없었고 -- 한 바이너리에 구현이 하나뿐이며 응답을
 * 스크립트하려면 전역 상태를 써야 했다.
 *
 * 이제 RaftTransport / BlockCopyClient는 평범한 추상 클래스이므로 아래는
 * 평범한 Mock이다: 테스트마다 인스턴스를 만들어 Server에 주입하고, 응답을
 * 람다로 주고, 무엇이 오갔는지 확인한다. 전역 상태가 없다.
 * ============================================================ */

namespace nvmeof_raft {
namespace testing {

class FakeRaftTransport : public RaftTransport {
public:
    /* 응답 훅. 비워 두면 "전송 실패"(false)로 동작한다 -- 실제 TCP 구현이
     * dial에 실패했을 때와 같다. */
    std::function<bool(int peer_index, const AppendEntriesRequest &,
                       AppendEntriesResponse &)> on_append_entries;
    std::function<bool(int peer_index, const RequestVoteRequest &,
                       RequestVoteResponse &)> on_request_vote;
    std::function<bool(int peer_index, const ReadIndexHeartbeatRequest &,
                       ReadIndexHeartbeatResponse &)> on_read_index_heartbeat;

    bool append_entries(int peer_index, const AppendEntriesRequest &req,
                         AppendEntriesResponse &rsp) override {
        std::lock_guard<std::mutex> lk(mu);
        ae_calls++;
        last_ae_peer = peer_index;
        last_ae_req = req;
        if (!on_append_entries) {
            return false;
        }
        return on_append_entries(peer_index, req, rsp);
    }

    bool request_vote(int peer_index, const RequestVoteRequest &req,
                       RequestVoteResponse &rsp) override {
        std::lock_guard<std::mutex> lk(mu);
        vote_calls++;
        last_vote_peer = peer_index;
        last_vote_req = req;
        if (!on_request_vote) {
            return false;
        }
        return on_request_vote(peer_index, req, rsp);
    }

    /* ReadIndex heartbeat 는 mu 를 **전혀 잡지 않는다** -- append_entries 가
     * mu 를 쥔 채 훅을 부르므로, 같은 락을 쓰면 AE 훅이 블록된 동안
     * heartbeat 도 막힌다. AE 와 독립인지 확인하는 테스트가 있다
     * (tests/test_read_index.cpp). 카운터와 훅은 read_mu 로 보호한다. */
    bool read_index_heartbeat(int peer_index, const ReadIndexHeartbeatRequest &req,
                              ReadIndexHeartbeatResponse &rsp) override {
        std::function<bool(int, const ReadIndexHeartbeatRequest &,
                           ReadIndexHeartbeatResponse &)> hook;
        {
            std::lock_guard<std::mutex> lk(read_mu);
            read_hb_calls++;
            hook = on_read_index_heartbeat;
        }
        if (!hook) {
            return false;
        }
        return hook(peer_index, req, rsp);
    }

    /* 관측값 (여러 스레드가 호출할 수 있으므로 mu로 보호한다) */
    std::mutex mu;
    int ae_calls = 0;
    int vote_calls = 0;
    std::mutex read_mu;
    int read_hb_calls = 0;   /* read_mu 로 보호 */
    int last_ae_peer = -1;
    int last_vote_peer = -1;
    AppendEntriesRequest last_ae_req;
    RequestVoteRequest last_vote_req;
};

class FakeBlockCopyClient : public BlockCopyClient {
public:
    /* copy_ns로 돌려줄 값. fail_with가 비어 있지 않으면 그 메시지로 실패한다. */
    int64_t copy_ns = 0;
    std::string fail_with;

    bool write_pba_batch(const std::vector<uint64_t> &pba_srcs,
                          const std::vector<uint64_t> &pba_dsts,
                          const std::vector<uint64_t> &nbytes,
                          int src_dev, int dst_dev,
                          int64_t *out_copy_ns,
                          std::string *out_error) override {
        std::lock_guard<std::mutex> lk(mu);
        calls++;
        last_srcs = pba_srcs;
        last_dsts = pba_dsts;
        last_nbytes = nbytes;
        last_src_dev = src_dev;
        last_dst_dev = dst_dev;
        if (!fail_with.empty()) {
            *out_error = fail_with;
            return false;
        }
        *out_copy_ns = copy_ns;
        return true;
    }

    std::mutex mu;
    int calls = 0;
    int last_src_dev = -1;
    int last_dst_dev = -1;
    std::vector<uint64_t> last_srcs, last_dsts, last_nbytes;
};

} /* namespace testing */
} /* namespace nvmeof_raft */

#endif /* RAFT_TESTS_FAKE_TRANSPORT_H */
