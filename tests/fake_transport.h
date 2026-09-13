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

    /* 관측값 (여러 스레드가 호출할 수 있으므로 mu로 보호한다) */
    std::mutex mu;
    int ae_calls = 0;
    int vote_calls = 0;
    int last_ae_peer = -1;
    int last_vote_peer = -1;
    AppendEntriesRequest last_ae_req;
    RequestVoteRequest last_vote_req;
};

class FakeBlockCopyClient : public BlockCopyClient {
public:
    /* fail_with가 비어 있지 않으면 그 메시지로 실패한다. */
    std::string fail_with;

    bool write_pba_batch(const std::vector<uint64_t> &pba_srcs,
                          const std::vector<uint64_t> &pba_dsts,
                          const std::vector<uint64_t> &nbytes,
                          int src_dev, int dst_dev,
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
