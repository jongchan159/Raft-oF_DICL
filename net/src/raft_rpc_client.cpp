/* ============================================================
 * TcpRaftTransport -- core/raft_transport.h의 RaftTransport TCP 구현.
 *
 * 원본 raft.go의 s.rpcCall 로직을 그대로 옮긴 것이다:
 *   "Read cached state without holding the lock during the dial... lazy
 *    connect with backoff on failure, close and clear on RPC error so the
 *    next call re-dials."
 *
 * 원본과 다른 점: 요청/응답 인코딩(protobuf)까지 여기서 한다. 원본은
 * Go net/rpc가 codec을 통해 자동으로 처리하지만 이 포팅은
 * WireClientCodec을 직접 다룬다.
 *
 * 리팩토링으로 달라진 점: dial/backoff/무효화 상태가 **이 클래스 안에**
 * 있다. 예전에는 같은 로직이 Server*와 cluster 인덱스를 받아
 * s->mu를 잡고 s->cluster[i].rpc_client / next_dial_time 을 갱신했다.
 * 동작(2초 backoff, 실패 시 커넥션 폐기, 경쟁 시 나중 것 버리기)은 동일하다.
 * ============================================================ */
#include "raft_tcp_clients.h"
#include "raft_proto_conv.h"
#include "raft_rpc_methods.h"

#include <iostream>
#include <utility>

namespace nvmeof_raft {

namespace {
using clock_type = std::chrono::steady_clock;

/* dial 실패 후 재시도까지 기다리는 시간 (원본 그대로 2초) */
constexpr int kDialBackoffSeconds = 2;
}  /* anonymous namespace */

TcpRaftTransport::TcpRaftTransport(std::vector<PeerEndpoint> peers) {
    peers_.reserve(peers.size());
    for (auto &p : peers) {
        Peer peer;
        peer.id = p.id;
        peer.address = std::move(p.address);
        peers_.push_back(std::move(peer));
    }
}

bool TcpRaftTransport::call_peer(int peer_index, const std::string &method,
                                  const std::vector<uint8_t> &req_body,
                                  std::vector<uint8_t> *out_rsp_body) {
    if (peer_index < 0 || static_cast<size_t>(peer_index) >= peers_.size()) {
        return false;
    }
    Peer &peer = peers_[static_cast<size_t>(peer_index)];

    /* "Read cached state without holding the lock during the dial." */
    std::shared_ptr<RpcClientHandle> client;
    std::string addr;
    uint64_t id = 0;
    clock_type::time_point next_dial;
    {
        std::lock_guard<std::mutex> lk(mu_);
        client = peer.client;
        addr = peer.address;
        id = peer.id;
        next_dial = peer.next_dial_time;
    }

    bool dial_failed = false;
    if (!client) {
        if (clock_type::now() < next_dial) {
            return false;   /* backoff 중 */
        }
        try {
            std::shared_ptr<RpcClientHandle> fresh(tcp_dial_http(addr));

            std::lock_guard<std::mutex> lk(mu_);
            if (!peer.client) {
                peer.client = fresh;
                peer.next_dial_time = clock_type::time_point{};
            }
            /* else: 다른 스레드가 먼저 연결했다 -- fresh가 스코프를 벗어나며
             * 자동으로 닫힌다 (원본의 newClient.Close()에 대응) */
            client = peer.client;
        } catch (const std::exception &e) {
            std::cerr << "[warn] Dial failed to " << id << ": " << e.what() << std::endl;
            std::lock_guard<std::mutex> lk(mu_);
            peer.next_dial_time =
                clock_type::now() + std::chrono::seconds(kDialBackoffSeconds);
            dial_failed = true;
        }
    }
    if (dial_failed || !client) {
        return false;
    }

    try {
        *out_rsp_body = rpc_invoke(client.get(), method, req_body);
        return true;
    } catch (const std::exception &e) {
        std::cerr << "[warn] Error calling " << method << " on " << id
                  << ": " << e.what() << std::endl;
        /* 실패한 커넥션은 버려서 다음 호출이 재연결하게 한다. 우리가 들고
         * 있던 것과 같은 핸들일 때만 지운다 (그 사이 다른 스레드가 새로
         * 연결했을 수 있다). 로컬 shared_ptr이 스코프를 벗어나며 fd가
         * 닫힌다 -- 원본의 explicit Close()에 대응. */
        std::lock_guard<std::mutex> lk(mu_);
        if (peer.client == client) {
            peer.client = nullptr;
        }
        return false;
    }
}

bool TcpRaftTransport::append_entries(int peer_index, const AppendEntriesRequest &req,
                                       AppendEntriesResponse &rsp) {
    rpcproto::AppendEntriesRequest proto_req;
    append_entries_request_to_proto(req, &proto_req);
    std::vector<uint8_t> req_body(static_cast<size_t>(proto_req.ByteSizeLong()));
    proto_req.SerializeToArray(req_body.data(), static_cast<int>(req_body.size()));

    std::vector<uint8_t> rsp_body;
    if (!call_peer(peer_index, rpc_method::kAppendEntries, req_body, &rsp_body)) {
        return false;
    }

    rpcproto::AppendEntriesResponse proto_rsp;
    if (!proto_rsp.ParseFromArray(rsp_body.data(), static_cast<int>(rsp_body.size()))) {
        std::cerr << "[warn] " << rpc_method::kAppendEntries
                  << ": failed to parse response" << std::endl;
        return false;
    }
    append_entries_response_from_proto(proto_rsp, rsp);
    return true;
}

bool TcpRaftTransport::request_vote(int peer_index, const RequestVoteRequest &req,
                                     RequestVoteResponse &rsp) {
    rpcproto::RequestVoteRequest proto_req;
    request_vote_request_to_proto(req, &proto_req);
    std::vector<uint8_t> req_body(static_cast<size_t>(proto_req.ByteSizeLong()));
    proto_req.SerializeToArray(req_body.data(), static_cast<int>(req_body.size()));

    std::vector<uint8_t> rsp_body;
    if (!call_peer(peer_index, rpc_method::kRequestVote, req_body, &rsp_body)) {
        return false;
    }

    rpcproto::RequestVoteResponse proto_rsp;
    if (!proto_rsp.ParseFromArray(rsp_body.data(), static_cast<int>(rsp_body.size()))) {
        std::cerr << "[warn] " << rpc_method::kRequestVote
                  << ": failed to parse response" << std::endl;
        return false;
    }
    request_vote_response_from_proto(proto_rsp, rsp);
    return true;
}

} /* namespace nvmeof_raft */
