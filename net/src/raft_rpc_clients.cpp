/* ============================================================
 * raft_rpc_clients.cpp -- 전송 중립 RaftTransport / BlockCopyClient 구현.
 *
 * 설계 근거는 net/include/raft_rpc_clients.h 참고. 여기 있는 것은
 *   (1) dial + backoff + 실패 시 커넥션 무효화 (원본 raft.go rpcCall)
 *   (2) protobuf 인코딩/디코딩
 * 두 가지뿐이고, 둘 다 전송을 모른다. 전송별 차이는 파일 아래쪽의
 * RpcConn 구현 두 개(TCP/RDMA)와 dialer 두 개에 갇혀 있다.
 * ============================================================ */
#include "raft_rpc_clients.h"

#include "raft_proto_conv.h"
#include "raft_rpc_methods.h"

#include <iostream>
#include <utility>

namespace nvmeof_raft {

namespace {
using clock_type = std::chrono::steady_clock;

/* dial 실패 후 재시도까지 기다리는 시간 (원본 그대로 2초) */
constexpr int kDialBackoffSeconds = 2;

/* 스토리지 노드(raft_blockcopy_server)의 기본 포트.
 * -addr 기본값 0.0.0.0:5050 과 맞춘다. */
constexpr int kDefaultBlockCopyPort = 5050;

} /* anonymous namespace */

/* ============================================================
 * RpcRaftTransport
 * ============================================================ */

RpcRaftTransport::RpcRaftTransport(std::vector<PeerEndpoint> peers, RpcDial dial)
    : dial_(std::move(dial)) {
    peers_.reserve(peers.size());
    for (auto &p : peers) {
        Peer peer;
        peer.id = p.id;
        peer.address = std::move(p.address);
        peers_.push_back(std::move(peer));
    }
}

bool RpcRaftTransport::call_peer(int peer_index, const std::string &method,
                                  const std::vector<uint8_t> &req_body,
                                  std::vector<uint8_t> *out_rsp_body) {
    if (peer_index < 0 || static_cast<size_t>(peer_index) >= peers_.size()) {
        return false;
    }
    Peer &peer = peers_[static_cast<size_t>(peer_index)];

    /* "Read cached state without holding the lock during the dial." */
    std::shared_ptr<RpcConn> conn;
    std::string addr;
    uint64_t id = 0;
    clock_type::time_point next_dial;
    {
        std::lock_guard<std::mutex> lk(mu_);
        conn = peer.conn;
        addr = peer.address;
        id = peer.id;
        next_dial = peer.next_dial_time;
    }

    bool dial_failed = false;
    if (!conn) {
        if (clock_type::now() < next_dial) {
            return false;   /* backoff 중 */
        }
        try {
            std::shared_ptr<RpcConn> fresh = dial_(addr);

            std::lock_guard<std::mutex> lk(mu_);
            if (!peer.conn) {
                peer.conn = fresh;
                peer.next_dial_time = clock_type::time_point{};
            }
            /* else: 다른 스레드가 먼저 연결했다 -- fresh 가 스코프를 벗어나며
             * 자동으로 닫힌다 (원본의 newClient.Close() 에 대응) */
            conn = peer.conn;
        } catch (const std::exception &e) {
            std::cerr << "[warn] Dial failed to " << id << ": " << e.what() << std::endl;
            std::lock_guard<std::mutex> lk(mu_);
            peer.next_dial_time =
                clock_type::now() + std::chrono::seconds(kDialBackoffSeconds);
            dial_failed = true;
        }
    }
    if (dial_failed || !conn) {
        return false;
    }

    try {
        *out_rsp_body = conn->invoke(method, req_body);
        return true;
    } catch (const std::exception &e) {
        std::cerr << "[warn] Error calling " << method << " on " << id
                  << ": " << e.what() << std::endl;
        /* 실패한 커넥션은 버려서 다음 호출이 재연결하게 한다. 우리가 들고
         * 있던 것과 같은 핸들일 때만 지운다 (그 사이 다른 스레드가 새로
         * 연결했을 수 있다). */
        std::lock_guard<std::mutex> lk(mu_);
        if (peer.conn == conn) {
            peer.conn = nullptr;
        }
        return false;
    }
}

bool RpcRaftTransport::append_entries(int peer_index, const AppendEntriesRequest &req,
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

bool RpcRaftTransport::request_vote(int peer_index, const RequestVoteRequest &req,
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

/* ============================================================
 * RpcBlockCopyClient
 * ============================================================ */

RpcBlockCopyClient::RpcBlockCopyClient(std::string storage_host, RpcDial dial)
    : host_(std::move(storage_host)), dial_(std::move(dial)) {
    if (!host_.empty() && host_.find(':') == std::string::npos) {
        host_ += ":" + std::to_string(kDefaultBlockCopyPort);
    }
}

bool RpcBlockCopyClient::write_pba_batch(const std::vector<uint64_t> &pba_srcs,
                                          const std::vector<uint64_t> &pba_dsts,
                                          const std::vector<uint64_t> &nbytes,
                                          int src_dev, int dst_dev,
                                          int64_t *out_copy_ns,
                                          std::string *out_error) {
    *out_copy_ns = 0;
    if (pba_srcs.empty()) {
        return true;   /* 복사할 것이 없다 */
    }
    if (host_.empty()) {
        *out_error = "no storage host configured";
        return false;
    }

    /* ---- lazy connect ----
     * 연결이 실패하면 conn_ 를 비운 채로 둔다 -> 다음 호출이 다시 시도한다
     * ("스토리지 노드가 아직 안 떠 있어서 초기 연결이 실패한 경우 재시작
     *   없이 복구되게" -- 원본 주석). */
    std::shared_ptr<RpcConn> conn;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!conn_) {
            try {
                conn_ = dial_(host_);
            } catch (const std::exception &e) {
                *out_error = std::string("connect to storage node failed: ") + e.what();
                return false;
            }
        }
        conn = conn_;
    }

    blockcopy::WritePBABatchReq req;
    req.pba_srcs = pba_srcs;
    req.pba_dsts = pba_dsts;
    req.nbytes = nbytes;
    req.block_size = 0;   /* Nbytes 를 쓰므로 legacy uniform-size 경로 미사용 */
    req.src_dev = src_dev;
    req.dst_dev = dst_dev;

    rpcproto::WritePBABatchRequest proto_req;
    write_pba_batch_request_to_proto(req, &proto_req);
    std::vector<uint8_t> body(static_cast<size_t>(proto_req.ByteSizeLong()));
    if (!proto_req.SerializeToArray(body.data(), static_cast<int>(body.size()))) {
        *out_error = "serialize WritePBABatch request failed";
        return false;
    }

    std::vector<uint8_t> rsp_body;
    try {
        rsp_body = conn->invoke(rpc_method::kWritePBABatch, body);
    } catch (const std::exception &e) {
        *out_error = std::string("blkcopy remote error: ") + e.what();
        /* 죽은 커넥션을 버려서 다음 호출이 재연결하게 한다.
         *
         * 리팩토링 전 TcpBlockCopyClient 는 이걸 하지 않아서, 스토리지
         * 커넥션이 한 번 죽으면 이후 모든 PBA 복사가 영구히 실패했다
         * (DECISIONS.md U6). 전송 두 개가 공유하는 새 코드에 그 버그를
         * 그대로 옮길 이유가 없어 여기서 고친다 -- raft RPC 경로
         * (RpcRaftTransport::call_peer) 가 하는 것과 같은 처리다. */
        std::lock_guard<std::mutex> lk(mu_);
        if (conn_ == conn) {
            conn_ = nullptr;
        }
        return false;
    }

    rpcproto::WritePBABatchResponse proto_rsp;
    if (!proto_rsp.ParseFromArray(rsp_body.data(), static_cast<int>(rsp_body.size()))) {
        *out_error = "parse WritePBABatch response failed";
        return false;
    }

    blockcopy::WritePBABatchRsp rsp = write_pba_batch_response_from_proto(proto_rsp);
    if (!rsp.error.empty()) {
        /* 스토리지 서버가 보고한 애플리케이션 레벨 실패 (pread/pwrite 등).
         * 커넥션 자체는 살아있으므로 닫지 않는다 -- 호출부(do_pba_copy)가
         * fail-soft 로 이 배치를 건너뛰고 다음 라운드에 재시도한다. */
        *out_error = "blkcopy storage error: " + rsp.error;
        return false;
    }

    *out_copy_ns = rsp.copy_nanos;
    return true;
}

/* ---- 팩토리 ---------------------------------------------------------- */

std::shared_ptr<RaftTransport> make_raft_transport(TransportKind kind,
                                                    std::vector<PeerEndpoint> peers) {
    return std::make_shared<RpcRaftTransport>(std::move(peers), dialer_for(kind));
}

std::shared_ptr<BlockCopyClient> make_blockcopy_client(TransportKind kind,
                                                        const std::string &storage_host) {
    return std::make_shared<RpcBlockCopyClient>(storage_host, dialer_for(kind));
}

} /* namespace nvmeof_raft */
