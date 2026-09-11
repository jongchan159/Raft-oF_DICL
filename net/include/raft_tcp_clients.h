#ifndef RAFT_TCP_CLIENTS_H
#define RAFT_TCP_CLIENTS_H

#include "raft_transport.h"
#include "raft_tcp_transport.h"

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

/* ============================================================
 * core/include/raft_transport.h 의 두 인터페이스에 대한 TCP 구현.
 *
 * RDMA로 갈 때는 이 파일과 짝이 되는 raft_rdma_clients.h 를 추가하고
 * main에서 어느 구현을 주입할지만 고르면 된다 -- core/ 는 물론
 * raft_tcp_server.h 도 건드릴 필요가 없다.
 *
 * 두 클래스 모두 **커넥션 상태를 자기 안에** 갖는다. 예전에는 그 상태가
 * core의 ClusterMember::rpc_client / next_dial_time 과 Server::blkcopy_rpc
 * 였고, 전송 구현체가 s.mu를 잡아 그걸 갱신했다.
 * ============================================================ */

namespace nvmeof_raft {

/* 피어 하나의 주소. cluster 인덱스 순서로 넘긴다. */
struct PeerEndpoint {
    uint64_t id = 0;
    std::string address;   /* "host:port" */
};

/* ============================================================
 * TcpRaftTransport -- 피어로의 AppendEntries / RequestVote.
 *
 * 원본 raft.go의 rpcCall 동작을 그대로 유지한다:
 *   "Read cached state without holding the lock during the dial... lazy
 *    connect with backoff on failure, close and clear on RPC error so the
 *    next call re-dials."
 * ============================================================ */
class TcpRaftTransport : public RaftTransport {
public:
    explicit TcpRaftTransport(std::vector<PeerEndpoint> peers);

    bool append_entries(int peer_index, const AppendEntriesRequest &req,
                         AppendEntriesResponse &rsp) override;
    bool request_vote(int peer_index, const RequestVoteRequest &req,
                       RequestVoteResponse &rsp) override;

private:
    struct Peer {
        uint64_t id = 0;
        std::string address;
        /* 아래 둘은 mu_로 보호된다. dial 자체는 락 밖에서 한다. */
        std::shared_ptr<RpcClientHandle> client;
        std::chrono::steady_clock::time_point next_dial_time{};
    };

    /* 실제 호출. 인코딩/디코딩은 호출자가 넘긴 람다가 한다. */
    bool call_peer(int peer_index, const std::string &method,
                   const std::vector<uint8_t> &req_body,
                   std::vector<uint8_t> *out_rsp_body);

    std::mutex mu_;
    std::vector<Peer> peers_;
};

/* ============================================================
 * TcpBlockCopyClient -- 스토리지 노드로의 PBA 배치 복사.
 *
 * lazy connect: 첫 호출에서 연결하고, 연결이 실패하면 핸들을 null로 남겨
 * 다음 호출이 다시 시도한다 ("server_random이 아직 안 떠 있어서 초기 연결이
 * 실패한 경우 재시작 없이 복구되게" -- 원본 주석).
 * ============================================================ */
class TcpBlockCopyClient : public BlockCopyClient {
public:
    /* storage_host에 ":"가 없으면 스토리지 노드의 기본 포트를 붙인다. */
    explicit TcpBlockCopyClient(std::string storage_host);

    bool write_pba_batch(const std::vector<uint64_t> &pba_srcs,
                          const std::vector<uint64_t> &pba_dsts,
                          const std::vector<uint64_t> &nbytes,
                          int src_dev, int dst_dev,
                          int64_t *out_copy_ns,
                          std::string *out_error) override;

private:
    std::string host_;                            /* 비어 있으면 설정되지 않은 것 */
    std::mutex mu_;                               /* handle_ 보호 */
    std::shared_ptr<RpcClientHandle> handle_;     /* lazy */
};

} /* namespace nvmeof_raft */

#endif /* RAFT_TCP_CLIENTS_H */
