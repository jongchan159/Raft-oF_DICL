#ifndef RAFT_RPC_CLIENTS_H
#define RAFT_RPC_CLIENTS_H

#include "raft_transport.h"   /* core/ 의 RaftTransport / BlockCopyClient */
#include "raft_rpc_conn.h"     /* RpcConn / RpcDial / TransportKind */

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

/* ============================================================
 * core/include/raft_transport.h 의 두 인터페이스에 대한 **전송 중립** 구현.
 *
 * 예전에는 TcpRaftTransport / TcpBlockCopyClient 가 (1) dial + backoff +
 * 실패 시 커넥션 무효화 상태기계와 (2) protobuf 인코딩/디코딩을 함께
 * 들고 있었다. RDMA 구현을 추가하면서 그 둘을 나눴다:
 *
 *   - (1)과 (2)는 전송과 무관하다  -> 이 파일의 RpcRaftTransport /
 *     RpcBlockCopyClient 가 한 벌만 갖는다
 *   - 전송마다 다른 것은 **커넥션을 어떻게 맺고 한 번 왕복하는가** 뿐이다
 *     -> RpcConn 인터페이스와 RpcDial 함수 두 개로 좁혔다
 *
 * 그래서 새 전송을 붙이는 비용이 "RpcConn 구현 + dialer 함수 하나"다.
 * backoff 초 수나 무효화 규칙이 전송마다 갈라질 일이 없다.
 * ============================================================ */

namespace nvmeof_raft {

/* 피어 하나의 주소. cluster 인덱스 순서로 넘긴다. */
struct PeerEndpoint {
    uint64_t id = 0;
    std::string address;   /* "host:port" -- RDMA 면 IPoIB 주소여야 한다 */
};

/* ============================================================
 * RpcRaftTransport -- 피어로의 AppendEntries / RequestVote.
 *
 * 원본 raft.go 의 rpcCall 동작을 그대로 유지한다:
 *   "Read cached state without holding the lock during the dial... lazy
 *    connect with backoff on failure, close and clear on RPC error so the
 *    next call re-dials."
 * ============================================================ */
class RpcRaftTransport : public RaftTransport {
public:
    RpcRaftTransport(std::vector<PeerEndpoint> peers, RpcDial dial);

    bool append_entries(int peer_index, const AppendEntriesRequest &req,
                         AppendEntriesResponse &rsp) override;
    bool request_vote(int peer_index, const RequestVoteRequest &req,
                       RequestVoteResponse &rsp) override;

private:
    struct Peer {
        uint64_t id = 0;
        std::string address;
        /* 아래 둘은 mu_ 로 보호된다. dial 자체는 락 밖에서 한다. */
        std::shared_ptr<RpcConn> conn;
        std::chrono::steady_clock::time_point next_dial_time{};
    };

    bool call_peer(int peer_index, const std::string &method,
                   const std::vector<uint8_t> &req_body,
                   std::vector<uint8_t> *out_rsp_body);

    RpcDial dial_;
    std::mutex mu_;
    std::vector<Peer> peers_;
};

/* ============================================================
 * RpcBlockCopyClient -- 스토리지 노드로의 PBA 배치 복사.
 *
 * lazy connect: 첫 호출에서 연결하고, 실패하면 핸들을 비워 다음 호출이
 * 다시 시도한다.
 * ============================================================ */
class RpcBlockCopyClient : public BlockCopyClient {
public:
    RpcBlockCopyClient(std::string storage_host, RpcDial dial);

    bool write_pba_batch(const std::vector<uint64_t> &pba_srcs,
                          const std::vector<uint64_t> &pba_dsts,
                          const std::vector<uint64_t> &nbytes,
                          int src_dev, int dst_dev,
                          int64_t *out_copy_ns,
                          std::string *out_error) override;

private:
    std::string host_;                  /* 비어 있으면 설정되지 않은 것 */
    RpcDial dial_;
    std::mutex mu_;                     /* conn_ 보호 */
    std::shared_ptr<RpcConn> conn_;     /* lazy */
};

/* 편의 팩토리 -- apps/ 의 조립부가 쓴다. */
std::shared_ptr<RaftTransport> make_raft_transport(TransportKind kind,
                                                    std::vector<PeerEndpoint> peers);
std::shared_ptr<BlockCopyClient> make_blockcopy_client(TransportKind kind,
                                                        const std::string &storage_host);

} /* namespace nvmeof_raft */

#endif /* RAFT_RPC_CLIENTS_H */
