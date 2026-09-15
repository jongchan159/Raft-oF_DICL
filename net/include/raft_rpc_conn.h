#ifndef RAFT_RPC_CONN_H
#define RAFT_RPC_CONN_H

#include <functional>
#include <memory>
#include <string>
#include <vector>

/* ============================================================
 * 전송 중립 RPC 커넥션.
 *
 * 이 헤더는 **core/ 를 전혀 모른다.** 그래서 core 를 링크하지 않는
 * 바이너리(raft_client / raft_blockcopy_server / raft_blkcopy_scale)도
 * 전송을 골라 쓸 수 있다. core 인터페이스(RaftTransport /
 * BlockCopyClient)의 구현은 raft_rpc_clients.h 쪽에 있다.
 * ============================================================ */

namespace nvmeof_raft {

/* ============================================================
 * RpcConn -- 맺어진 커넥션 하나. 요청 한 번 -> 응답 한 번.
 *
 * 구현체는 **커넥션당 직렬화**를 스스로 해야 한다 (여러 스레드가 같은
 * 커넥션으로 동시에 들어온다). TCP 는 RpcClientHandle::call_mu,
 * RDMA 는 QP 당 뮤텍스가 그 역할이다.
 *
 * 실패는 예외로 알린다 (전송 오류 / 원격이 보고한 에러 둘 다).
 * ============================================================ */
class RpcConn {
public:
    virtual ~RpcConn() = default;
    virtual std::vector<uint8_t> invoke(const std::string &method,
                                         const std::vector<uint8_t> &req_body) = 0;
};

/* 주소("host:port")를 받아 커넥션을 맺는다. 실패 시 예외. */
using RpcDial = std::function<std::shared_ptr<RpcConn>(const std::string &address)>;

/* 전송 종류. apps/ 의 -transport 플래그가 이걸 고른다.
 * **기본값은 Rdma 다** -- 이 클러스터의 모든 멤버가 EDR IB 로 붙어 있고,
 * TCP 는 IB 가 없는 호스트를 위한 선택지로 남긴다. */
enum class TransportKind { Rdma, Tcp };

bool parse_transport_kind(const std::string &s, TransportKind *out);
const char *transport_kind_name(TransportKind k);

RpcDial tcp_dialer();
RpcDial rdma_dialer();
RpcDial dialer_for(TransportKind kind);

} /* namespace nvmeof_raft */

#endif /* RAFT_RPC_CONN_H */
