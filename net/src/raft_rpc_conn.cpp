/* ============================================================
 * raft_rpc_conn.cpp -- 전송별 커넥션 구현과 dialer.
 *
 * 여기가 TCP 와 RDMA 가 갈리는 **유일한** 지점이다. 위쪽
 * (RpcRaftTransport / RpcBlockCopyClient 의 dial backoff, 무효화,
 * protobuf 인코딩)과 아래쪽(메서드 디스패치)은 전송을 모른다.
 * ============================================================ */
#include "raft_rpc_conn.h"

#include "raft_tcp_transport.h"    /* tcp_dial_http / rpc_invoke */
#include "raft_rdma_transport.h"   /* rdma_dial / rdma_invoke */

namespace nvmeof_raft {

namespace {

class TcpRpcConn : public RpcConn {
public:
    explicit TcpRpcConn(RpcClientHandle *h) : h_(h) {}
    ~TcpRpcConn() override { delete h_; }

    std::vector<uint8_t> invoke(const std::string &method,
                                 const std::vector<uint8_t> &req_body) override {
        /* 커넥션당 직렬화는 rpc_invoke 안의 call_mu 가 한다. */
        return rpc_invoke(h_, method, req_body);
    }

private:
    RpcClientHandle *h_;
};

class RdmaRpcConn : public RpcConn {
public:
    explicit RdmaRpcConn(RdmaClientHandle *h) : h_(h) {}
    ~RdmaRpcConn() override { rdma_close(h_); }

    std::vector<uint8_t> invoke(const std::string &method,
                                 const std::vector<uint8_t> &req_body) override {
        /* 커넥션당 직렬화는 rdma_invoke 안의 call_mu 가 한다 (QP 하나에
         * 여러 스레드가 post_send 를 섞으면 응답을 짝지을 수 없다). */
        return rdma_invoke(h_, method, req_body);
    }

private:
    RdmaClientHandle *h_;
};

} /* anonymous namespace */

RpcDial tcp_dialer() {
    return [](const std::string &address) -> std::shared_ptr<RpcConn> {
        return std::make_shared<TcpRpcConn>(tcp_dial_http(address));
    };
}

RpcDial rdma_dialer() {
    return [](const std::string &address) -> std::shared_ptr<RpcConn> {
        return std::make_shared<RdmaRpcConn>(rdma_dial(address));
    };
}

RpcDial dialer_for(TransportKind kind) {
    return kind == TransportKind::Rdma ? rdma_dialer() : tcp_dialer();
}

bool parse_transport_kind(const std::string &s, TransportKind *out) {
    if (s == "rdma") { *out = TransportKind::Rdma; return true; }
    if (s == "tcp")  { *out = TransportKind::Tcp;  return true; }
    return false;
}

const char *transport_kind_name(TransportKind k) {
    return k == TransportKind::Rdma ? "rdma" : "tcp";
}

} /* namespace nvmeof_raft */
