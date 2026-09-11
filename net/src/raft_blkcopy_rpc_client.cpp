/* ============================================================
 * TcpBlockCopyClient -- core/raft_transport.h의 BlockCopyClient TCP 구현.
 *
 * core/do_pba_copy가 호출하는 "스토리지 노드로의 blockcopy RPC".
 * raft.go의
 *   blockcopy.RPCConnectRDMA(storageHost)  -> 여기의 lazy connect
 *   s.blkcopyRPC.WritePBABatch(...)        -> write_pba_batch
 * 에 대응하고, 전송만 TCP + Go net/rpc 프레이밍으로 대체했다.
 * RDMA 결선 시 이 파일에 대응하는 구현체를 하나 더 만들고 main에서 그걸
 * 주입하면 된다.
 *
 * 리팩토링으로 달라진 점: lazy connect와 그 직렬화가 **이 클래스 안에**
 * 있다. 예전에는 Server::blkcopy_rpc + Server::blkcopy_mu +
 * Server::blkcopy_client() 가 그 일을 했다.
 * ============================================================ */
#include "raft_tcp_clients.h"
#include "raft_proto_conv.h"
#include "raft_rpc_methods.h"

#include <utility>

namespace nvmeof_raft {

namespace {
/* 스토리지 노드(raft_blockcopy_server)의 기본 포트.
 * -addr 기본값 0.0.0.0:5050과 맞춘다. */
constexpr int kDefaultBlockCopyPort = 5050;
}  /* anonymous namespace */

TcpBlockCopyClient::TcpBlockCopyClient(std::string storage_host)
    : host_(std::move(storage_host)) {
    if (!host_.empty() && host_.find(':') == std::string::npos) {
        host_ += ":" + std::to_string(kDefaultBlockCopyPort);
    }
}

bool TcpBlockCopyClient::write_pba_batch(const std::vector<uint64_t> &pba_srcs,
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
     * 연결이 실패하면 handle_를 null로 남긴다 -> 다음 호출이 다시 시도한다
     * ("server_random이 아직 안 떠 있어서 초기 연결이 실패한 경우
     *   재시작 없이 복구되게" -- 원본 주석). */
    std::shared_ptr<RpcClientHandle> handle;
    {
        std::lock_guard<std::mutex> lk(mu_);
        if (!handle_) {
            try {
                handle_.reset(tcp_dial_http(host_));
            } catch (const std::exception &e) {
                *out_error = std::string("connect to storage node failed: ") + e.what();
                return false;
            }
        }
        handle = handle_;
    }

    blockcopy::WritePBABatchReq req;
    req.pba_srcs = pba_srcs;
    req.pba_dsts = pba_dsts;
    req.nbytes = nbytes;
    req.block_size = 0;   /* Nbytes를 쓰므로 legacy uniform-size 경로 미사용 */
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
        /* 커넥션당 직렬화는 rpc_invoke 안에서 한다. 팔로워의 AppendEntries
         * 핸들러는 커넥션마다 스레드가 붙고 그 전부가 이 클라이언트 하나를
         * 공유하므로 그 직렬화가 없으면 프레임이 섞인다. */
        rsp_body = rpc_invoke(handle.get(), rpc_method::kWritePBABatch, body,
                              "blkcopy remote error: ");
    } catch (const std::exception &e) {
        *out_error = e.what();
        /* 주의: 여기서 handle_를 비우지 **않는다.** 리팩토링 전 동작을 그대로
         * 유지한 것이다 -- Server::blkcopy_reset()이 존재하기만 하고 호출부가
         * 하나도 없었다. 그래서 스토리지 커넥션이 한 번 죽으면 이후 모든 PBA
         * 복사가 영구히 실패한다 (DECISIONS.md U6).
         * 이건 버그이고, 이번 리팩토링의 범위(동작 불변)를 벗어나므로 고치지
         * 않았다. 고치려면 여기서 `std::lock_guard lk(mu_); if (handle_ ==
         * handle) handle_.reset();` 한 줄이면 된다 -- TcpRaftTransport의
         * call_peer가 하는 것과 같은 처리다. */
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
         * 커넥션 자체는 살아있으므로 닫지 않는다 -- 호출부(do_pba_copy ->
         * handle_append_entries_request / append_entries_worker)가 fail-soft로
         * 이 배치를 건너뛰고 다음 라운드에 재시도한다. */
        *out_error = "blkcopy storage error: " + rsp.error;
        return false;
    }

    *out_copy_ns = rsp.copy_nanos;
    return true;
}

} /* namespace nvmeof_raft */
