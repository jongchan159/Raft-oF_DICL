#include "raft_blockcopy_tcp_server.h"
#include "raft_rdma_transport.h"

#include "raft_proto_conv.h"
#include "raft_rpc_methods.h"

#include <string>
#include <vector>

/* net/include/raft_blockcopy_tcp_server.h 의 구현. 선언은 그 헤더를 볼 것. */

namespace nvmeof_raft {

bool dispatch_blockcopy_method(blockcopy::BlockCopyServer *bcs,
                                       const std::string &method,
                                       const std::vector<uint8_t> &body,
                                       std::vector<uint8_t> &rsp_body) {
    if (method == rpc_method::kWritePBABatch) {
        rpcproto::WritePBABatchRequest proto_req;
        proto_req.ParseFromArray(body.data(), static_cast<int>(body.size()));
        blockcopy::WritePBABatchReq req = write_pba_batch_request_from_proto(proto_req);

        blockcopy::WritePBABatchRsp rsp = bcs->handle_write_pba_batch(req);

        rpcproto::WritePBABatchResponse proto_rsp;
        write_pba_batch_response_to_proto(rsp, &proto_rsp);
        rsp_body.resize(static_cast<size_t>(proto_rsp.ByteSizeLong()));
        proto_rsp.SerializeToArray(rsp_body.data(), static_cast<int>(rsp_body.size()));
        return true;
    }
    return false;
}

void blockcopy_serve_connection(int fd, blockcopy::BlockCopyServer *bcs) {
    serve_rpc_connection(fd, "blockcopy",
        [bcs](const std::string &method, const std::vector<uint8_t> &body,
              std::vector<uint8_t> &rsp_body) {
            return dispatch_blockcopy_method(bcs, method, body, rsp_body);
        });
}

void run_blockcopy_tcp_server(int port, blockcopy::BlockCopyServer *bcs,
                                      std::atomic<bool> *stop_flag) {
    run_rpc_listener(port, "blockcopy",
                     [bcs](int conn_fd) { blockcopy_serve_connection(conn_fd, bcs); },
                     stop_flag);
}

void run_blockcopy_server(TransportKind kind, int port, blockcopy::BlockCopyServer *bcs,
                           std::atomic<bool> *stop_flag) {
    if (kind == TransportKind::Rdma) {
        run_rdma_listener(port, "blockcopy",
            [bcs](const std::string &method, const std::vector<uint8_t> &body,
                  std::vector<uint8_t> &rsp_body) {
                return dispatch_blockcopy_method(bcs, method, body, rsp_body);
            },
            stop_flag);
        return;
    }
    run_blockcopy_tcp_server(port, bcs, stop_flag);
}

} /* namespace nvmeof_raft */
