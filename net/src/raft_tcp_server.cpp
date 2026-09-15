#include "raft_tcp_server.h"
#include "raft_rdma_transport.h"

#include "raft_statemachine_hash.h"
#include "raft_wire_codec.h"
#include "raft_proto_conv.h"
#include "raft_rpc_methods.h"

#include <string>
#include <vector>

/* net/include/raft_tcp_server.h 의 구현. 선언은 그 헤더를 볼 것.
 *
 * **이 파일만 core 심볼을 필요로 한다** (Server::apply / apply_timed /
 * handle_append_entries_request / handle_request_vote_request / prof).
 * 그래서 NET_SRCS(= raft_node 전용 그룹)에 들어가고, raft_client 나
 * raft_blockcopy_server 가 실수로 이 그룹을 링크하면 즉시 undefined 가 난다.
 *
 * 예전에는 이 구현이 헤더에 inline 으로 있었다 -- 프로젝트에서 "헤더의
 * inline 이 다른 TU 의 심볼을 부르는" 마지막 사례였다. */

namespace nvmeof_raft {

bool dispatch_client_method(const std::string &method,
                                    const std::vector<uint8_t> &body,
                                    std::vector<uint8_t> &rsp_body,
                                    Server *server) {
    auto serialize = [&rsp_body](const auto &msg) {
        rsp_body.resize(static_cast<size_t>(msg.ByteSizeLong()));
        msg.SerializeToArray(rsp_body.data(), static_cast<int>(rsp_body.size()));
    };

    if (method == rpc_method::kClientApply) {
        rpcproto::ClientApplyRequest proto_req;
        proto_req.ParseFromArray(body.data(), static_cast<int>(body.size()));
        ClientApplyRequest req;
        client_apply_request_from_proto(proto_req, req);

        bool busy = false;
        ApplyResult res = server->apply(req.commands, &busy);

        ClientApplyResponse rsp;
        rsp.error = res.error;
        rsp.busy = busy;
        rsp.retry_after_ms = busy ? apply_busy_retry_after_ms() : 0;

        rpcproto::ClientApplyResponse proto_rsp;
        client_apply_response_to_proto(rsp, &proto_rsp);
        serialize(proto_rsp);
        return true;
    }

    if (method == rpc_method::kClientApplyTimed) {
        rpcproto::ClientApplyTimedRequest proto_req;
        proto_req.ParseFromArray(body.data(), static_cast<int>(body.size()));
        ClientApplyTimedRequest req;
        client_apply_timed_request_from_proto(proto_req, req);

        bool busy = false;
        ApplyTimings t;
        ApplyResult res = server->apply_timed(req.commands, &t, &busy);

        ClientApplyTimedResponse rsp;
        rsp.error = res.error;
        rsp.busy = busy;
        rsp.retry_after_ms = busy ? apply_busy_retry_after_ms() : 0;
        rsp.l_handler_ns = t.l_handler_ns;
        rsp.l_persist_ns = t.l_persist_ns;
        rsp.ae_net_ns = t.ae_net_ns;
        rsp.f_handler_ns = t.f_handler_ns;
        rsp.repl_net_ns = t.repl_net_ns;
        rsp.replication_ns = t.storage_io_ns;   /* 원본 Replication = StorageIO */
        rsp.quorum_wait_ns = t.quorum_wait_ns;
        rsp.mutex_ns = t.mutex_ns;
        rsp.total_ns = t.total_ns;
        rsp.post_rpc_ns = t.post_rpc_ns;
        rsp.commit_wait_ns = t.commit_wait_ns;
        rsp.wg_scheduling_ns = t.wg_scheduling_ns;

        rpcproto::ClientApplyTimedResponse proto_rsp;
        client_apply_timed_response_to_proto(rsp, &proto_rsp);
        serialize(proto_rsp);
        return true;
    }

    if (method == rpc_method::kClientEcho) {
        rpcproto::ClientEchoRequest proto_req;
        proto_req.ParseFromArray(body.data(), static_cast<int>(body.size()));
        ClientEchoRequest req;
        client_echo_request_from_proto(proto_req, req);

        /* 원본: 네트워크/코덱 왕복만 재는 no-op. 받은 바이트 수를 돌려준다 */
        ClientEchoResponse rsp;
        int64_t n = 0;
        for (const auto &c : req.commands) {
            n += static_cast<int64_t>(c.size());
        }
        rsp.n = n;

        rpcproto::ClientEchoResponse proto_rsp;
        client_echo_response_to_proto(rsp, &proto_rsp);
        serialize(proto_rsp);
        return true;
    }

    if (method == rpc_method::kClientGetCommitIndex) {
        ClientGetCommitIndexResponse rsp;
        {
            std::lock_guard<std::mutex> lk(server->mu);
            rsp.commit_index = server->raft.commit_index;
        }
        rpcproto::ClientGetCommitIndexResponse proto_rsp;
        client_get_commit_index_response_to_proto(rsp, &proto_rsp);
        serialize(proto_rsp);
        return true;
    }

    if (method == rpc_method::kClientGetHash) {
        rpcproto::ClientGetHashRequest proto_req;
        proto_req.ParseFromArray(body.data(), static_cast<int>(body.size()));
        ClientGetHashRequest req;
        client_get_hash_request_from_proto(proto_req, req);

        ClientGetHashResponse rsp;
        auto *hsm = dynamic_cast<HashStateMachine *>(server->statemachine.get());
        if (hsm == nullptr) {
            rsp.error = "state machine does not expose a hash";
        } else {
            /* at_count > 0이면 그 개수만큼 apply될 때까지 짧게 기다린다
             * (apply는 apply 워커가 비동기로 하므로 클라이언트가 커밋
             *  직후에 물어보면 아직 덜 반영돼 있을 수 있다) */
            HashStateMachine::Snapshot snap = hsm->snapshot();
            if (req.at_count > 0) {
                for (int i = 0; i < 2000 && snap.count < req.at_count; i++) {
                    std::this_thread::sleep_for(std::chrono::microseconds(500));
                    snap = hsm->snapshot();
                }
            }
            rsp.hash = snap.hash;
            rsp.count = snap.count;
            if (req.at_count > 0 && snap.count != req.at_count) {
                rsp.error = "count " + std::to_string(snap.count) +
                            " != requested " + std::to_string(req.at_count);
            }
        }
        rpcproto::ClientGetHashResponse proto_rsp;
        client_get_hash_response_to_proto(rsp, &proto_rsp);
        serialize(proto_rsp);
        return true;
    }

    if (method == rpc_method::kClientGetAEBatchStats) {
        ClientGetAEBatchStatsResponse rsp;
        rsp.ae_count = server->prof.ae_count.load();
        rsp.ae_entries = server->prof.ae_entries.load();
        rpcproto::ClientGetAEBatchStatsResponse proto_rsp;
        client_get_ae_batch_stats_response_to_proto(rsp, &proto_rsp);
        serialize(proto_rsp);
        return true;
    }

    return false;
}

bool dispatch_raft_method(Server *server, const std::string &method,
                                  const std::vector<uint8_t> &body,
                                  std::vector<uint8_t> &rsp_body) {
    if (method == rpc_method::kAppendEntries) {
        rpcproto::AppendEntriesRequest proto_req;
        proto_req.ParseFromArray(body.data(), static_cast<int>(body.size()));
        AppendEntriesRequest req;
        append_entries_request_from_proto(proto_req, req);

        AppendEntriesResponse rsp;
        server->handle_append_entries_request(req, rsp);

        rpcproto::AppendEntriesResponse proto_rsp;
        append_entries_response_to_proto(rsp, &proto_rsp);
        rsp_body.resize(static_cast<size_t>(proto_rsp.ByteSizeLong()));
        proto_rsp.SerializeToArray(rsp_body.data(), static_cast<int>(rsp_body.size()));
        return true;
    }

    if (method == rpc_method::kRequestVote) {
        rpcproto::RequestVoteRequest proto_req;
        proto_req.ParseFromArray(body.data(), static_cast<int>(body.size()));
        RequestVoteRequest req;
        request_vote_request_from_proto(proto_req, req);

        RequestVoteResponse rsp;
        server->handle_request_vote_request(req, rsp);

        rpcproto::RequestVoteResponse proto_rsp;
        request_vote_response_to_proto(rsp, &proto_rsp);
        rsp_body.resize(static_cast<size_t>(proto_rsp.ByteSizeLong()));
        proto_rsp.SerializeToArray(rsp_body.data(), static_cast<int>(rsp_body.size()));
        return true;
    }

    return dispatch_client_method(method, body, rsp_body, server);
}

void serve_connection(int fd, Server *server) {
    serve_rpc_connection(fd, "raft",
        [server](const std::string &method, const std::vector<uint8_t> &body,
                 std::vector<uint8_t> &rsp_body) {
            return dispatch_raft_method(server, method, body, rsp_body);
        });
}

void run_tcp_server(int port, Server *server,
                           std::atomic<bool> *stop_flag) {
    run_rpc_listener(port, "raft",
                     [server](int conn_fd) { serve_connection(conn_fd, server); },
                     stop_flag);
}

void run_raft_server(TransportKind kind, int port, Server *server,
                      std::atomic<bool> *stop_flag) {
    if (kind == TransportKind::Rdma) {
        run_rdma_listener(port, "raft",
            [server](const std::string &method, const std::vector<uint8_t> &body,
                     std::vector<uint8_t> &rsp_body) {
                return dispatch_raft_method(server, method, body, rsp_body);
            },
            stop_flag);
        return;
    }
    run_tcp_server(port, server, stop_flag);
}

} /* namespace nvmeof_raft */
