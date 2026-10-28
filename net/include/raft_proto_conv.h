#ifndef RAFT_PROTO_CONV_HPP
#define RAFT_PROTO_CONV_HPP

#include "raft_entry.h"

#include <vector>
#include "raft_blockcopy_server.h"
#include "rpcproto.pb.h"

namespace nvmeof_raft {

/* ============================================================
 * commands 변환 템플릿
 *
 * ClientApplyRequest / ClientApplyTimedRequest / ClientEchoRequest는 셋 다
 * `repeated bytes commands = 1` 하나뿐인 동일한 형태다. 리팩토링 전에는
 * 이 왕복 변환이 **세 번 글자 그대로 복사**되어 있었다.
 * ============================================================ */
template <typename ProtoT>
inline void commands_to_proto(const std::vector<std::vector<uint8_t>> &commands,
                               ProtoT *p) {
    p->clear_commands();
    for (const auto &cmd : commands) {
        p->add_commands(reinterpret_cast<const char *>(cmd.data()), cmd.size());
    }
}

template <typename ProtoT>
inline void commands_from_proto(const ProtoT &p,
                                 std::vector<std::vector<uint8_t>> &commands) {
    commands.clear();
    commands.reserve(static_cast<size_t>(p.commands_size()));
    for (const auto &c : p.commands()) {
        commands.emplace_back(c.begin(), c.end());
    }
}

/* ============================================================
 * Conversions: EntryMeta (proto_codec.go 원본 그대로)
 * ============================================================ */

inline void entry_meta_to_proto(const EntryMeta &g, rpcproto::EntryMeta *p) {
    p->set_term(g.term);
    p->set_cmd_len(g.cmd_len);
}

inline EntryMeta entry_meta_from_proto(const rpcproto::EntryMeta &p) {
    return EntryMeta{p.term(), p.cmd_len()};
}

/* push 모드 링 extent (blockio/cached_fd.h의 Extent, DECISIONS.md D20) */
inline void ring_extent_to_proto(const Extent &g, rpcproto::RingExtent *p) {
    p->set_logical(g.logical);
    p->set_physical(g.physical);
    p->set_length(g.length);
}

inline Extent ring_extent_from_proto(const rpcproto::RingExtent &p) {
    return Extent{p.logical(), p.physical(), p.length()};
}

/* ============================================================
 * Conversions: WritePBABatch (rpc_rdma.go 원본 그대로.
 * follower -> storage node, destination-side block copy)
 * ============================================================ */

inline void write_pba_batch_request_to_proto(const blockcopy::WritePBABatchReq &g,
                                               rpcproto::WritePBABatchRequest *p) {
    p->mutable_pba_srcs()->Assign(g.pba_srcs.begin(), g.pba_srcs.end());
    p->mutable_pba_dsts()->Assign(g.pba_dsts.begin(), g.pba_dsts.end());
    p->mutable_nbytes()->Assign(g.nbytes.begin(), g.nbytes.end());
    p->set_block_size(g.block_size);
    p->set_src_dev(g.src_dev);
    p->set_dst_dev(g.dst_dev);
}

inline blockcopy::WritePBABatchReq write_pba_batch_request_from_proto(
        const rpcproto::WritePBABatchRequest &p) {
    blockcopy::WritePBABatchReq g;
    g.pba_srcs.assign(p.pba_srcs().begin(), p.pba_srcs().end());
    g.pba_dsts.assign(p.pba_dsts().begin(), p.pba_dsts().end());
    g.nbytes.assign(p.nbytes().begin(), p.nbytes().end());
    g.block_size = p.block_size();
    g.src_dev = p.src_dev();
    g.dst_dev = p.dst_dev();
    return g;
}

inline void write_pba_batch_response_to_proto(const blockcopy::WritePBABatchRsp &g,
                                                rpcproto::WritePBABatchResponse *p) {
    p->set_err(g.error);
    p->set_copy_nanos(g.copy_nanos);
    p->set_read_nanos(g.read_nanos);
    p->set_write_nanos(g.write_nanos);
}

inline blockcopy::WritePBABatchRsp write_pba_batch_response_from_proto(
        const rpcproto::WritePBABatchResponse &p) {
    blockcopy::WritePBABatchRsp g;
    g.error = p.err();
    g.copy_nanos = p.copy_nanos();
    g.read_nanos = p.read_nanos();
    g.write_nanos = p.write_nanos();
    return g;
}

/* ============================================================
 * Conversions: RequestVote (cold path, 원본 그대로)
 * ============================================================ */

inline void request_vote_request_to_proto(const RequestVoteRequest &g,
                                           rpcproto::RequestVoteRequest *p) {
    p->set_term(g.rpc.term);
    p->set_candidate_id(g.candidate_id);
    p->set_last_log_index(g.last_log_index);
    p->set_last_log_term(g.last_log_term);
}

inline void request_vote_request_from_proto(const rpcproto::RequestVoteRequest &p,
                                             RequestVoteRequest &g) {
    g.rpc.term = p.term();
    g.candidate_id = p.candidate_id();
    g.last_log_index = p.last_log_index();
    g.last_log_term = p.last_log_term();
}

inline void request_vote_response_to_proto(const RequestVoteResponse &g,
                                            rpcproto::RequestVoteResponse *p) {
    p->set_term(g.rpc.term);
    p->set_vote_granted(g.vote_granted);
}

inline void request_vote_response_from_proto(const rpcproto::RequestVoteResponse &p,
                                              RequestVoteResponse &g) {
    g.rpc.term = p.term();
    g.vote_granted = p.vote_granted();
}

/* ============================================================
 * Conversions: ReadIndexHeartbeat [수정-17]
 * ============================================================ */

inline void read_index_heartbeat_request_to_proto(const ReadIndexHeartbeatRequest &g,
                                                  rpcproto::ReadIndexHeartbeatRequest *p) {
    p->set_term(g.rpc.term);
    p->set_leader_id(g.leader_id);
    p->set_round(g.round);
}

inline void read_index_heartbeat_request_from_proto(const rpcproto::ReadIndexHeartbeatRequest &p,
                                                    ReadIndexHeartbeatRequest &g) {
    g.rpc.term = p.term();
    g.leader_id = p.leader_id();
    g.round = p.round();
}

inline void read_index_heartbeat_response_to_proto(const ReadIndexHeartbeatResponse &g,
                                                   rpcproto::ReadIndexHeartbeatResponse *p) {
    p->set_term(g.rpc.term);
    p->set_ok(g.ok);
    p->set_round(g.round);
}

inline void read_index_heartbeat_response_from_proto(const rpcproto::ReadIndexHeartbeatResponse &p,
                                                     ReadIndexHeartbeatResponse &g) {
    g.rpc.term = p.term();
    g.ok = p.ok();
    g.round = p.round();
}

/* ============================================================
 * Conversions: AppendEntries (hot path, 원본 그대로)
 * ============================================================ */

inline void append_entries_request_to_proto(const AppendEntriesRequest &g,
                                             rpcproto::AppendEntriesRequest *p) {
    p->set_term(g.rpc.term);
    p->set_leader_id(g.leader_id);
    p->set_prev_log_index(g.prev_log_index);
    p->set_prev_log_term(g.prev_log_term);
    p->set_leader_commit(g.leader_commit);
    p->set_leader_pba_src(g.leader_pba_src);
    p->set_log_block_length(g.log_block_length);
    p->set_num_entries(g.num_entries);
    p->set_slots_per_entry(g.slots_per_entry);
    p->set_start_slot(g.start_slot);
    p->set_leader_dev_index(g.leader_dev_index);
    p->set_data_already_copied(g.data_already_copied);
    p->set_need_extent_map(g.need_extent_map);

    p->clear_entry_metas();
    for (const auto &m : g.entry_metas) {
        entry_meta_to_proto(m, p->add_entry_metas());
    }
}

inline void append_entries_request_from_proto(const rpcproto::AppendEntriesRequest &p,
                                               AppendEntriesRequest &g) {
    g.rpc.term = p.term();
    g.leader_id = p.leader_id();
    g.prev_log_index = p.prev_log_index();
    g.prev_log_term = p.prev_log_term();
    g.leader_commit = p.leader_commit();
    g.leader_pba_src = p.leader_pba_src();
    g.log_block_length = p.log_block_length();
    g.num_entries = p.num_entries();
    g.slots_per_entry = p.slots_per_entry();
    g.start_slot = p.start_slot();
    g.leader_dev_index = p.leader_dev_index();
    g.data_already_copied = p.data_already_copied();
    g.need_extent_map = p.need_extent_map();

    g.entry_metas.clear();
    g.entry_metas.reserve(static_cast<size_t>(p.entry_metas_size()));
    for (const auto &pm : p.entry_metas()) {
        g.entry_metas.push_back(entry_meta_from_proto(pm));
    }
}

inline void append_entries_response_to_proto(const AppendEntriesResponse &g,
                                              rpcproto::AppendEntriesResponse *p) {
    p->set_term(g.rpc.term);
    p->set_success(g.success);
    p->set_conflict_term(g.conflict_term);
    p->set_conflict_index(g.conflict_index);
    p->set_handler_duration_nanos(g.handler_duration_ns);
    p->set_write_pba_rt_nanos(g.write_pba_rt_ns);
    p->set_storage_copy_nanos(g.storage_copy_ns);
    /* 팔로워 서브스테이지: 팔로워가 이미 채워놓는데 와이어 필드가 없어서
     * 리더의 ReplSample.handle_ae_*가 항상 0이던 것 */
    p->set_handle_ae_lock_wait_nanos(g.handle_ae_lock_wait_ns);
    p->set_handle_ae_pre_nanos(g.handle_ae_pre_ns);
    p->set_handle_ae_lock_wait2_nanos(g.handle_ae_lock_wait2_ns);
    p->set_handle_ae_post_nanos(g.handle_ae_post_ns);
    p->set_handle_ae_persist_nanos(g.handle_ae_persist_ns);
    p->set_push_map_hash(g.push_map_hash);
    p->clear_extent_map();
    for (const auto &e : g.extent_map) {
        ring_extent_to_proto(e, p->add_extent_map());
    }
    p->set_volume_wwid(g.volume_wwid);
}

inline void append_entries_response_from_proto(const rpcproto::AppendEntriesResponse &p,
                                                AppendEntriesResponse &g) {
    g.rpc.term = p.term();
    g.success = p.success();
    g.conflict_term = p.conflict_term();
    g.conflict_index = p.conflict_index();
    g.handler_duration_ns = p.handler_duration_nanos();
    g.write_pba_rt_ns = p.write_pba_rt_nanos();
    g.storage_copy_ns = p.storage_copy_nanos();
    g.handle_ae_lock_wait_ns = p.handle_ae_lock_wait_nanos();
    g.handle_ae_pre_ns = p.handle_ae_pre_nanos();
    g.handle_ae_lock_wait2_ns = p.handle_ae_lock_wait2_nanos();
    g.handle_ae_post_ns = p.handle_ae_post_nanos();
    g.handle_ae_persist_ns = p.handle_ae_persist_nanos();
    g.push_map_hash = p.push_map_hash();
    g.extent_map.clear();
    g.extent_map.reserve(static_cast<size_t>(p.extent_map_size()));
    for (const auto &pe : p.extent_map()) {
        g.extent_map.push_back(ring_extent_from_proto(pe));
    }
    g.volume_wwid = p.volume_wwid();
}

/* ============================================================
 * Conversions: ClientApply / ClientApplyTimed / ClientEcho
 * (proto_codec.go 원본 그대로. commands는 repeated bytes -> vector<vector<uint8_t>>)
 * ============================================================ */

inline void client_apply_request_to_proto(const ClientApplyRequest &g,
                                           rpcproto::ClientApplyRequest *p) {
    commands_to_proto(g.commands, p);
}

inline void client_apply_request_from_proto(const rpcproto::ClientApplyRequest &p,
                                             ClientApplyRequest &g) {
    commands_from_proto(p, g.commands);
}

inline void client_apply_response_to_proto(const ClientApplyResponse &g,
                                            rpcproto::ClientApplyResponse *p) {
    p->set_err(g.error);
    p->set_busy(g.busy);
    p->set_retry_after_ms(g.retry_after_ms);
}

inline void client_apply_response_from_proto(const rpcproto::ClientApplyResponse &p,
                                              ClientApplyResponse &g) {
    g.error = p.err();
    g.busy = p.busy();
    g.retry_after_ms = p.retry_after_ms();
}

inline void client_apply_timed_request_to_proto(const ClientApplyTimedRequest &g,
                                                 rpcproto::ClientApplyTimedRequest *p) {
    commands_to_proto(g.commands, p);
}

inline void client_apply_timed_request_from_proto(const rpcproto::ClientApplyTimedRequest &p,
                                                   ClientApplyTimedRequest &g) {
    commands_from_proto(p, g.commands);
}

inline void client_apply_timed_response_to_proto(const ClientApplyTimedResponse &g,
                                                  rpcproto::ClientApplyTimedResponse *p) {
    p->set_err(g.error);
    p->set_l_handler_nanos(g.l_handler_ns);
    p->set_l_persist_nanos(g.l_persist_ns);
    p->set_ae_net_nanos(g.ae_net_ns);
    p->set_f_handler_nanos(g.f_handler_ns);
    p->set_repl_net_nanos(g.repl_net_ns);
    /* proto 필드명은 원본 raft.go의 Replication을 따라 replication_nanos로
     * 유지한다 (wire 호환 + Go 베이스라인과의 컬럼 대응). 담기는 값은
     * StorageIO이며, C++ 쪽 필드명이 그쪽을 따른다. 이름이 어긋나는 곳은
     * 이 변환 두 줄뿐이다. */
    p->set_replication_nanos(g.replication_ns);
    p->set_quorum_wait_nanos(g.quorum_wait_ns);
    p->set_mutex_nanos(g.mutex_ns);
    p->set_total_nanos(g.total_ns);
    p->set_busy(g.busy);
    p->set_retry_after_ms(g.retry_after_ms);
    p->set_post_rpc_nanos(g.post_rpc_ns);
    p->set_commit_wait_nanos(g.commit_wait_ns);
    p->set_wg_scheduling_nanos(g.wg_scheduling_ns);
}

inline void client_apply_timed_response_from_proto(const rpcproto::ClientApplyTimedResponse &p,
                                                    ClientApplyTimedResponse &g) {
    g.error = p.err();
    g.l_handler_ns = p.l_handler_nanos();
    g.l_persist_ns = p.l_persist_nanos();
    g.ae_net_ns = p.ae_net_nanos();
    g.f_handler_ns = p.f_handler_nanos();
    g.repl_net_ns = p.repl_net_nanos();
    g.replication_ns = p.replication_nanos();   /* 위 to_proto 주석 참고 */
    g.quorum_wait_ns = p.quorum_wait_nanos();
    g.mutex_ns = p.mutex_nanos();
    g.total_ns = p.total_nanos();
    g.busy = p.busy();
    g.retry_after_ms = p.retry_after_ms();
    g.post_rpc_ns = p.post_rpc_nanos();
    g.commit_wait_ns = p.commit_wait_nanos();
    g.wg_scheduling_ns = p.wg_scheduling_nanos();
}

/* ClientRead: 필드 이름이 queries / results 라 commands_* 템플릿을 못 쓴다 */
inline void client_read_request_to_proto(const ClientReadRequest &g,
                                         rpcproto::ClientReadRequest *p) {
    p->clear_queries();
    for (const auto &q : g.queries) {
        p->add_queries(reinterpret_cast<const char *>(q.data()), q.size());
    }
}

inline void client_read_request_from_proto(const rpcproto::ClientReadRequest &p,
                                           ClientReadRequest &g) {
    g.queries.clear();
    g.queries.reserve(static_cast<size_t>(p.queries_size()));
    for (const auto &q : p.queries()) {
        g.queries.emplace_back(q.begin(), q.end());
    }
}

inline void client_read_response_to_proto(const ClientReadResponse &g,
                                          rpcproto::ClientReadResponse *p) {
    p->set_err(g.error);
    p->set_busy(g.busy);
    p->set_retry_after_ms(g.retry_after_ms);
    p->clear_results();
    for (const auto &r : g.results) {
        p->add_results(reinterpret_cast<const char *>(r.data()), r.size());
    }
    p->set_latency_nanos(g.latency_ns);
    p->set_confirm_nanos(g.confirm_ns);
    p->set_apply_wait_nanos(g.apply_wait_ns);
    p->set_sm_nanos(g.sm_ns);
}

inline void client_read_response_from_proto(const rpcproto::ClientReadResponse &p,
                                            ClientReadResponse &g) {
    g.error = p.err();
    g.busy = p.busy();
    g.retry_after_ms = p.retry_after_ms();
    g.results.clear();
    g.results.reserve(static_cast<size_t>(p.results_size()));
    for (const auto &r : p.results()) {
        g.results.emplace_back(r.begin(), r.end());
    }
    g.latency_ns = p.latency_nanos();
    g.confirm_ns = p.confirm_nanos();
    g.apply_wait_ns = p.apply_wait_nanos();
    g.sm_ns = p.sm_nanos();
}

inline void client_echo_request_to_proto(const ClientEchoRequest &g,
                                          rpcproto::ClientEchoRequest *p) {
    commands_to_proto(g.commands, p);
}

inline void client_echo_request_from_proto(const rpcproto::ClientEchoRequest &p,
                                            ClientEchoRequest &g) {
    commands_from_proto(p, g.commands);
}

inline void client_echo_response_to_proto(const ClientEchoResponse &g,
                                           rpcproto::ClientEchoResponse *p) {
    p->set_n(g.n);
}

inline void client_echo_response_from_proto(const rpcproto::ClientEchoResponse &p,
                                             ClientEchoResponse &g) {
    g.n = p.n();
}

/* ============================================================
 * Conversions: Client GetCommitIndex / GetHash / GetAEBatchStats (cold)
 * ============================================================ */

/* ClientGetCommitIndexRequest / ClientGetAEBatchStatsRequest는 필드가 없는
 * 빈 메시지다. 그 no-op 변환 함수 4개가 있었으나 호출부가 하나도 없어
 * 삭제했다 -- 서버는 body를 파싱하지 않고 바로 핸들러를 부르고, 클라이언트는
 * 빈 body를 그대로 보낸다. */

inline void client_get_commit_index_response_to_proto(
    const ClientGetCommitIndexResponse &g, rpcproto::ClientGetCommitIndexResponse *p) {
    p->set_commit_index(g.commit_index);
    p->set_err(g.error);
}

inline void client_get_commit_index_response_from_proto(
    const rpcproto::ClientGetCommitIndexResponse &p, ClientGetCommitIndexResponse &g) {
    g.commit_index = p.commit_index();
    g.error = p.err();
}

inline void client_get_hash_request_to_proto(const ClientGetHashRequest &g,
                                              rpcproto::ClientGetHashRequest *p) {
    p->set_at_count(g.at_count);
}

inline void client_get_hash_request_from_proto(const rpcproto::ClientGetHashRequest &p,
                                                ClientGetHashRequest &g) {
    g.at_count = p.at_count();
}

inline void client_get_hash_response_to_proto(const ClientGetHashResponse &g,
                                               rpcproto::ClientGetHashResponse *p) {
    p->set_hash(g.hash);
    p->set_count(g.count);
    p->set_err(g.error);
}

inline void client_get_hash_response_from_proto(const rpcproto::ClientGetHashResponse &p,
                                                 ClientGetHashResponse &g) {
    g.hash = p.hash();
    g.count = p.count();
    g.error = p.err();
}

inline void client_get_ae_batch_stats_response_to_proto(
    const ClientGetAEBatchStatsResponse &g, rpcproto::ClientGetAEBatchStatsResponse *p) {
    p->set_ae_count(g.ae_count);
    p->set_ae_entries(g.ae_entries);
    p->set_err(g.error);
}

inline void client_get_ae_batch_stats_response_from_proto(
    const rpcproto::ClientGetAEBatchStatsResponse &p, ClientGetAEBatchStatsResponse &g) {
    g.ae_count = p.ae_count();
    g.ae_entries = p.ae_entries();
    g.error = p.err();
}

} /* namespace nvmeof_raft */

#endif /* RAFT_PROTO_CONV_HPP */