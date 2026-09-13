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
 * ClientApplyRequest / ClientEchoRequest는 둘 다
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
    /* .proto에는 되감기 힌트(conflict_*)와 계측 필드가 그대로 남아 있다.
     * 와이어 포맷을 바꾸지 않으려고 채우지만 않는다 -- 이 버전은 그 둘을
     * 쓰지 않는다. */
}

inline void append_entries_response_from_proto(const rpcproto::AppendEntriesResponse &p,
                                                AppendEntriesResponse &g) {
    g.rpc.term = p.term();
    g.success = p.success();
}

/* ============================================================
 * Conversions: ClientApply / ClientEcho
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
 * Conversions: Client GetCommitIndex / GetHash (cold)
 * ============================================================ */

/* ClientGetCommitIndexRequest는 필드가 없는
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


} /* namespace nvmeof_raft */

#endif /* RAFT_PROTO_CONV_HPP */