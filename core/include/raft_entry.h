#ifndef RAFT_ENTRY_HPP
#define RAFT_ENTRY_HPP

#include <cstdint>
#include <string>
#include <vector>
#include <functional>

namespace nvmeof_raft {

struct Entry {
    std::vector<uint8_t> command;
    uint64_t term = 0;

};

struct EntryMeta {
    uint64_t term = 0;
    uint64_t cmd_len = 0;
};

struct RPCMessage {
    uint64_t term = 0;
};

struct AppendEntriesRequest {
    RPCMessage rpc;

    uint64_t leader_id = 0;
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    uint64_t leader_commit = 0;

    // Raw Block COpy Metadata
    uint64_t leader_pba_src = 0;
    uint64_t log_block_length = 0;
    uint64_t num_entries = 0;
    uint64_t slots_per_entry = 0;   // 얘도 모르겠음
    uint64_t start_slot = 0;        // 얘 역할 모르겠음
    int leader_dev_index = 0;

    std::vector<EntryMeta> entry_metas;
};

struct AppendEntriesResponse {
    RPCMessage rpc;
    bool success = false;

    
};

};

#endif // RAFT_ENTRY_HPP