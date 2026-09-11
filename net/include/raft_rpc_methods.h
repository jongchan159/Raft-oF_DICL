#ifndef RAFT_RPC_METHODS_H
#define RAFT_RPC_METHODS_H

/* ============================================================
 * 와이어 메서드 이름의 단일 출처.
 *
 * 이름은 Go net/rpc 관례("타입.메서드")를 따르며 원본 raft.go의 서비스
 * 등록과 동일하다. **문자열이 곧 와이어 포맷의 일부이므로 바꾸면
 * 호환성이 깨진다.**
 *
 * 리팩토링 전에는 같은 문자열이 클라이언트(raft_rpc_client.cpp,
 * raft_blkcopy_rpc_client.cpp, raft_client_main.cpp)와 서버
 * (raft_tcp_server.h, raft_blockcopy_tcp_server.h) 양쪽에 각각 하드코딩돼
 * 있었다. 한쪽에 오타가 나면 컴파일은 통과하고 런타임에
 * "unregistered method: ..." 로만 드러난다 -- 그것도 서버 쪽
 * [rpc-error] 로그가 있는 경우에만.
 * ============================================================ */

namespace nvmeof_raft {
namespace rpc_method {

/* --- Raft 노드 사이 (peer) --- */
constexpr const char *kAppendEntries = "Server.HandleAppendEntriesRequest";
constexpr const char *kRequestVote   = "Server.HandleRequestVoteRequest";

/* --- 클라이언트 -> Raft 노드 --- */
constexpr const char *kClientApply           = "Server.ClientApply";
constexpr const char *kClientApplyTimed      = "Server.ClientApplyTimed";
constexpr const char *kClientEcho            = "Server.ClientEcho";
constexpr const char *kClientGetCommitIndex  = "Server.ClientGetCommitIndex";
constexpr const char *kClientGetHash         = "Server.ClientGetHash";
constexpr const char *kClientGetAEBatchStats = "Server.ClientGetAEBatchStats";

/* --- Raft 노드 -> 스토리지(blockcopy) 노드 --- */
constexpr const char *kWritePBABatch = "BlockCopyServer.HandleWritePBABatch";

/* 원본 raft.go에는 있으나 이 포팅에 디스패치가 없는 것:
 *   BlockCopyServer.HandleWritePBA (단건), GetTime, ResetTime
 * core/do_pba_copy가 배치만 쓰기 때문이고, proto 메시지도 아직 없다.
 * 서버 측 구현(storage/raft_blockcopy_server.h)은 이미 있으므로 현재는
 * 도달 불가능한 코드다. */

} /* namespace rpc_method */
} /* namespace nvmeof_raft */

#endif /* RAFT_RPC_METHODS_H */
