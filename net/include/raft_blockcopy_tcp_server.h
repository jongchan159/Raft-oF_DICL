#ifndef RAFT_BLOCKCOPY_TCP_SERVER_HPP
#define RAFT_BLOCKCOPY_TCP_SERVER_HPP

#include "raft_blockcopy_server.h"
#include "raft_wire_codec.h"
#include "raft_proto_conv.h"
#include "raft_rpc_listener.h"   /* accept 루프 / 커넥션 처리 / HTTP 핸드셰이크 (공용) */
#include "raft_rpc_methods.h"
#include "raft_rpc_conn.h"   /* TransportKind */

#include <atomic>
#include <string>
#include <vector>

namespace nvmeof_raft {

/* ============================================================
 * server_random_main.go 대응: BlockCopyServer를 TCP 리슨+net/rpc
 * 프레이밍으로 감싸는 스토리지 노드 서버 껍데기.
 *
 * accept 루프와 커넥션 루프(HTTP CONNECT 핸드셰이크 -> WireServerCodec
 * 루프 -> 응답 쓰기)는 raft_rpc_listener.h에서 Raft 노드와 **같은 코드**를
 * 쓴다. 예전에는 이 골격이 raft_tcp_server.h에 복사돼 있었고, 그 결과
 * [rpc-error] 로깅이 raft 쪽에만 있어 스토리지 서버의 RPC 에러가 완전히
 * 침묵했다. 여기 남는 것은 디스패치 대상이 blockcopy::BlockCopyServer라는
 * 차이뿐이다.
 *
 * 지금 등록된 메서드는 do_pba_copy가 실제로 쓰는 WritePBABatch 하나뿐.
 * WritePBA(단건)/GetTime/ResetTime은 Go 원본에 있지만 이 포팅의
 * 호출부(core/do_pba_copy)가 안 쓰므로 등록하지 않는다 (proto 메시지도
 * 아직 미정의 -- 필요해지면 rpcproto.proto에 추가).
 * ============================================================ */

/* 스토리지 노드의 메서드 디스패치.
 *
 * 지금 등록된 것은 core/do_pba_copy가 실제로 쓰는 WritePBABatch 하나뿐이다.
 * WritePBA(단건)/GetTime/ResetTime은 서버 측 구현이
 * storage/raft_blockcopy_server.h에 이미 있으나 proto 메시지가 없어 도달할 수
 * 없다 (DECISIONS.md 참고 -- 당장 필요하지 않다). */
bool dispatch_blockcopy_method(blockcopy::BlockCopyServer *bcs, const std::string &method, const std::vector<uint8_t> &body, std::vector<uint8_t> &rsp_body);

void blockcopy_serve_connection(int fd, blockcopy::BlockCopyServer *bcs);

void run_blockcopy_tcp_server(int port, blockcopy::BlockCopyServer *bcs, std::atomic<bool> *stop_flag = nullptr);

/* 전송을 골라서 리슨한다 (디스패치는 전송과 무관하게 공유). */
void run_blockcopy_server(TransportKind kind, int port, blockcopy::BlockCopyServer *bcs,
                           std::atomic<bool> *stop_flag = nullptr);

} /* namespace nvmeof_raft */

#endif /* RAFT_BLOCKCOPY_TCP_SERVER_HPP */