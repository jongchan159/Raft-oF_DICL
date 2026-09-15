#ifndef RAFT_TCP_SERVER_HPP
#define RAFT_TCP_SERVER_HPP

#include "raft_server.h"
#include "raft_rpc_listener.h"   /* accept 루프 / 커넥션 처리 (공용) */
#include "raft_rpc_conn.h"       /* TransportKind */

#include <atomic>
#include <string>
#include <vector>

namespace nvmeof_raft {

/* apply_busy_retry_after_ms: core/raft_apply.cpp에서 정의 */
int apply_busy_retry_after_ms();

/* ============================================================
 * dispatch_client_method
 *
 * rpcproto.proto의 Client* 메시지들은 구조체와 변환 함수까지 전부
 * 있었지만 serve_connection에 등록돼 있지 않아서 (AppendEntries /
 * RequestVote 두 개만 있었다) 클라이언트가 로그에 엔트리를 넣을 방법이
 * 없었다. 여기서 등록한다.
 *
 * 메서드 이름은 Go net/rpc 관례("타입.메서드")를 따라
 * raft.go의 서비스 등록과 동일하게 Server.Client* 로 맞췄다.
 *
 * 반환: 처리했으면 true, 모르는 메서드면 false.
 * ============================================================ */
bool dispatch_client_method(const std::string &method, const std::vector<uint8_t> &body, std::vector<uint8_t> &rsp_body, Server *server);

/* ============================================================
 * Raft 노드의 메서드 디스패치.
 *
 * accept 루프와 커넥션 루프(HTTP 핸드셰이크 -> 프레임 읽기 -> 응답 쓰기)는
 * raft_rpc_listener.h가 스토리지 서버와 공유한다. 여기 남는 것은
 * "메서드 이름 -> 핸들러" 매핑뿐이다.
 * ============================================================ */
bool dispatch_raft_method(Server *server, const std::string &method, const std::vector<uint8_t> &body, std::vector<uint8_t> &rsp_body);

/* 커넥션 하나를 처리한다 (fd 소유권을 넘겨받아 닫는다). */
void serve_connection(int fd, Server *server);

/* Raft 노드의 RPC 리슨 루프 (TCP). stop_flag로 종료를 폴링한다. */
void run_tcp_server(int port, Server *server, std::atomic<bool> *stop_flag = nullptr);

/* 전송을 골라서 리슨한다. 메서드 디스패치(dispatch_raft_method /
 * dispatch_client_method)는 전송과 무관하게 **같은 코드**를 쓴다 --
 * 갈리는 것은 프레이밍과 운반뿐이다. */
void run_raft_server(TransportKind kind, const std::string &bind_host, int port,
                      Server *server, std::atomic<bool> *stop_flag = nullptr);

} /* namespace nvmeof_raft */

#endif /* RAFT_TCP_SERVER_HPP */