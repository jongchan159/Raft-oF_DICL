#ifndef RAFT_RPC_LISTENER_H
#define RAFT_RPC_LISTENER_H

#include "raft_wire_codec.h"

#include <atomic>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

/* ============================================================
 * RPC 리스너 -- Raft 노드와 스토리지(blockcopy) 노드가 공유한다.
 *
 * 리팩토링 전에는 이 파일의 내용이 raft_tcp_server.h와
 * raft_blockcopy_tcp_server.h에 **두 벌로 복사**되어 있었다. 두 accept
 * 루프의 diff는 12줄뿐이었고 그 12줄은 전부 함수명, 에러 문자열 안의
 * 함수명, std::thread의 진입 함수였다. serve_connection 쪽도 골격이
 * 100% 같고 메서드 디스패치 본문만 달랐다.
 *
 * 복사본이 이미 갈라져 있었다는 증거:
 *   - [rpc-error] 로깅이 raft 쪽에만 있어서 **스토리지 서버의 RPC 에러가
 *     완전히 침묵**했다. 이제 양쪽이 같은 코드를 쓴다.
 *   - accept된 fd의 SO_RCVTIMEO를 지우는 11줄 주석(DECISIONS.md D3)이
 *     두 파일에 한 글자도 다르지 않게 중복돼, 한쪽만 고치면 즉시
 *     갈라지는 상태였다.
 *
 * 이 헤더는 **raft_server.h를 include하지 않는다.** 예전에는
 * raft_blockcopy_tcp_server.h가 핸드셰이크 함수 두 개를 쓰려고
 * raft_tcp_server.h를 include했고, 그 바람에 스토리지 서버 바이너리가
 * Raft Server(620줄) 전체를 컴파일했다 -- 정작 build.sh는 그 바이너리에
 * core/를 링크하지 않으므로, inline 함수가 그 TU에서 미사용이라
 * emit이 생략되는 것에만 의존하는 취약한 상태였다.
 * ============================================================ */

namespace nvmeof_raft {

/* ---- HTTP CONNECT 핸드셰이크 (Go net/rpc DialHTTP 호환) ---------------- */

void send_http_connect_ok(int fd);

void consume_http_connect_request(int fd);

/* ---- 메서드 디스패처 ---------------------------------------------------
 * 요청 body(직렬화된 protobuf)를 받아 응답 body를 채운다.
 * 반환: 처리했으면 true, 등록되지 않은 메서드면 false
 *       (호출자가 "unregistered method" 에러로 응답한다).
 * 예외를 던지면 그 메시지가 RPC 에러로 클라이언트에 전달된다. */
using RpcMethodDispatcher =
    std::function<bool(const std::string &method,
                       const std::vector<uint8_t> &req_body,
                       std::vector<uint8_t> &rsp_body)>;

/* ---- 커넥션 하나를 끝까지 처리한다 (커넥션당 스레드 하나) --------------
 * 반환 시 fd를 닫는다. tag는 로그 접두사에만 쓰인다("raft"/"blockcopy"). */
void serve_rpc_connection(int fd, const char *tag, const RpcMethodDispatcher &dispatch);

/* ---- accept 루프 -------------------------------------------------------
 * stop_flag != nullptr이면 listen fd에 200ms 수신 타임아웃을 걸어
 * 주기적으로 플래그를 확인한다. nullptr이면 무한 블로킹.
 * on_connection은 accept된 fd의 소유권을 넘겨받는다 (닫을 책임까지). */
void run_rpc_listener(int port, const char *tag, const std::function<void(int)> &on_connection, std::atomic<bool> *stop_flag = nullptr);

} /* namespace nvmeof_raft */

#endif /* RAFT_RPC_LISTENER_H */
