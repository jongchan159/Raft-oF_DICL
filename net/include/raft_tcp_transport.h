#ifndef RAFT_TCP_TRANSPORT_HPP
#define RAFT_TCP_TRANSPORT_HPP

#include <cstdint>
#include <string>
#include <stdexcept>
#include <cstring>
#include <mutex>
#include <vector>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

#include "raft_wire_codec.h"

namespace nvmeof_raft {

/* ============================================================
 * RpcClientHandle 실제 구현 (TCP 기반, RDMA 이전 단계)
 *
 * 원본 rdmaDialHTTP()가 하는 일 중 RDMA 관련 부분(rdmacm.Dial)만
 * 일반 TCP connect로 대체하고, 나머지(HTTP CONNECT 핸드셰이크,
 * 데드라인 설정)는 원본 그대로 포팅.
 * ============================================================ */
struct RpcClientHandle {
    int fd = -1;
    WireClientCodec *codec = nullptr;
    uint64_t next_seq = 1;

    /* call_mu: 하나의 커넥션에 대한 "요청 쓰기 -> 응답 헤더 읽기 ->
     * 응답 바디 읽기" 전체를 직렬화한다.
     *
     * WireClientCodec은 pending_body_len_/pending_method_를 단일 슬롯으로
     * 들고 있어서 요청/응답이 엄격히 1:1 순차로 오가는 것을 전제한다.
     * 데이터 있는 AppendEntries는 ClusterMember::inflight 플래그로
     * 직렬화되지만 하트비트는 그 가드를 받지 않으므로, 하트비트와
     * 데이터 AE가 같은 팔로워로 동시에 나가면 프레임이 섞이고
     * next_seq++도 비원자적으로 경쟁했다. blkcopy 클라이언트도
     * 팔로워의 AE 핸들러 여러 개가 공유하므로 같은 보호가 필요하다.
     *
     * RDMA 전송으로 교체할 때도 "커넥션당 직렬화" 요구는 남는다
     * (QP 하나에 여러 스레드가 post_send를 섞으면 같은 문제). */
    std::mutex call_mu;

    ~RpcClientHandle();
};

/* ============================================================
 * rpc_invoke: 커넥션 하나로 요청 body를 보내고 응답 body를 받는다.
 *
 * "call_mu 잠금 -> next_seq 발급 -> write_request -> read_response_header
 *  -> error 검사 -> read_response_body" 이 시퀀스가 리팩토링 전에는 세 벌
 * 복사되어 있었다 (raft_rpc_client.cpp / raft_blkcopy_rpc_client.cpp /
 * raft_client_main.cpp). 직렬화 규약(커넥션당 동시 1건)이 세 곳에 흩어져
 * 있으면 한 곳만 빠뜨려도 프레임이 섞이는데, 실제로 그 버그가 있었다
 * (RpcClientHandle::call_mu 주석 참고).
 *
 * 락은 응답 body를 다 읽은 시점에 풀린다. protobuf 파싱은 락 밖에서
 * 한다 -- WireClientCodec의 단일 슬롯(pending_body_len_)은 body를 읽는
 * 순간 비므로 파싱을 임계구역에 둘 이유가 없다.
 *
 * 실패 시 예외. err_prefix는 원격이 보고한 에러 메시지 앞에 붙는다.
 * ============================================================ */
std::vector<uint8_t> rpc_invoke(RpcClientHandle *h, const std::string &method, const std::vector<uint8_t> &req_body, const char *err_prefix = "remote error: ");

/* tcp_connect / http_connect_handshake 는 tcp_dial_http 하나만 쓰므로
 * net/src/raft_tcp_transport.cpp 의 익명 namespace 로 내렸다 (외부 API 아님). */

RpcClientHandle *tcp_dial_http(const std::string &address);

} /* namespace nvmeof_raft */

#endif /* RAFT_TCP_TRANSPORT_HPP */