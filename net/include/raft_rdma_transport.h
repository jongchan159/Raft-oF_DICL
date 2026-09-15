#ifndef RAFT_RDMA_TRANSPORT_H
#define RAFT_RDMA_TRANSPORT_H

#include "raft_rpc_listener.h"   /* RpcMethodDispatcher (서버 쪽 계약을 공유한다) */

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

/* ============================================================
 * RDMA 전송 (rdma_cm + RC QP, SEND/RECV 메시징).
 *
 * TCP 쪽(raft_tcp_transport.h / raft_rpc_listener.h)과 **대칭인 API**를
 * 제공한다. 그래야 apps/ 의 조립부가 전송 종류만 고르면 되고, 위쪽
 * (RaftTransport / BlockCopyClient 구현체, 메서드 디스패치)은 그대로다:
 *
 *   TCP                              RDMA
 *   tcp_dial_http(addr)              rdma_dial(addr)
 *   rpc_invoke(h, method, body)      rdma_invoke(h, method, body)
 *   run_rpc_listener(port, ...)      run_rdma_listener(port, ...)
 *
 * **서버 쪽 디스패처(RpcMethodDispatcher)는 완전히 공유한다.** 즉
 * dispatch_raft_method / dispatch_client_method / dispatch_blockcopy_method
 * 는 전송을 전혀 모르고, 와이어에 실리는 것도 같은 protobuf 바디다.
 * 바뀌는 것은 프레이밍과 운반뿐이다.
 *
 * ---- 왜 SEND/RECV 인가 ----
 * 이 RPC 는 요청/응답이 커넥션당 **엄격히 1건씩** 오간다(아래 직렬화
 * 규약). RDMA_WRITE + 별도 완료 통지를 쓰면 원격 버퍼 주소 교환과
 * 크레딧 관리가 붙는데, 메시지가 수 KB~수백 KB 수준이라 그 복잡도를
 * 상쇄할 이득이 없다. SEND/RECV 는 메시지 경계를 하드웨어가 지켜 주므로
 * TCP 쪽의 길이 프레이밍(WireCodec)도 필요 없다.
 *
 * ---- 주소 ----
 * rdma_cm 은 IP 주소로 RDMA 장치를 찾는다. 따라서 여기 넘기는 주소는
 * **IPoIB 주소**여야 한다 (이 클러스터에서는 10.0.0.x). 1GbE 주소를 주면
 * rdma_resolve_addr 가 실패한다 -- 그 실패는 "RDMA 로 못 붙는다"는 뜻이지
 * 설정 오류가 아닐 수 있으므로, 호출부는 TCP 로 폴백할지 결정할 수 있다.
 *
 * ---- 직렬화 규약 ----
 * 커넥션(QP) 하나에 동시에 한 요청만 태운다. TCP 쪽 RpcClientHandle::call_mu
 * 와 같은 이유이고, RDMA 에서는 더 강하게 필요하다 -- 여러 스레드가 같은
 * QP 에 post_send 를 섞으면 완료 큐에서 어느 응답이 누구 것인지 구분할
 * 방법이 없다.
 * ============================================================ */

namespace nvmeof_raft {

/* 한 메시지의 최대 크기. 요청/응답 버퍼가 커넥션마다 이만큼 등록된다
 * (send + recv 각각). AppendEntries 의 entry_metas 가 가장 큰데, 엔트리당
 * 약 16B 이므로 1 MiB 면 6만 엔트리가 한 배치에 들어간다. 넘으면 명시적
 * 예외를 던진다 -- 조용히 잘리는 것보다 낫다. */
constexpr size_t kRdmaMaxMessageBytes = 1u << 20;   /* 1 MiB */

/* ============================================================
 * 클라이언트 핸들. rdma_dial 이 만들고, 소멸자가 QP/MR/커넥션을 정리한다.
 * ============================================================ */
struct RdmaClientHandle;

/* address 는 "host:port" 이고 host 는 IPoIB 주소여야 한다.
 * 실패 시 예외 (연결 불가 / 장치 없음 / 주소 해석 실패).
 * 소유권은 호출자에게 있다 (delete 로 해제). */
RdmaClientHandle *rdma_dial(const std::string &address, int timeout_ms = 5000);

void rdma_close(RdmaClientHandle *h);

/* 요청 body 를 보내고 응답 body 를 받는다. 커넥션 내부 뮤텍스로
 * 직렬화되며, 실패 시 예외를 던진다 (TCP 의 rpc_invoke 와 같은 계약).
 * err_prefix 는 원격이 보고한 에러 메시지 앞에 붙는다. */
std::vector<uint8_t> rdma_invoke(RdmaClientHandle *h, const std::string &method,
                                  const std::vector<uint8_t> &req_body,
                                  const char *err_prefix = "remote error: ");

/* ============================================================
 * 서버. TCP 쪽 run_rpc_listener 와 같은 계약이다:
 * 0.0.0.0:port 에 바인드하고, 커넥션마다 스레드를 띄워 dispatch 를 돌린다.
 * stop_flag 가 서면 200ms 안에 루프를 빠져나오고 모든 커넥션 스레드를
 * 정리한 뒤 반환한다.
 * ============================================================ */
void run_rdma_listener(int port, const char *tag,
                        const RpcMethodDispatcher &dispatch,
                        std::atomic<bool> *stop_flag = nullptr);

/* 이 빌드에 RDMA 장치가 하나라도 보이는가. -transport 기본값이 rdma 인데
 * 장치가 없는 호스트에서 기동할 때, 진단 메시지를 위해 쓴다. */
bool rdma_devices_available();

} /* namespace nvmeof_raft */

#endif /* RAFT_RDMA_TRANSPORT_H */
