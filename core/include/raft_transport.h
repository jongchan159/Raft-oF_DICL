#ifndef RAFT_TRANSPORT_H
#define RAFT_TRANSPORT_H

#include "raft_entry.h"

#include <cstdint>
#include <string>
#include <vector>

/* ============================================================
 * 전송 계층 인터페이스.
 *
 * core/ 는 이 두 인터페이스만 알고, TCP도 protobuf도 소켓도 모른다.
 * 구현체는 net/ 이 제공하고(TcpRaftTransport / TcpBlockCopyClient),
 * 테스트는 tests/fake_transport.h 의 Mock이 제공한다.
 *
 * ---- 리팩토링 전에는 어땠는가 ----
 * 추상화가 인터페이스가 아니라 **링크타임 자유 함수 5개 + 불완전 타입
 * 1개**였다:
 *   rpc_call_append_entries(Server*, int, ...)  <- net/ 이 정의
 *   rpc_call_request_vote(Server*, int, ...)    <- net/ 이 정의
 *   blkcopy_connect / blkcopy_write_pba_batch   <- net/ 이 정의
 *   BlkcopyRpcClient::~BlkcopyRpcClient         <- net/ 이 정의
 *   struct RpcClientHandle;  (전방선언, 정의는 net/)
 *
 * 그 형태의 구체적 결과가 세 가지였다:
 *
 *  (1) **전송 구현체가 core의 상태를 직접 조작했다.** dial + backoff +
 *      실패 시 캐시 무효화 상태기계 전체가 net/src/raft_rpc_client.cpp 안에
 *      있으면서 s->mu를 잡고 s->cluster[i].rpc_client /
 *      next_dial_time 을 갱신했다. RDMA 구현은 그걸 바이트 단위로
 *      재현해야 했다. 이제 그 상태는 전송 구현체가 자기 안에 갖는다.
 *
 *  (2) **한 바이너리에 전송이 하나만 존재할 수 있었다.**
 *      ClusterMember::rpc_client 의 타입이 net/ 에서 정의되므로
 *      런타임 선택(-transport rdma)도, 테스트용 fake 주입도 불가능했다.
 *      그래서 selftest 타깃이 protobuf(5,502줄)를 함께 링크해야 했다.
 *
 *  (3) 에러 계약이 둘로 갈렸다 (blkcopy는 예외, rpc_call은 bool).
 *      여기서는 **양쪽 다 bool + out-param** 으로 통일한다 -- RDMA의
 *      완료 큐 폴링(ibv_wc status) 모델은 예외와 잘 맞지 않는다.
 *      core 내부의 제어 흐름(do_pba_copy의 예외 전파)은 그대로다:
 *      false를 받으면 do_pba_copy가 같은 예외를 던진다.
 * ============================================================ */

namespace nvmeof_raft {

/* ============================================================
 * RaftTransport -- 피어로의 Raft RPC (AppendEntries / RequestVote).
 *
 * peer_index는 Server::raft.cluster의 인덱스다. 구현체는 그 인덱스로
 * 주소를 찾을 수 있어야 하며(생성 시 주입받는다), 연결·재연결·backoff를
 * **전부 내부에서** 처리한다.
 *
 * 반환 false = 이번 호출이 실패했다 (연결 안 됨 / backoff 중 / 전송 오류
 * / 응답 파싱 실패). 호출자는 그것만 알면 되고, 다음 라운드에서 다시
 * 시도한다 -- 원본 raft.go의 rpcCall 계약과 같다.
 *
 * **여러 스레드가 동시에 호출한다** (팔로워마다 워커 스레드가 하나씩).
 * 구현체가 필요한 직렬화를 스스로 해야 한다.
 * ============================================================ */
class RaftTransport {
public:
    virtual ~RaftTransport() = default;

    virtual bool append_entries(int peer_index, const AppendEntriesRequest &req,
                                 AppendEntriesResponse &rsp) = 0;

    virtual bool request_vote(int peer_index, const RequestVoteRequest &req,
                              RequestVoteResponse &rsp) = 0;
};

/* ============================================================
 * BlockCopyClient -- 스토리지 노드로의 PBA 배치 복사
 * (raft.go의 blockcopy.RPCClientRDMA 대응).
 *
 * 세 벡터는 같은 길이여야 하며 i번째 청크가
 * (pba_srcs[i] -> pba_dsts[i], nbytes[i] 바이트) 를 뜻한다.
 * src_dev / dst_dev는 스토리지 노드가 -devices 로 받은 목록의 인덱스이며
 * **클러스터 인덱스와 같은 순서**다.
 *
 * 반환 true: 요청한 청크들이 스토리지 노드에서 복사되었다.
 * 반환 false: *out_error 에 사람이 읽을 이유가 담긴다. 커넥션 수준 오류와
 *            스토리지 노드의 애플리케이션 수준 실패를 구분하지 않는다 --
 *            호출부(do_pba_copy)가 어느 쪽이든 fail-soft로 이 배치를
 *            건너뛰고 다음 라운드에 재시도하기 때문이다.
 *
 * **여러 스레드가 동시에 호출한다** (팔로워의 AE 핸들러가 커넥션마다
 * 스레드를 띄우고 그 전부가 이 클라이언트 하나를 공유한다).
 * ============================================================ */
class BlockCopyClient {
public:
    virtual ~BlockCopyClient() = default;

    virtual bool write_pba_batch(const std::vector<uint64_t> &pba_srcs,
                                  const std::vector<uint64_t> &pba_dsts,
                                  const std::vector<uint64_t> &nbytes,
                                  int src_dev, int dst_dev,
                                  std::string *out_error) = 0;
};

} /* namespace nvmeof_raft */

#endif /* RAFT_TRANSPORT_H */
