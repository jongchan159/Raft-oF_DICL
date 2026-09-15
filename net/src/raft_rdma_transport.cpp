/* ============================================================
 * raft_rdma_transport.cpp -- rdma_cm + RC QP 위의 요청/응답 RPC.
 *
 * 계약과 설계 근거는 net/include/raft_rdma_transport.h 참고.
 * 이 파일이 아는 것은 "메서드 이름 + 요청 바이트 -> 응답 바이트"까지이고,
 * 그 바이트가 protobuf 라는 것도, 어떤 메서드가 있는지도 모른다.
 *
 * ---- 와이어 프레이밍 ----
 * SEND 한 번이 메시지 하나다 (하드웨어가 경계를 지킨다).
 *   요청:  [u32 method_len][u32 body_len][method][body]
 *   응답:  [u32 err_len   ][u32 body_len][err   ][body]
 * 길이는 빅엔디언 -- 이 프로젝트의 와이어 규약이다 (온-디스크 링은 LE,
 * 혼동하지 말 것: core/include/raft_basics.h 주석 참고).
 * err_len > 0 이면 body 는 비어 있고 클라이언트가 예외를 던진다.
 * ============================================================ */
#include "raft_rdma_transport.h"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <arpa/inet.h>
#include <netdb.h>
#include <poll.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace nvmeof_raft {

namespace {

/* 프레임 헤더: 길이 필드 두 개 */
constexpr size_t kHdrBytes = 8;

/* 완료를 기다리는 한 번의 poll 간격. stop_flag 확인 주기이기도 하다. */
constexpr int kPollSliceMs = 200;

/* 클라이언트 RPC 의 기본 상한. 무한 대기하면 리더가 죽은 팔로워에
 * 영구히 매달린다 (TCP 쪽 SO_RCVTIMEO 와 같은 역할). */
constexpr int kCallTimeoutMs = 5000;

constexpr int kCqDepth = 8;
constexpr int kMaxWr = 4;

void put_be32(uint8_t *p, uint32_t v) {
    p[0] = static_cast<uint8_t>((v >> 24) & 0xFF);
    p[1] = static_cast<uint8_t>((v >> 16) & 0xFF);
    p[2] = static_cast<uint8_t>((v >> 8) & 0xFF);
    p[3] = static_cast<uint8_t>(v & 0xFF);
}

uint32_t get_be32(const uint8_t *p) {
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}

[[noreturn]] void fail(const std::string &what) {
    throw std::runtime_error("rdma: " + what + ": " + std::strerror(errno));
}

/* ============================================================
 * RdmaConn -- 커넥션 하나의 verbs 자원 일체.
 * 클라이언트와 서버가 같은 구조를 쓴다 (한쪽은 connect, 한쪽은 accept).
 * ============================================================ */
struct RdmaConn {
    rdma_cm_id *id = nullptr;
    ibv_pd *pd = nullptr;
    ibv_cq *cq = nullptr;
    ibv_comp_channel *chan = nullptr;
    ibv_mr *send_mr = nullptr;
    ibv_mr *recv_mr = nullptr;
    uint8_t *send_buf = nullptr;
    uint8_t *recv_buf = nullptr;

    /* 커넥션당 동시 1건 (헤더의 직렬화 규약) */
    std::mutex call_mu;

    /* 서버 쪽 커넥션 스레드 종료 신호 */
    std::atomic<bool> stop{false};

    ~RdmaConn() { teardown(); }

    void teardown() {
        if (id != nullptr && id->qp != nullptr) {
            rdma_destroy_qp(id);
        }
        if (send_mr != nullptr) { ibv_dereg_mr(send_mr); send_mr = nullptr; }
        if (recv_mr != nullptr) { ibv_dereg_mr(recv_mr); recv_mr = nullptr; }
        delete[] send_buf; send_buf = nullptr;
        delete[] recv_buf; recv_buf = nullptr;
        if (cq != nullptr) { ibv_destroy_cq(cq); cq = nullptr; }
        if (chan != nullptr) { ibv_destroy_comp_channel(chan); chan = nullptr; }
        if (pd != nullptr) { ibv_dealloc_pd(pd); pd = nullptr; }
        if (id != nullptr) { rdma_destroy_id(id); id = nullptr; }
    }
};

/* PD / CQ / QP / MR 를 만들고 recv 버퍼를 등록한다. id 는 이미
 * (클라이언트) route 해석이 끝났거나 (서버) CONNECT_REQUEST 로 받은 것. */
void setup_conn(RdmaConn *c, rdma_cm_id *id) {
    c->id = id;

    c->pd = ibv_alloc_pd(id->verbs);
    if (c->pd == nullptr) { fail("ibv_alloc_pd"); }

    c->chan = ibv_create_comp_channel(id->verbs);
    if (c->chan == nullptr) { fail("ibv_create_comp_channel"); }

    c->cq = ibv_create_cq(id->verbs, kCqDepth, nullptr, c->chan, 0);
    if (c->cq == nullptr) { fail("ibv_create_cq"); }
    if (ibv_req_notify_cq(c->cq, 0) != 0) { fail("ibv_req_notify_cq"); }

    ibv_qp_init_attr qa{};
    qa.send_cq = c->cq;
    qa.recv_cq = c->cq;
    qa.qp_type = IBV_QPT_RC;
    qa.cap.max_send_wr = kMaxWr;
    qa.cap.max_recv_wr = kMaxWr;
    qa.cap.max_send_sge = 1;
    qa.cap.max_recv_sge = 1;
    if (rdma_create_qp(id, c->pd, &qa) != 0) { fail("rdma_create_qp"); }

    c->send_buf = new uint8_t[kRdmaMaxMessageBytes];
    c->recv_buf = new uint8_t[kRdmaMaxMessageBytes];
    c->send_mr = ibv_reg_mr(c->pd, c->send_buf, kRdmaMaxMessageBytes, IBV_ACCESS_LOCAL_WRITE);
    if (c->send_mr == nullptr) { fail("ibv_reg_mr(send)"); }
    c->recv_mr = ibv_reg_mr(c->pd, c->recv_buf, kRdmaMaxMessageBytes, IBV_ACCESS_LOCAL_WRITE);
    if (c->recv_mr == nullptr) { fail("ibv_reg_mr(recv)"); }
}

void post_recv(RdmaConn *c) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uint64_t>(c->recv_buf);
    sge.length = static_cast<uint32_t>(kRdmaMaxMessageBytes);
    sge.lkey = c->recv_mr->lkey;

    ibv_recv_wr wr{};
    wr.wr_id = 1;   /* RECV */
    wr.sg_list = &sge;
    wr.num_sge = 1;

    ibv_recv_wr *bad = nullptr;
    if (ibv_post_recv(c->id->qp, &wr, &bad) != 0) { fail("ibv_post_recv"); }
}

void post_send(RdmaConn *c, size_t len) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uint64_t>(c->send_buf);
    sge.length = static_cast<uint32_t>(len);
    sge.lkey = c->send_mr->lkey;

    ibv_send_wr wr{};
    wr.wr_id = 2;   /* SEND */
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(c->id->qp, &wr, &bad) != 0) { fail("ibv_post_send"); }
}

/* 완료 하나를 기다린다.
 * 반환 true = wc 채움, false = timeout_ms 안에 아무것도 안 옴.
 * 완료 상태가 IBV_WC_SUCCESS 가 아니면 예외 (커넥션이 끊긴 경우 포함). */
bool wait_completion(RdmaConn *c, ibv_wc *wc, int timeout_ms, std::atomic<bool> *stop) {
    int waited = 0;
    for (;;) {
        int n = ibv_poll_cq(c->cq, 1, wc);
        if (n < 0) { fail("ibv_poll_cq"); }
        if (n > 0) {
            if (wc->status != IBV_WC_SUCCESS) {
                throw std::runtime_error(std::string("rdma: completion failed: ") +
                                          ibv_wc_status_str(wc->status));
            }
            return true;
        }

        if (stop != nullptr && stop->load()) { return false; }
        if (timeout_ms >= 0 && waited >= timeout_ms) { return false; }

        int slice = kPollSliceMs;
        if (timeout_ms >= 0 && timeout_ms - waited < slice) { slice = timeout_ms - waited; }

        pollfd pfd{};
        pfd.fd = c->chan->fd;
        pfd.events = POLLIN;
        int pr = ::poll(&pfd, 1, slice);
        waited += slice;
        if (pr < 0) {
            if (errno == EINTR) { continue; }
            fail("poll(comp_channel)");
        }
        if (pr == 0) { continue; }   /* 타임슬라이스 만료 -- 위에서 stop/timeout 재확인 */

        ibv_cq *ev_cq = nullptr;
        void *ev_ctx = nullptr;
        if (ibv_get_cq_event(c->chan, &ev_cq, &ev_ctx) != 0) { fail("ibv_get_cq_event"); }
        ibv_ack_cq_events(ev_cq, 1);
        if (ibv_req_notify_cq(c->cq, 0) != 0) { fail("ibv_req_notify_cq"); }
    }
}

/* 요청/응답 프레임을 send_buf 에 채운다. 반환: 총 길이 */
size_t encode_frame(RdmaConn *c, const std::string &head, const std::vector<uint8_t> &body) {
    size_t total = kHdrBytes + head.size() + body.size();
    if (total > kRdmaMaxMessageBytes) {
        throw std::runtime_error("rdma: message too large (" + std::to_string(total) +
                                  " > " + std::to_string(kRdmaMaxMessageBytes) + ")");
    }
    put_be32(c->send_buf, static_cast<uint32_t>(head.size()));
    put_be32(c->send_buf + 4, static_cast<uint32_t>(body.size()));
    std::memcpy(c->send_buf + kHdrBytes, head.data(), head.size());
    if (!body.empty()) {
        std::memcpy(c->send_buf + kHdrBytes + head.size(), body.data(), body.size());
    }
    return total;
}

/* recv_buf 의 프레임을 분해한다. 길이가 버퍼를 넘으면 예외. */
void decode_frame(const RdmaConn *c, uint32_t received,
                  std::string &head, std::vector<uint8_t> &body) {
    if (received < kHdrBytes) {
        throw std::runtime_error("rdma: short frame");
    }
    uint32_t head_len = get_be32(c->recv_buf);
    uint32_t body_len = get_be32(c->recv_buf + 4);
    if (static_cast<size_t>(head_len) + body_len + kHdrBytes > received) {
        throw std::runtime_error("rdma: truncated frame");
    }
    head.assign(reinterpret_cast<const char *>(c->recv_buf + kHdrBytes), head_len);
    body.assign(c->recv_buf + kHdrBytes + head_len,
                c->recv_buf + kHdrBytes + head_len + body_len);
}

/* "host:port" -> addrinfo. rdma_resolve_addr 에 넘길 sockaddr 을 얻는다. */
void split_addr(const std::string &address, std::string &host, std::string &port) {
    size_t colon = address.rfind(':');
    if (colon == std::string::npos) {
        throw std::runtime_error("rdma: address must be host:port -- got '" + address + "'");
    }
    host = address.substr(0, colon);
    port = address.substr(colon + 1);
}

} /* anonymous namespace */

/* ============================================================
 * 클라이언트
 * ============================================================ */

struct RdmaClientHandle {
    rdma_event_channel *ec = nullptr;
    RdmaConn conn;

    ~RdmaClientHandle() {
        if (conn.id != nullptr) {
            rdma_disconnect(conn.id);
        }
        conn.teardown();
        if (ec != nullptr) { rdma_destroy_event_channel(ec); ec = nullptr; }
    }
};

namespace {

/* cm 이벤트 하나를 기다린다. 기대한 타입이 아니면 예외. */
void expect_cm_event(rdma_event_channel *ec, rdma_cm_event_type want, int timeout_ms) {
    pollfd pfd{};
    pfd.fd = ec->fd;
    pfd.events = POLLIN;
    int pr = ::poll(&pfd, 1, timeout_ms);
    if (pr < 0) { fail("poll(cm event channel)"); }
    if (pr == 0) {
        throw std::runtime_error(std::string("rdma: timed out waiting for ") +
                                  rdma_event_str(want));
    }

    rdma_cm_event *ev = nullptr;
    if (rdma_get_cm_event(ec, &ev) != 0) { fail("rdma_get_cm_event"); }
    rdma_cm_event_type got = ev->event;
    int status = ev->status;
    rdma_ack_cm_event(ev);

    if (got != want) {
        throw std::runtime_error(std::string("rdma: expected ") + rdma_event_str(want) +
                                  " but got " + rdma_event_str(got) +
                                  " (status " + std::to_string(status) + ")");
    }
}

} /* anonymous namespace */

RdmaClientHandle *rdma_dial(const std::string &address, int timeout_ms) {
    std::string host, port;
    split_addr(address, host, port);

    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo *ai = nullptr;
    int rc = ::getaddrinfo(host.c_str(), port.c_str(), &hints, &ai);
    if (rc != 0) {
        throw std::runtime_error("rdma: getaddrinfo(" + address + "): " + gai_strerror(rc));
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> ai_guard(ai, &::freeaddrinfo);

    auto h = std::unique_ptr<RdmaClientHandle>(new RdmaClientHandle());

    h->ec = rdma_create_event_channel();
    if (h->ec == nullptr) { fail("rdma_create_event_channel"); }

    rdma_cm_id *id = nullptr;
    if (rdma_create_id(h->ec, &id, nullptr, RDMA_PS_TCP) != 0) { fail("rdma_create_id"); }
    h->conn.id = id;   /* 실패 경로에서도 소멸자가 정리하도록 먼저 넣어 둔다 */

    if (rdma_resolve_addr(id, nullptr, ai->ai_addr, timeout_ms) != 0) {
        fail("rdma_resolve_addr(" + address + ")");
    }
    expect_cm_event(h->ec, RDMA_CM_EVENT_ADDR_RESOLVED, timeout_ms);

    if (rdma_resolve_route(id, timeout_ms) != 0) { fail("rdma_resolve_route"); }
    expect_cm_event(h->ec, RDMA_CM_EVENT_ROUTE_RESOLVED, timeout_ms);

    setup_conn(&h->conn, id);
    post_recv(&h->conn);   /* 응답을 받을 자리를 먼저 깔아 둔다 */

    rdma_conn_param cp{};
    cp.initiator_depth = 1;
    cp.responder_resources = 1;
    cp.retry_count = 7;
    cp.rnr_retry_count = 7;
    if (rdma_connect(id, &cp) != 0) { fail("rdma_connect(" + address + ")"); }
    expect_cm_event(h->ec, RDMA_CM_EVENT_ESTABLISHED, timeout_ms);

    return h.release();
}

void rdma_close(RdmaClientHandle *h) { delete h; }

std::vector<uint8_t> rdma_invoke(RdmaClientHandle *h, const std::string &method,
                                  const std::vector<uint8_t> &req_body,
                                  const char *err_prefix) {
    if (h == nullptr) {
        throw std::runtime_error("rdma_invoke: null handle");
    }
    std::lock_guard<std::mutex> lk(h->conn.call_mu);

    size_t len = encode_frame(&h->conn, method, req_body);
    post_send(&h->conn, len);

    /* SEND 완료와 RECV 완료를 각각 기다린다. 순서는 보장되지 않으므로
     * opcode 로 분류한다 (커넥션당 1건이라 둘뿐이다). */
    bool got_send = false, got_recv = false;
    uint32_t received = 0;
    while (!got_send || !got_recv) {
        ibv_wc wc{};
        if (!wait_completion(&h->conn, &wc, kCallTimeoutMs, nullptr)) {
            throw std::runtime_error("rdma_invoke: timed out on " + method);
        }
        if (wc.opcode == IBV_WC_RECV) {
            got_recv = true;
            received = wc.byte_len;
        } else {
            got_send = true;
        }
    }

    std::string err;
    std::vector<uint8_t> body;
    decode_frame(&h->conn, received, err, body);

    /* 다음 호출을 위한 recv 를 즉시 다시 깔아 둔다 */
    post_recv(&h->conn);

    if (!err.empty()) {
        throw std::runtime_error(std::string(err_prefix) + err);
    }
    return body;
}

/* ============================================================
 * 서버
 * ============================================================ */

namespace {

/* 커넥션 하나를 끝까지 처리한다 (커넥션당 스레드 하나).
 * TCP 쪽 serve_rpc_connection 과 같은 골격이다. */
void serve_rdma_connection(RdmaConn *c, const char *tag,
                            const RpcMethodDispatcher &dispatch) {
    try {
        for (;;) {
            ibv_wc wc{};
            if (!wait_completion(c, &wc, -1, &c->stop)) {
                return;   /* stop 신호 */
            }
            if (wc.opcode != IBV_WC_RECV) {
                continue;   /* 직전 응답의 SEND 완료 */
            }

            std::string method;
            std::vector<uint8_t> body;
            decode_frame(c, wc.byte_len, method, body);

            std::string err;
            std::vector<uint8_t> rsp_body;
            try {
                if (!dispatch(method, body, rsp_body)) {
                    err = "unregistered method: " + method;
                }
            } catch (const std::exception &e) {
                err = e.what();
            }

            /* 응답을 보내기 **전에** 다음 요청 자리를 깔아 둔다 --
             * 클라이언트는 응답을 받는 즉시 다음 요청을 보낼 수 있다. */
            post_recv(c);

            size_t len = encode_frame(c, err, err.empty() ? rsp_body : std::vector<uint8_t>{});
            post_send(c, len);

            if (!err.empty()) {
                std::cerr << "[rpc-error] " << tag << " " << method << ": " << err << "\n";
            }
        }
    } catch (const std::exception &e) {
        if (!c->stop.load()) {
            std::cerr << "[rpc-error] " << tag << " connection closed: " << e.what() << "\n";
        }
    }
}

} /* anonymous namespace */

void run_rdma_listener(const std::string &bind_host, int port, const char *tag,
                        const RpcMethodDispatcher &dispatch,
                        std::atomic<bool> *stop_flag) {
    rdma_event_channel *ec = rdma_create_event_channel();
    if (ec == nullptr) { fail("rdma_create_event_channel"); }

    rdma_cm_id *listener = nullptr;
    if (rdma_create_id(ec, &listener, nullptr, RDMA_PS_TCP) != 0) { fail("rdma_create_id"); }

    sockaddr_in sin{};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(static_cast<uint16_t>(port));
    sin.sin_addr.s_addr = INADDR_ANY;
    if (!bind_host.empty() && bind_host != "0.0.0.0") {
        if (::inet_pton(AF_INET, bind_host.c_str(), &sin.sin_addr) != 1) {
            throw std::runtime_error("rdma: bind address '" + bind_host +
                                      "' is not an IPv4 address (RDMA needs the "
                                      "IPoIB address, not a hostname)");
        }
    }
    if (rdma_bind_addr(listener, reinterpret_cast<sockaddr *>(&sin)) != 0) {
        int e = errno;
        std::string where = (bind_host.empty() ? std::string("0.0.0.0") : bind_host) +
                            ":" + std::to_string(port);
        /* RDMA CM 은 **커널 TCP 포트 테이블과 별개의 포트 공간**을 쓴다.
         * 그래서 (1) 같은 포트를 이미 쓰는 RDMA 리스너가 있어도 `ss` 에는
         * 안 보이고, (2) librdmacm 은 그 충돌을 EADDRINUSE 가 아니라
         * EADDRNOTAVAIL 로 보고한다. 실제로 이 메시지 때문에 "주소가 없다"로
         * 오해하기 쉬워서 확인 방법을 같이 적는다. */
        throw std::runtime_error(
            "rdma: rdma_bind_addr(" + where + "): " + std::strerror(e) +
            " -- RDMA 포트 공간은 ss 에 안 보인다. 같은 포트를 쓰는 리스너가"
            " 있는지 `rdma resource show cm_id | grep :" + std::to_string(port) +
            "` 로 확인할 것");
    }
    if (rdma_listen(listener, 16) != 0) { fail("rdma_listen"); }

    std::vector<std::unique_ptr<RdmaConn>> conns;
    std::vector<std::thread> threads;

    for (;;) {
        if (stop_flag != nullptr && stop_flag->load()) { break; }

        pollfd pfd{};
        pfd.fd = ec->fd;
        pfd.events = POLLIN;
        int pr = ::poll(&pfd, 1, kPollSliceMs);
        if (pr < 0) {
            if (errno == EINTR) { continue; }
            break;
        }
        if (pr == 0) { continue; }

        rdma_cm_event *ev = nullptr;
        if (rdma_get_cm_event(ec, &ev) != 0) { break; }

        rdma_cm_event_type type = ev->event;
        rdma_cm_id *cid = ev->id;
        rdma_ack_cm_event(ev);

        if (type == RDMA_CM_EVENT_CONNECT_REQUEST) {
            auto c = std::unique_ptr<RdmaConn>(new RdmaConn());
            try {
                setup_conn(c.get(), cid);
                post_recv(c.get());
                rdma_conn_param cp{};
                cp.initiator_depth = 1;
                cp.responder_resources = 1;
                cp.rnr_retry_count = 7;
                if (rdma_accept(cid, &cp) != 0) { fail("rdma_accept"); }
            } catch (const std::exception &e) {
                std::cerr << "[rpc-error] " << tag << " accept failed: " << e.what() << "\n";
                rdma_reject(cid, nullptr, 0);
                c->id = nullptr;   /* cid 소유권을 넘기지 않는다 */
                continue;
            }
            cid->context = c.get();
            conns.push_back(std::move(c));
        } else if (type == RDMA_CM_EVENT_ESTABLISHED) {
            auto *c = static_cast<RdmaConn *>(cid->context);
            if (c != nullptr) {
                threads.emplace_back([c, tag, &dispatch]() {
                    serve_rdma_connection(c, tag, dispatch);
                });
            }
        } else if (type == RDMA_CM_EVENT_DISCONNECTED) {
            auto *c = static_cast<RdmaConn *>(cid->context);
            if (c != nullptr) { c->stop.store(true); }
        }
    }

    for (auto &c : conns) { c->stop.store(true); }
    for (auto &t : threads) { if (t.joinable()) { t.join(); } }
    conns.clear();

    rdma_destroy_id(listener);
    rdma_destroy_event_channel(ec);
}

bool rdma_devices_available() {
    int num = 0;
    ibv_device **list = ibv_get_device_list(&num);
    if (list == nullptr) { return false; }
    ibv_free_device_list(list);
    return num > 0;
}

} /* namespace nvmeof_raft */
