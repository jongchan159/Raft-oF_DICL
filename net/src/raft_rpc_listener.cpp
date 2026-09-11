#include "raft_rpc_listener.h"

#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

/* net/include/raft_rpc_listener.h 의 구현. Raft 노드와 스토리지 노드가 같은 코드를 쓴다. */

namespace nvmeof_raft {

void send_http_connect_ok(int fd) {
    static const char *kResp = "HTTP/1.0 200 Connected to Go RPC\n\n";
    wire_write_all(fd, reinterpret_cast<const uint8_t *>(kResp), strlen(kResp));
}

void consume_http_connect_request(int fd) {
    char buf[256] = {0};
    size_t off = 0;
    while (off < sizeof(buf) - 1) {
        ssize_t n = ::read(fd, buf + off, 1);
        if (n <= 0) {
            throw std::runtime_error("consume_http_connect_request: read failed");
        }
        off += static_cast<size_t>(n);
        if (off >= 2 && buf[off - 1] == '\n' && buf[off - 2] == '\n') {
            return;
        }
    }
}

void serve_rpc_connection(int fd, const char *tag,
                                  const RpcMethodDispatcher &dispatch) {
    try {
        consume_http_connect_request(fd);
        send_http_connect_ok(fd);
    } catch (const std::exception &e) {
        std::cerr << "[warn] " << tag << " handshake failed: " << e.what() << std::endl;
        ::close(fd);
        return;
    }

    WireServerCodec codec(fd);
    while (true) {
        WireServerCodec::RequestHeader hdr;
        std::vector<uint8_t> body;
        try {
            hdr = codec.read_request_header();
            body = codec.read_request_body();
        } catch (const std::exception &) {
            break;   /* 커넥션 종료 (정상 종료와 구분하지 않는다 -- 원본 동작) */
        }

        std::vector<uint8_t> rsp_body;
        std::string error;
        try {
            if (!dispatch(hdr.service_method, body, rsp_body)) {
                error = "unregistered method: " + hdr.service_method;
            }
        } catch (const std::exception &e) {
            error = e.what();
        }

        /* 서버 쪽 에러는 이렇게 찍지 않으면 어디에도 안 남는다 --
         * 클라이언트는 "remote error: ..."만 받고, 프레임을 못 쓰면
         * 그마저도 못 본다. */
        if (!error.empty()) {
            std::cerr << "[rpc-error] " << tag << " " << hdr.service_method
                      << ": " << error << std::endl;
        }

        try {
            codec.write_response(hdr.seq, error, rsp_body);
        } catch (const std::exception &e) {
            std::cerr << "[rpc-error] " << tag << " write_response for "
                      << hdr.service_method << " failed: " << e.what() << std::endl;
            break;
        }
    }
    ::close(fd);
}

void run_rpc_listener(int port, const char *tag,
                             const std::function<void(int)> &on_connection,
                             std::atomic<bool> *stop_flag) {
    const std::string where = std::string("run_rpc_listener(") + tag + ")";

    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        throw std::runtime_error(where + ": socket() failed");
    }
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(listen_fd, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) != 0) {
        ::close(listen_fd);
        throw std::runtime_error(where + ": bind() failed on port " + std::to_string(port));
    }
    if (::listen(listen_fd, 16) != 0) {
        ::close(listen_fd);
        throw std::runtime_error(where + ": listen() failed");
    }

    if (stop_flag != nullptr) {
        struct timeval tv{};
        tv.tv_sec = 0;
        tv.tv_usec = 200 * 1000;
        setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    while (stop_flag == nullptr || !stop_flag->load()) {
        int conn_fd = ::accept(listen_fd, nullptr, nullptr);
        if (conn_fd < 0) {
            if (stop_flag != nullptr) {
                continue;
            }
            break;
        }
        /* [수정-3] accept()로 얻은 fd에서 SO_RCVTIMEO를 반드시 지운다.
         * Linux가 listen 소켓의 타임아웃을 accept된 소켓에 상속시키기
         * 때문이다. 지우지 않으면 롱리브드 RPC 커넥션이 200ms마다 죽어
         * RequestVote가 거의 항상 실패하고 선거가 끝나지 않는다.
         * 자세한 경위는 DECISIONS.md D3. */
        struct timeval no_timeout{};
        setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &no_timeout, sizeof(no_timeout));

        int one2 = 1;
        setsockopt(conn_fd, IPPROTO_TCP, TCP_NODELAY, &one2, sizeof(one2));

        std::thread([on_connection, conn_fd]() { on_connection(conn_fd); }).detach();
    }
    ::close(listen_fd);
}

} /* namespace nvmeof_raft */
