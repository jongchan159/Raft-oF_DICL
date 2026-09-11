#include "raft_tcp_transport.h"

#include <cstring>
#include <stdexcept>
#include <string>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

/* net/include/raft_tcp_transport.h 의 구현. 선언은 그 헤더를 볼 것.
 * tcp_connect / http_connect_handshake 는 이 파일 밖에서 쓰이지 않으므로
 * 익명 namespace 에 둔다 -- 예전에는 헤더의 `namespace detail` 에 노출돼 있었다. */

namespace nvmeof_raft {

namespace {

int tcp_connect(const std::string &host, int port, int timeout_sec) {
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *res = nullptr;
    std::string port_str = std::to_string(port);
    int gai_rc = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
    if (gai_rc != 0 || res == nullptr) {
        throw std::runtime_error("tcp_connect: getaddrinfo failed for " + host);
    }

    int fd = -1;
    for (struct addrinfo *p = res; p != nullptr; p = p->ai_next) {
        fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) {
            continue;
        }

        struct timeval tv{};
        tv.tv_sec = timeout_sec;
        tv.tv_usec = 0;
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            break;
        }
        ::close(fd);
        fd = -1;
    }
    freeaddrinfo(res);

    if (fd < 0) {
        throw std::runtime_error("tcp_connect: connect failed to " + host);
    }

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    return fd;
}

void http_connect_handshake(int fd) {
    static const char *kConnectReq = "CONNECT /_goRPC_ HTTP/1.0\n\n";
    wire_write_all(fd, reinterpret_cast<const uint8_t *>(kConnectReq), strlen(kConnectReq));

    char buf[256] = {0};
    size_t off = 0;
    while (off < sizeof(buf) - 1) {
        ssize_t n = ::read(fd, buf + off, 1);
        if (n <= 0) {
            throw std::runtime_error("http_connect_handshake: read failed");
        }
        off += static_cast<size_t>(n);
        if (off >= 2 && buf[off - 1] == '\n' && buf[off - 2] == '\n') {
            break;
        }
    }
    std::string resp(buf, off);
    if (resp.find("200") == std::string::npos) {
        throw std::runtime_error("http_connect_handshake: unexpected response: " + resp);
    }
}

}  /* anonymous namespace */

RpcClientHandle::~RpcClientHandle(){
        delete codec;
        if (fd >= 0) {
            ::close(fd);
        }
    }

std::vector<uint8_t> rpc_invoke(RpcClientHandle *h, const std::string &method,
                                        const std::vector<uint8_t> &req_body,
                                        const char *err_prefix) {   /* 기본값은 헤더에만 */
    std::lock_guard<std::mutex> lk(h->call_mu);
    uint64_t seq = h->next_seq++;

    h->codec->write_request(method, seq, req_body);
    auto hdr = h->codec->read_response_header();
    if (!hdr.error.empty()) {
        throw std::runtime_error(std::string(err_prefix) + hdr.error);
    }
    return h->codec->read_response_body();
}

RpcClientHandle *tcp_dial_http(const std::string &address) {
    size_t colon = address.rfind(':');
    if (colon == std::string::npos) {
        throw std::runtime_error("tcp_dial_http: invalid address " + address);
    }
    std::string host = address.substr(0, colon);
    int port = std::stoi(address.substr(colon + 1));

    int fd = tcp_connect(host, port, 2);
    http_connect_handshake(fd);

    auto *h = new RpcClientHandle();
    h->fd = fd;
    h->codec = new WireClientCodec(fd);
    return h;
}

} /* namespace nvmeof_raft */
