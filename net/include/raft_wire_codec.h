#ifndef RAFT_WIRE_CODEC_HPP
#define RAFT_WIRE_CODEC_HPP

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <stdexcept>
#include <arpa/inet.h>   /* htons/ntohs/htonl/ntohl -- big-endian 변환 */
#include <unistd.h>

namespace nvmeof_raft {

/* ============================================================
 * Wire framing (proto_codec.go의 writeAll/readFull 및 프레임 포맷
 * 그대로 포팅)
 *
 * 요청 프레임: [method_len(2B, BE)][method][seq(8B, BE)][body_len(4B, BE)][body]
 * 응답 프레임: [seq(8B, BE)][error_len(2B, BE)][error][body_len(4B, BE)][body]
 *
 * Go의 io.Writer/io.Reader 인터페이스는 fd 기반 read/write로 대응.
 * 이 레이어는 protobuf와 독립적이다 -- body 바이트를 그대로 옮기기만
 * 하고, 그 안의 직렬화 방식(protobuf)은 상위 계층(raft_proto_conv.h)
 * 책임이다.
 * ============================================================ */

void wire_write_all(int fd, const uint8_t *buf, size_t len);

void wire_read_full(int fd, uint8_t *buf, size_t len);

inline uint64_t wire_htobe64(uint64_t v) {
    uint32_t hi = htonl(static_cast<uint32_t>(v >> 32));
    uint32_t lo = htonl(static_cast<uint32_t>(v & 0xFFFFFFFFu));
    return (static_cast<uint64_t>(lo) << 32) | hi;
}
inline uint64_t wire_be64toh(uint64_t v) { return wire_htobe64(v); /* 대칭 연산 */ }

/* ============================================================
 * ClientCodec (protoClientCodec 원본 그대로 포팅)
 * ============================================================ */

class WireClientCodec {
public:
    explicit WireClientCodec(int fd);

    void write_request(const std::string &service_method, uint64_t seq, const std::vector<uint8_t> &body);

    struct ResponseHeader {
        uint64_t seq;
        std::string service_method;
        std::string error;
    };
    ResponseHeader read_response_header();

    std::vector<uint8_t> read_response_body();

private:
    int fd_;
    std::mutex write_mu_;
    std::mutex pending_mu_;
    std::unordered_map<uint64_t, std::string> pending_table_;
    uint32_t pending_body_len_ = 0;
    std::string pending_method_;
};

/* ============================================================
 * ServerCodec (protoServerCodec 원본 그대로 포팅)
 * ============================================================ */

class WireServerCodec {
public:
    explicit WireServerCodec(int fd);

    struct RequestHeader {
        std::string service_method;
        uint64_t seq;
    };

    RequestHeader read_request_header();

    std::vector<uint8_t> read_request_body();

    void write_response(uint64_t seq, const std::string &error, const std::vector<uint8_t> &body);

private:
    int fd_;
    std::mutex write_mu_;
    std::mutex pending_mu_;
    uint32_t pending_body_len_ = 0;
    std::string pending_method_;
};

} /* namespace nvmeof_raft */

#endif /* RAFT_WIRE_CODEC_HPP */