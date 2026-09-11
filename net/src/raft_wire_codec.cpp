#include "raft_wire_codec.h"

#include <cstring>
#include <stdexcept>
#include <string>

#include <unistd.h>

/* net/include/raft_wire_codec.h 의 구현. 선언과 프레임 포맷 설명은 그 헤더를 볼 것.
 *
 * 이 파일은 **protobuf 도 core 도 모른다** -- body 를 opaque 바이트로만
 * 다룬다. 그래서 raft_node / raft_client / raft_blockcopy_server 세 바이너리가
 * 전부 이 하나를 공유한다 (WIRE 그룹). */

namespace nvmeof_raft {

void wire_write_all(int fd, const uint8_t *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::write(fd, buf + off, len - off);
        if (n < 0) {
            throw std::runtime_error("wire_write_all: write failed");
        }
        off += static_cast<size_t>(n);
    }
}

void wire_read_full(int fd, uint8_t *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = ::read(fd, buf + off, len - off);
        if (n <= 0) {
            throw std::runtime_error("wire_read_full: read failed or connection closed");
        }
        off += static_cast<size_t>(n);
    }
}

WireClientCodec::WireClientCodec(int fd) : fd_(fd) {}

void WireClientCodec::write_request(const std::string &service_method, uint64_t seq,
                        const std::vector<uint8_t> &body){
        if (service_method.size() > 0xFFFF) {
            throw std::runtime_error("wire: method name too long");
        }
        size_t preamble = 2 + service_method.size() + 8 + 4;
        std::vector<uint8_t> buf(preamble + body.size());

        uint16_t mlen = htons(static_cast<uint16_t>(service_method.size()));
        std::memcpy(buf.data(), &mlen, 2);
        std::memcpy(buf.data() + 2, service_method.data(), service_method.size());

        uint64_t seq_be = wire_htobe64(seq);
        std::memcpy(buf.data() + 2 + service_method.size(), &seq_be, 8);

        uint32_t body_len_be = htonl(static_cast<uint32_t>(body.size()));
        std::memcpy(buf.data() + 2 + service_method.size() + 8, &body_len_be, 4);

        if (!body.empty()) {
            std::memcpy(buf.data() + preamble, body.data(), body.size());
        }

        std::lock_guard<std::mutex> lk(write_mu_);
        {
            std::lock_guard<std::mutex> lk2(pending_mu_);
            pending_table_[seq] = service_method;
        }
        wire_write_all(fd_, buf.data(), buf.size());
    }

WireClientCodec::ResponseHeader WireClientCodec::read_response_header(){
        uint8_t hdr[10];
        wire_read_full(fd_, hdr, sizeof(hdr));
        uint64_t seq;
        std::memcpy(&seq, hdr, 8);
        seq = wire_be64toh(seq);
        uint16_t err_len;
        std::memcpy(&err_len, hdr + 8, 2);
        err_len = ntohs(err_len);

        std::string err_buf;
        if (err_len > 0) {
            err_buf.resize(err_len);
            wire_read_full(fd_, reinterpret_cast<uint8_t *>(err_buf.data()), err_len);
        }

        uint8_t body_len_buf[4];
        wire_read_full(fd_, body_len_buf, 4);
        uint32_t body_len;
        std::memcpy(&body_len, body_len_buf, 4);
        body_len = ntohl(body_len);

        std::string method;
        {
            std::lock_guard<std::mutex> lk(pending_mu_);
            auto it = pending_table_.find(seq);
            if (it != pending_table_.end()) {
                method = it->second;
                pending_table_.erase(it);
            }
            pending_body_len_ = body_len;
            pending_method_ = method;
        }

        return {seq, method, err_buf};
    }

std::vector<uint8_t> WireClientCodec::read_response_body(){
        uint32_t body_len;
        std::string method;
        {
            std::lock_guard<std::mutex> lk(pending_mu_);
            body_len = pending_body_len_;
            method = pending_method_;
            pending_body_len_ = 0;
            pending_method_.clear();
        }
        (void)method;
        if (body_len == 0) {
            return {};
        }
        std::vector<uint8_t> body(body_len);
        wire_read_full(fd_, body.data(), body_len);
        return body;
    }

WireServerCodec::WireServerCodec(int fd) : fd_(fd) {}

WireServerCodec::RequestHeader WireServerCodec::read_request_header(){
        uint8_t mlen_buf[2];
        wire_read_full(fd_, mlen_buf, 2);
        uint16_t mlen;
        std::memcpy(&mlen, mlen_buf, 2);
        mlen = ntohs(mlen);
        if (mlen == 0) {
            throw std::runtime_error("wire: empty method name");
        }
        std::string method(mlen, '\0');
        wire_read_full(fd_, reinterpret_cast<uint8_t *>(method.data()), mlen);

        uint8_t seq_buf[8];
        wire_read_full(fd_, seq_buf, 8);
        uint64_t seq;
        std::memcpy(&seq, seq_buf, 8);
        seq = wire_be64toh(seq);

        uint8_t body_len_buf[4];
        wire_read_full(fd_, body_len_buf, 4);
        uint32_t body_len;
        std::memcpy(&body_len, body_len_buf, 4);
        body_len = ntohl(body_len);

        {
            std::lock_guard<std::mutex> lk(pending_mu_);
            pending_body_len_ = body_len;
            pending_method_ = method;
        }
        return {method, seq};
    }

std::vector<uint8_t> WireServerCodec::read_request_body(){
        uint32_t body_len;
        {
            std::lock_guard<std::mutex> lk(pending_mu_);
            body_len = pending_body_len_;
            pending_body_len_ = 0;
            pending_method_.clear();
        }
        if (body_len == 0) {
            return {};
        }
        std::vector<uint8_t> body(body_len);
        wire_read_full(fd_, body.data(), body_len);
        return body;
    }

void WireServerCodec::write_response(uint64_t seq, const std::string &error,
                         const std::vector<uint8_t> &body){
        if (error.size() > 0xFFFF) {
            throw std::runtime_error("wire: error string too long");
        }
        size_t preamble = 8 + 2 + error.size() + 4;
        std::vector<uint8_t> buf(preamble + body.size());

        uint64_t seq_be = wire_htobe64(seq);
        std::memcpy(buf.data(), &seq_be, 8);

        uint16_t err_len_be = htons(static_cast<uint16_t>(error.size()));
        std::memcpy(buf.data() + 8, &err_len_be, 2);
        if (!error.empty()) {
            std::memcpy(buf.data() + 10, error.data(), error.size());
        }

        uint32_t body_len_be = htonl(static_cast<uint32_t>(body.size()));
        std::memcpy(buf.data() + 10 + error.size(), &body_len_be, 4);

        /* 빈 body(예: 반환값 없는 RPC)일 때 vector::data()는 nullptr이고
         * memcpy(dst, nullptr, 0)은 형식상 UB -- UBSan이 잡는다. */
        if (!body.empty()) {
            std::memcpy(buf.data() + preamble, body.data(), body.size());
        }

        std::lock_guard<std::mutex> lk(write_mu_);
        wire_write_all(fd_, buf.data(), buf.size());
    }

} /* namespace nvmeof_raft */
