#ifndef RAFT_KVS_WIRE_HPP
#define RAFT_KVS_WIRE_HPP

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

/* ============================================================
 * KVS 명령의 와이어 포맷 -- DARE kvs_cmd_t (kvs/dare_kvs_sm.h) 의 의미를
 * 그대로 옮기되 **len 만 uint16 -> uint32 로 넓혔다** (값 64 KiB 상한 해제,
 * 커맨드 상한 128 KiB).
 *
 *   off  size  field
 *     0     1  type   KVS_PUT=1 / KVS_GET=2 / KVS_RM=3 (DARE 와 같은 값)
 *     1    64  key    NUL 종료 문자열 (최대 63 바이트 + NUL)
 *    65     3  pad    0
 *    68     4  len    값 길이, little-endian
 *    72   len  data   값 (PUT 만; GET/RM 은 len=0)
 *
 * C 구조체를 memcpy 하지 않고 오프셋으로 직접 읽고 쓴다 -- 정렬/패딩이
 * 컴파일러에 따라 달라지지 않게, 그리고 flexible array([0]) 를 C++ 에
 * 들이지 않기 위해서다.
 *
 * raft_client 와 KvsStateMachine 이 함께 쓴다. **헤더 온리이므로 링크
 * 의존이 생기지 않는다** -- raft_client 는 kvs 를 링크하지 않는다.
 * ============================================================ */

namespace nvmeof_raft {
namespace kvs {

enum : uint8_t { KVS_PUT = 1, KVS_GET = 2, KVS_RM = 3 };

constexpr size_t kKeySize = 64;                      /* DARE KEY_SIZE */
constexpr size_t kMaxKeyLen = kKeySize - 1;          /* NUL 자리 */
constexpr size_t kOffType = 0;
constexpr size_t kOffKey = 1;
constexpr size_t kOffLen = 68;
constexpr size_t kHeaderBytes = 72;
constexpr size_t kMaxCommandBytes = 128 * 1024;      /* 128 KiB */
constexpr size_t kMaxValueBytes = kMaxCommandBytes - kHeaderBytes;

/* parse 결과. key/value 는 cmd 버퍼를 가리킨다 (cmd 보다 오래 살면 안 됨). */
struct View {
    uint8_t type = 0;
    std::string key;
    const uint8_t *value = nullptr;
    size_t value_len = 0;
};

inline std::vector<uint8_t> encode(uint8_t type, const std::string &key,
                                   const uint8_t *value, size_t value_len) {
    if (key.empty() || key.size() > kMaxKeyLen) {
        throw std::invalid_argument("kvs: key must be 1.." +
                                    std::to_string(kMaxKeyLen) + " bytes");
    }
    if (key.find('\0') != std::string::npos) {
        throw std::invalid_argument("kvs: key must not contain NUL");
    }
    if (type == KVS_PUT && (value_len == 0 || value_len > kMaxValueBytes)) {
        throw std::invalid_argument("kvs: PUT value must be 1.." +
                                    std::to_string(kMaxValueBytes) + " bytes");
    }
    if (type != KVS_PUT && value_len != 0) {
        throw std::invalid_argument("kvs: GET/RM carry no value");
    }
    std::vector<uint8_t> cmd(kHeaderBytes + value_len, 0);
    cmd[kOffType] = type;
    std::memcpy(cmd.data() + kOffKey, key.data(), key.size());
    const uint32_t len = static_cast<uint32_t>(value_len);
    for (int i = 0; i < 4; i++) {
        cmd[kOffLen + static_cast<size_t>(i)] = static_cast<uint8_t>((len >> (8 * i)) & 0xFF);
    }
    if (value_len > 0) {
        std::memcpy(cmd.data() + kHeaderBytes, value, value_len);
    }
    return cmd;
}

inline std::vector<uint8_t> encode_put(const std::string &key, const uint8_t *value,
                                       size_t value_len) {
    return encode(KVS_PUT, key, value, value_len);
}

inline std::vector<uint8_t> encode_put(const std::string &key, const std::string &value) {
    return encode(KVS_PUT, key, reinterpret_cast<const uint8_t *>(value.data()),
                  value.size());
}

inline std::vector<uint8_t> encode_get(const std::string &key) {
    return encode(KVS_GET, key, nullptr, 0);
}

inline std::vector<uint8_t> encode_rm(const std::string &key) {
    return encode(KVS_RM, key, nullptr, 0);
}

/* 형식이 맞지 않으면 nullopt. 검사 항목:
 *   - 헤더(72B) 미만, 128 KiB 초과
 *   - type 이 PUT/GET/RM 이 아님
 *   - key 가 64 바이트 안에서 NUL 로 끝나지 않거나 비어 있음
 *   - len 이 실제 남은 바이트 수와 다름, PUT 인데 len=0, GET/RM 인데 len!=0
 * DARE 는 이 검사를 하지 않는다 (strcmp 가 key 밖을 읽을 수 있다). */
inline std::optional<View> parse(const std::vector<uint8_t> &cmd) {
    if (cmd.size() < kHeaderBytes || cmd.size() > kMaxCommandBytes) {
        return std::nullopt;
    }
    View v;
    v.type = cmd[kOffType];
    if (v.type != KVS_PUT && v.type != KVS_GET && v.type != KVS_RM) {
        return std::nullopt;
    }
    const char *k = reinterpret_cast<const char *>(cmd.data() + kOffKey);
    const void *nul = std::memchr(k, '\0', kKeySize);
    if (nul == nullptr || nul == k) {
        return std::nullopt;
    }
    v.key.assign(k, static_cast<const char *>(nul));

    uint32_t len = 0;
    for (int i = 0; i < 4; i++) {
        len |= static_cast<uint32_t>(cmd[kOffLen + static_cast<size_t>(i)]) << (8 * i);
    }
    if (len != cmd.size() - kHeaderBytes) {
        return std::nullopt;
    }
    if ((v.type == KVS_PUT) != (len > 0)) {
        return std::nullopt;
    }
    v.value = len > 0 ? cmd.data() + kHeaderBytes : nullptr;
    v.value_len = len;
    return v;
}

} /* namespace kvs */
} /* namespace nvmeof_raft */

#endif /* RAFT_KVS_WIRE_HPP */
