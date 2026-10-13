/* ============================================================
 * KvsStateMachine / KVS 와이어 포맷 단위 테스트 (kvs/)
 *
 * DARE KVS(kvs/dare_kvs_sm.c) 와 같은 semantics 인지 -- PUT 덮어쓰기,
 * GET 은 값 또는 빈 결과, RM 삭제 -- 와, DARE 가 하지 않는 형식 검사가
 * 잘못된 명령을 **에러 없이 무시**하는지를 고정한다 (로그에 이미 커밋된
 * 명령이 클라이언트에 실패로 돌아가면 안 된다 -- kvs/README.md).
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_kvs_wire.h"
#include "raft_statemachine_kvs.h"

#include <string>
#include <vector>

using namespace nvmeof_raft;

namespace {

std::string as_string(const ApplyResult &r) {
    return std::string(r.result.begin(), r.result.end());
}

std::string get(KvsStateMachine &sm, const std::string &key) {
    ApplyResult r = sm.apply(kvs::encode_get(key));
    REQUIRE(r.error.empty());
    return as_string(r);
}

}  /* anonymous namespace */

TEST_CASE("kvs wire: encode/parse 왕복과 레이아웃") {
    auto cmd = kvs::encode_put("k1", "hello");
    REQUIRE(cmd.size() == kvs::kHeaderBytes + 5);
    CHECK(cmd[kvs::kOffType] == kvs::KVS_PUT);
    CHECK(cmd[kvs::kOffKey] == 'k');
    CHECK(cmd[kvs::kOffLen] == 5);   /* little-endian uint32 */
    CHECK(cmd[kvs::kOffLen + 1] == 0);

    auto v = kvs::parse(cmd);
    REQUIRE(v.has_value());
    CHECK(v->type == kvs::KVS_PUT);
    CHECK(v->key == "k1");
    CHECK(std::string(reinterpret_cast<const char *>(v->value), v->value_len) == "hello");

    auto g = kvs::parse(kvs::encode_get("k1"));
    REQUIRE(g.has_value());
    CHECK(g->type == kvs::KVS_GET);
    CHECK(g->value_len == 0);
}

TEST_CASE("kvs wire: encode 가 범위 밖 입력을 거부한다") {
    CHECK_THROWS(kvs::encode_put("", "v"));
    CHECK_THROWS(kvs::encode_put(std::string(kvs::kKeySize, 'k'), "v"));   /* NUL 자리 없음 */
    CHECK_NOTHROW(kvs::encode_put(std::string(kvs::kMaxKeyLen, 'k'), "v"));
    CHECK_THROWS(kvs::encode_put("k", ""));
    CHECK_THROWS(kvs::encode_put("k", std::string(kvs::kMaxValueBytes + 1, 'x')));
    CHECK_NOTHROW(kvs::encode_put("k", std::string(kvs::kMaxValueBytes, 'x')));
}

TEST_CASE("kvs: PUT 후 GET 은 값을 돌려준다 (8B .. 128KiB 커맨드)") {
    KvsStateMachine sm;
    for (size_t sz = 8; sz < kvs::kMaxValueBytes; sz *= 2) {
        const std::string key = "key" + std::to_string(sz);
        const std::string value(sz, 'x');
        CHECK(sm.apply(kvs::encode_put(key, value)).error.empty());
        CHECK(get(sm, key) == value);
    }
    const std::string big(kvs::kMaxValueBytes, 'y');
    sm.apply(kvs::encode_put("big", big));
    CHECK(get(sm, "big") == big);
}

TEST_CASE("kvs: PUT 은 덮어쓴다 (길이가 달라도)") {
    KvsStateMachine sm;
    sm.apply(kvs::encode_put("k", "short"));
    sm.apply(kvs::encode_put("k", std::string(1000, 'L')));
    CHECK(get(sm, "k") == std::string(1000, 'L'));
    sm.apply(kvs::encode_put("k", "s"));
    CHECK(get(sm, "k") == "s");
    CHECK(sm.counters().keys == 1);
}

TEST_CASE("kvs: 없는 키의 GET 은 빈 결과, RM 은 삭제한다") {
    KvsStateMachine sm;
    CHECK(get(sm, "missing").empty());
    sm.apply(kvs::encode_put("k", "v"));
    CHECK(sm.apply(kvs::encode_rm("k")).error.empty());
    CHECK(get(sm, "k").empty());
    CHECK(sm.apply(kvs::encode_rm("k")).error.empty());   /* 없는 키 RM 도 성공 */
    CHECK(sm.counters().keys == 0);
    CHECK(sm.counters().kvs_size == 0);
}

TEST_CASE("kvs: kvs_size 를 DARE 와 같은 방식으로 센다") {
    KvsStateMachine sm;
    sm.apply(kvs::encode_put("a", std::string(10, 'x')));
    sm.apply(kvs::encode_put("b", std::string(20, 'x')));
    /* 엔트리당 (2 + 64) + 값 길이 */
    CHECK(sm.counters().kvs_size == (66 + 10) + (66 + 20));
    sm.apply(kvs::encode_put("a", std::string(4, 'x')));
    CHECK(sm.counters().kvs_size == (66 + 4) + (66 + 20));
}

TEST_CASE("kvs: 형식이 맞지 않는 명령은 에러 없이 무시한다") {
    KvsStateMachine sm;
    sm.apply(kvs::encode_put("k", "v"));

    std::vector<std::vector<uint8_t>> bad;
    bad.push_back({0x00});                                   /* raft_client 리더 probe */
    bad.push_back({});                                       /* 빈 명령 */
    bad.push_back(std::vector<uint8_t>(512, 'x'));           /* -op apply 페이로드 */
    bad.push_back(std::vector<uint8_t>(kvs::kHeaderBytes - 1, 0));   /* 헤더 미만 */
    {
        auto c = kvs::encode_put("k", "zzz");
        c[kvs::kOffType] = 9;                                /* 모르는 type */
        bad.push_back(c);
    }
    {
        auto c = kvs::encode_put("k", "zzz");
        c[kvs::kOffLen] = 7;                                 /* len 이 실제와 다름 */
        bad.push_back(c);
    }
    {
        auto c = kvs::encode_put("k", "zzz");
        c.resize(kvs::kHeaderBytes);
        c[kvs::kOffLen] = 0;                                 /* PUT 인데 len 0 */
        bad.push_back(c);
    }
    {
        auto c = kvs::encode_put("k", "zzz");
        for (size_t i = 0; i < kvs::kKeySize; i++) {
            c[kvs::kOffKey + i] = 'k';                       /* key 에 NUL 없음 */
        }
        bad.push_back(c);
    }
    bad.push_back(std::vector<uint8_t>(kvs::kMaxCommandBytes + 1, 0));   /* 상한 초과 */

    for (const auto &c : bad) {
        ApplyResult r = sm.apply(c);
        CHECK(r.error.empty());
        CHECK(r.result.empty());
    }
    CHECK(sm.counters().ignored == bad.size());
    CHECK(get(sm, "k") == "v");   /* 상태 불변 */
}

TEST_CASE("kvs: GET 은 상태를 바꾸지 않고, 돌려준 값은 이후 PUT 에 영향받지 않는다") {
    KvsStateMachine sm;
    sm.apply(kvs::encode_put("k", "old"));
    const auto before = sm.counters();
    ApplyResult r = sm.apply(kvs::encode_get("k"));
    sm.apply(kvs::encode_put("k", "new-value"));

    CHECK(as_string(r) == "old");   /* 불변 버퍼: 결과는 GET 시점의 값 */
    CHECK(get(sm, "k") == "new-value");
    CHECK(before.keys == 1);
    CHECK(before.kvs_size == 66 + 3);
}
