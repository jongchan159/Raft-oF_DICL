#ifndef RAFT_STATEMACHINE_HASH_HPP
#define RAFT_STATEMACHINE_HASH_HPP

#include "raft_server.h"

#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

/* ============================================================
 * HashStateMachine -- core/ 의 StateMachine 인터페이스를 구현한 상태머신.
 *
 * 적용된 명령들의 FNV-1a 해시와 개수를 들고 있어서, 세 노드가 같은 로그를
 * 같은 순서로 적용했는지 -op hash 로 비교할 수 있다.
 *
 * core/ 가 아니라 net/ 에 있는 이유: Raft 알고리즘이 아니라 **이 응용이
 * 고른 상태머신 구현체**이고, 실제 사용자가 apps/raft_node_main.cpp(생성)와
 * net/include/raft_tcp_server.h(-op hash 응답)다.
 * 이 헤더를 include하지만, **헤더 온리이므로 링크 의존이 생기지 않는다** --
 * 유닛 테스트는 core/ 만 링크한다(protobuf 없이).
 * 그 성질을 깨지 않도록 **여기에 .cpp 를 만들지 말 것.**
 * ============================================================ */

namespace nvmeof_raft {

/* ============================================================
 * HashStateMachine
 *
 * rpcproto.proto의 ClientGetHashRequest{at_count} /
 * ClientGetHashResponse{hash, count}가 전제하는 상태머신: 적용된
 * 명령들에 대한 롤링 해시와 적용 횟수를 유지한다. 벤치마크에서
 * "노드 N개가 정확히 같은 명령 순서를 같은 개수만큼 적용했는가"를
 * 한 문자열 비교로 확인하기 위한 것이다.
 *
 * 해시 함수는 FNV-1a 64비트를 쓴다 (외부 crypto 의존성 없이
 * 순서 민감한 체크섬을 얻기 위함 -- 충돌 저항이 필요한 용도가 아니고,
 * 두 노드의 적용 순서가 다르면 값이 달라지기만 하면 된다).
 *
 * apply()는 apply_pending()이 s.mu를 잡은 채로 호출하므로 절대
 * 블록하면 안 된다. 여기서는 짧은 뮤텍스 아래 산술만 한다.
 * ============================================================ */
class HashStateMachine : public StateMachine {
public:
    ApplyResult apply(const std::vector<uint8_t> &cmd) override {
        std::lock_guard<std::mutex> lk(mu_);
        for (uint8_t b : cmd) {
            hash_ ^= static_cast<uint64_t>(b);
            hash_ *= 1099511628211ull;   /* FNV-1a 64 prime */
        }
        count_++;
        ApplyResult r;
        /* 결과로 현재 count를 8바이트 LE로 돌려준다 (클라이언트가
         * "몇 번째로 적용됐는지" 확인할 수 있게) */
        r.result.resize(8);
        for (int i = 0; i < 8; i++) {
            r.result[static_cast<size_t>(i)] =
                static_cast<uint8_t>((count_ >> (8 * i)) & 0xFF);
        }
        return r;
    }

    struct Snapshot {
        std::string hash;
        uint64_t count;
    };

    Snapshot snapshot() const {
        std::lock_guard<std::mutex> lk(mu_);
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%016llx",
                      static_cast<unsigned long long>(hash_));
        return Snapshot{std::string(buf), count_};
    }

private:
    mutable std::mutex mu_;
    uint64_t hash_ = 14695981039346656037ull;   /* FNV-1a 64 offset basis */
    uint64_t count_ = 0;
};

} /* namespace nvmeof_raft */

#endif /* RAFT_STATEMACHINE_HASH_HPP */
