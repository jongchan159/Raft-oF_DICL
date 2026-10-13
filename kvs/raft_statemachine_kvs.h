#ifndef RAFT_STATEMACHINE_KVS_HPP
#define RAFT_STATEMACHINE_KVS_HPP

#include "raft_kvs_wire.h"
#include "raft_server.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

/* ============================================================
 * KvsStateMachine -- DARE KVS (kvs/dare_kvs_sm.c) 의 semantics 를 C++ 로
 * 옮긴 상태머신. DARE C 코드는 빌드에 넣지 않는다 (참조 원본).
 * [수정-16] C 코드를 링크하지 않은 이유는 DECISIONS.md D16.
 *
 * DARE 와 같은 것:  PUT 은 덮어쓰기, GET 은 값 (없으면 빈 결과), RM 은 삭제,
 *                   kvs_size = sum(엔트리 헤더 + 값 길이) 계수.
 * DARE 와 다른 것:  len 폭 (uint16 -> uint32, kvs/raft_kvs_wire.h),
 *                   자료구조 (1024 버킷 체이닝 -> unordered_map),
 *                   형식 검사 (DARE 는 하지 않는다).
 *
 * **호출 경로가 둘이다** (core/include/raft_server.h 의 락 규약 참고):
 *   - apply 경로 (apply_pending / become_leader replay): Server::mu 아래
 *   - read 경로  (Server::read, ReadIndex 의 마지막 단계): Server::mu 밖
 * 그래서 내부 mu_ 로 스스로 직렬화한다. read 경로의 GET 이 apply 경로를
 * 오래 막지 않도록 값은 **불변 버퍼(shared_ptr<const ...>)** 로 들고,
 * GET 은 mu_ 아래에서 포인터만 복사하고 바이트 복사는 락 밖에서 한다.
 *
 * 형식이 맞지 않는 명령 (리더 탐색 probe "\x00", -op apply 의 'x' 페이로드,
 * 빈 명령) 은 **에러 없이 무시**한다. 에러를 내면 apply_internal 이 그걸
 * 클라이언트에 실패로 돌려주는데, 그 명령은 이미 로그에 커밋됐다.
 *
 * 헤더 온리 (R0) -- 여기에 .cpp 를 만들지 말 것.
 * ============================================================ */

namespace nvmeof_raft {

class KvsStateMachine : public StateMachine {
public:
    struct Counters {
        uint64_t puts = 0;
        uint64_t gets = 0;
        uint64_t rms = 0;
        uint64_t ignored = 0;
        uint64_t keys = 0;
        uint64_t kvs_size = 0;
    };

    ApplyResult apply(const std::vector<uint8_t> &cmd) override {
        ApplyResult r;
        std::optional<kvs::View> v = kvs::parse(cmd);
        if (!v) {
            std::lock_guard<std::mutex> lk(mu_);
            c_.ignored++;
            return r;
        }

        switch (v->type) {
        case kvs::KVS_PUT: {
            /* 새 버퍼는 락 밖에서 만든다 -- 128 KiB 복사를 mu_ 아래 두지 않는다 */
            auto blob = std::make_shared<const std::vector<uint8_t>>(
                v->value, v->value + v->value_len);
            std::shared_ptr<const std::vector<uint8_t>> old;   /* 해제도 락 밖에서 */
            std::lock_guard<std::mutex> lk(mu_);
            c_.puts++;
            auto it = table_.find(v->key);
            if (it == table_.end()) {
                c_.kvs_size += kEntryOverhead + v->value_len;
                table_.emplace(v->key, std::move(blob));
            } else {
                c_.kvs_size += v->value_len;
                c_.kvs_size -= it->second->size();
                old = std::move(it->second);
                it->second = std::move(blob);
            }
            return r;
        }
        case kvs::KVS_GET: {
            std::shared_ptr<const std::vector<uint8_t>> blob;
            {
                std::lock_guard<std::mutex> lk(mu_);
                c_.gets++;
                auto it = table_.find(v->key);
                if (it != table_.end()) {
                    blob = it->second;
                }
            }
            if (blob) {
                r.result = *blob;
            }
            return r;
        }
        case kvs::KVS_RM: {
            std::shared_ptr<const std::vector<uint8_t>> old;
            std::lock_guard<std::mutex> lk(mu_);
            c_.rms++;
            auto it = table_.find(v->key);
            if (it != table_.end()) {
                c_.kvs_size -= kEntryOverhead + it->second->size();
                old = std::move(it->second);
                table_.erase(it);
            }
            return r;
        }
        default:
            return r;   /* parse 가 걸러서 오지 않는다 */
        }
    }

    Counters counters() const {
        std::lock_guard<std::mutex> lk(mu_);
        Counters c = c_;
        c.keys = table_.size();
        return c;
    }

private:
    /* DARE kvs_snapshot_entry_t {uint16 len; char key[64]; value[]} 의 헤더
     * 크기와 같은 의미 -- kvs_size 를 DARE 와 같은 방식으로 센다. */
    static constexpr uint64_t kEntryOverhead = 2 + kvs::kKeySize;

    mutable std::mutex mu_;
    std::unordered_map<std::string, std::shared_ptr<const std::vector<uint8_t>>> table_;
    Counters c_;
};

} /* namespace nvmeof_raft */

#endif /* RAFT_STATEMACHINE_KVS_HPP */
