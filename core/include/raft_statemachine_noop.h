#ifndef RAFT_STATEMACHINE_NOOP_HPP
#define RAFT_STATEMACHINE_NOOP_HPP

#include "raft_server.h"

#include <cstdint>
#include <mutex>
#include <vector>

/* ============================================================
 * NoopStateMachine -- 명령 바이트를 읽지 않는 상태머신. **기본값이다.**
 *
 * 왜 이게 기본인가: 상태머신은 Raft 가 아니라 **애플리케이션**이다. Raft 의
 * 계약은 "커밋된 명령을 모든 노드에 같은 순서로 배달한다" 까지이고, 그 명령으로
 * 무엇을 하는가는 이 저장소의 관심사가 아니다. 그런데 자리를 채워 둔
 * HashStateMachine(net/include/raft_statemachine_hash.h)은 바이트마다 64비트
 * 곱셈을 하는 직렬 의존 사슬이라 **명령이 커지면 측정을 통째로 왜곡한다**
 * (eternity5 실측: 1 MiB 명령 하나당 2008us). 그건 측정 대상이 아니라
 * 측정 장비의 무게다.
 *
 * 그래서 기본은 비용 0 인 이 구현이고, 정합성 검증이 필요할 때만
 * `-statemachine hash` 로 해시를 켠다. 측정 명령줄에 그 플래그가 **없다는
 * 사실 자체가** "이 수치는 애플리케이션 비용 0 을 가정했다" 는 기록이 된다.
 * 노드 기동 로그의 `statemachine : noop` 줄이 같은 근거다.
 *
 * **Raft 의 배달 경로는 이걸 써도 한 줄도 바뀌지 않는다.** apply_pending 의
 * s.mu 획득, last_applied 전진, command.clear(), result_sink 호출은 전부
 * 그대로 돈다 -- result_sink 가 `if (!log_entry.command.empty())` 블록 **안**에
 * 있으므로(core/src/raft_commit.cpp), apply() 호출 자체를 건너뛰는 선택지는
 * 없다. 건너뛰면 클라이언트가 200ms 타임아웃에 걸린다.
 *
 * core/ 에 두는 이유: 의존성이 0 이고(이 세 헤더뿐), StateMachine 인터페이스
 * 자체가 core/include/raft_server.h 에 있다. net/ 에 두면 유닛 테스트가 쓸 수
 * 없다 -- 여기 있으면 나중에 apply_pending 을 protobuf 없이 덮는 테스트를
 * 쓸 수 있다. **여기에 .cpp 를 만들지 말 것** (core TU 전부가 링크 의존을
 * 얻는다) -- DECISIONS.md §X R0.
 * ============================================================ */

namespace nvmeof_raft {

class NoopStateMachine : public StateMachine {
public:
    ApplyResult apply(const std::vector<uint8_t> &cmd) override {
        (void)cmd;   /* 바이트를 읽지 않는다 = 애플리케이션 비용 0 */
        std::lock_guard<std::mutex> lk(mu_);
        count_++;

        /* 결과로 현재 count 를 8바이트 LE 로 돌려준다 -- HashStateMachine 과
         * 같은 계약이다 (클라이언트가 "몇 번째로 적용됐는지" 확인 가능).
         * 지금은 이 값이 와이어까지 가지는 않는다: ClientApplyResponse 에
         * 결과 필드가 없어서 net/src/raft_tcp_server.cpp 가 버린다. */
        ApplyResult r;
        r.result.resize(8);
        for (int i = 0; i < 8; i++) {
            r.result[static_cast<size_t>(i)] =
                static_cast<uint8_t>((count_ >> (8 * i)) & 0xFF);
        }
        return r;
    }

    uint64_t count() const {
        std::lock_guard<std::mutex> lk(mu_);
        return count_;
    }

private:
    mutable std::mutex mu_;
    uint64_t count_ = 0;
};

} /* namespace nvmeof_raft */

#endif /* RAFT_STATEMACHINE_NOOP_HPP */
