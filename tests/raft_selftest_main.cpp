/* ============================================================
 * raft_selftest_main.cpp -- 네트워크 없이 도는 로컬 검증
 *
 * 검증 대상 (전부 이번에 고친 버그를 직접 겨냥한다):
 *
 *  T1  persist_circular <-> read_entry_direct 왕복
 *      리더가 O_DIRECT로 쓴 엔트리를 read_entry_direct로 다시 읽어
 *      명령 바이트가 정확히 일치하는지 본다. 이 함수는 슬롯 0의
 *      [32:512) 480바이트를 건너뛰는 버그가 있었고, become_leader가
 *      deferred 엔트리를 로드할 때 이 경로를 쓰므로 승격 시 로그가
 *      조용히 손상됐다. cmd_len을 1슬롯 미만/경계/여러 슬롯으로
 *      나눠 확인한다.
 *
 *  T2  링 slot 회계
 *      persist_circular가 기록한 log_slot_map의 (start, num_slots)가
 *      slots_for_entry와 일치하고 연속적인지 본다.
 *
 *  T3  leader_pba_for_range
 *      배치 범위의 PBA/길이가 identity extent 맵에서 기대값과 맞는지.
 *
 *  T4  트리밍 후 인덱스 시맨틱
 *      trim_log_locked가 log 앞부분을 잘라낸 뒤에도
 *      oldest_log_index()/log_slice()가 같은 절대 인덱스를 같은
 *      엔트리로 해석하는지.
 * ============================================================ */
#include "raft_server.h"
#include "raft_constants.h"
#include "raft_statemachine_hash.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <random>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <sys/types.h>

using namespace nvmeof_raft;

namespace {

int g_failures = 0;

void check(bool cond, const std::string &what) {
    if (cond) {
        std::printf("  [ok]   %s\n", what.c_str());
    } else {
        std::printf("  [FAIL] %s\n", what.c_str());
        g_failures++;
    }
}

std::vector<uint8_t> make_payload(size_t len, uint8_t seed) {
    std::vector<uint8_t> v(len);
    for (size_t i = 0; i < len; i++) {
        /* 위치에 따라 값이 달라지게 -- 480바이트 밀림 같은 오류가
         * 그냥 "0"이 아니라 "다른 값"으로도 잡히도록 */
        v[i] = static_cast<uint8_t>((i * 31 + seed * 7 + (i >> 8)) & 0xFF);
    }
    return v;
}

/* 테스트용 Server: 네트워크 없이 링 파일만 쓰는 리더 상태 */
std::unique_ptr<Server> make_leader(const std::string &dir, uint64_t id,
                                     uint64_t ring_pages) {
    auto s = std::make_unique<Server>();
    /* 링 크기는 init_storage보다 먼저 확정해야 한다 (fallocate 이후에 바꾸면
     * 슬롯 인덱스 해석이 깨진다). 예전에는 전역 configure_ring()이었다. */
    s->ring.configure(ring_pages);
    s->raft.id = id;
    s->raft.cluster_index = 0;
    s->io.metadata_dir = dir;
    s->raft.heartbeat_ms = 300;
    s->io.identity_pba = true;      /* FIEMAP 대신 논리==물리 */
    s->io.device_path = "";         /* 링 파일 자신을 볼륨으로 */
    s->statemachine = std::make_shared<HashStateMachine>();
    s->raft.cluster.resize(1);
    s->raft.cluster[0].id = id;
    s->init_storage();
    s->raft.state = ServerState::Leader;   /* persist_circular가 slot map을 채우게 */
    return s;
}

} /* anonymous namespace */

int main(int argc, char **argv) {
    std::string dir = "/tmp/raftof_selftest";
    if (argc > 1) {
        dir = argv[1];
    }

    /* 링을 작게: 2048 페이지 = 8MiB 파일 */
    constexpr uint64_t kRingPages = 2048;

    /* 작업 디렉터리를 스스로 만든다. 예전에는 호출자가 mkdir -p를 선행해야
     * 했고(README §5), 안 하면 init_storage가 예외를 던졌다. CTest가 인자만
     * 주고 바로 실행할 수 있게 여기서 만든다. 모드는 init_storage와 동일한 0755. */
    if (::mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
        std::fprintf(stderr, "mkdir %s failed: %s\n", dir.c_str(), std::strerror(errno));
        return 1;
    }

    /* 매번 새 파일에서 시작 */
    std::string ring_path = dir + "/raft-1.ring";
    ::remove(ring_path.c_str());

    std::unique_ptr<Server> s;
    try {
        s = make_leader(dir, 1, kRingPages);
    } catch (const std::exception &e) {
        std::fprintf(stderr, "init failed: %s\n", e.what());
        return 1;
    }

    std::printf("raft_selftest: dir=%s ring=%llu pages (%llu bytes, %llu slots)\n",
                dir.c_str(),
                static_cast<unsigned long long>(s->ring.num_pages),
                static_cast<unsigned long long>(s->ring.file_size_bytes()),
                static_cast<unsigned long long>(s->ring.ring_slots));

    /* ============================================================
     * T1: persist_circular -> read_entry_direct 왕복
     * ============================================================ */
    std::printf("\nT1: persist_circular <-> read_entry_direct round-trip\n");

    /* 경계값을 골고루: 슬롯 0 안에서만 끝나는 것(<=480),
     * 정확히 480, 481(2슬롯), 4064(8슬롯=1페이지), 그리고 큰 것 */
    std::vector<size_t> sizes = {1, 32, 479, 480, 481, 512, 1000, 4064, 4065, 9000};
    std::vector<std::vector<uint8_t>> payloads;
    std::vector<uint64_t> abs_indices;

    for (size_t k = 0; k < sizes.size(); k++) {
        std::vector<uint8_t> p = make_payload(sizes[k], static_cast<uint8_t>(k + 1));
        payloads.push_back(p);

        Entry e;
        e.term = 7;
        e.command = p;
        {
            std::lock_guard<std::mutex> lk(s->mu);
            s->raft.log.push_back(std::move(e));
            s->ring.tail_log_index++;
            abs_indices.push_back(s->ring.tail_log_index - 1);
            try {
                s->persist_circular(true, 1);
            } catch (const std::exception &ex) {
                std::printf("  [FAIL] persist_circular(%zu bytes): %s\n", sizes[k], ex.what());
                g_failures++;
            }
        }
    }

    for (size_t k = 0; k < sizes.size(); k++) {
        uint64_t idx = abs_indices[k];
        uint64_t slot = 0;
        bool have_slot = false;
        {
            std::lock_guard<std::mutex> lk(s->mu);
            auto it = s->ring.log_slot_map.find(idx);
            if (it != s->ring.log_slot_map.end()) {
                slot = it->second.start;
                have_slot = true;
            }
        }
        if (!have_slot) {
            std::printf("  [FAIL] no slot map entry for log index %llu\n",
                        static_cast<unsigned long long>(idx));
            g_failures++;
            continue;
        }

        try {
            std::lock_guard<std::mutex> lk(s->mu);
            Server::ReadEntryResult r = s->read_entry_direct(slot);
            bool len_ok = (r.entry.command.size() == payloads[k].size());
            bool bytes_ok = len_ok && (std::memcmp(r.entry.command.data(),
                                                    payloads[k].data(),
                                                    payloads[k].size()) == 0);
            bool term_ok = (r.entry.term == 7);
            char msg[192];
            if (bytes_ok && term_ok) {
                std::snprintf(msg, sizeof(msg),
                    "cmd_len=%zu (%llu slots) round-trips byte-exact",
                    payloads[k].size(),
                    static_cast<unsigned long long>(slots_for_entry(payloads[k].size())));
                check(true, msg);
            } else {
                /* 어디가 틀렸는지 구체적으로 */
                size_t first_bad = payloads[k].size();
                if (len_ok) {
                    for (size_t i = 0; i < payloads[k].size(); i++) {
                        if (r.entry.command[i] != payloads[k][i]) { first_bad = i; break; }
                    }
                }
                std::snprintf(msg, sizeof(msg),
                    "cmd_len=%zu: got_len=%zu term=%llu first_mismatch_at=%zu",
                    payloads[k].size(), r.entry.command.size(),
                    static_cast<unsigned long long>(r.entry.term), first_bad);
                check(false, msg);
            }
        } catch (const std::exception &ex) {
            std::printf("  [FAIL] read_entry_direct(slot=%llu): %s\n",
                        static_cast<unsigned long long>(slot), ex.what());
            g_failures++;
        }
    }

    /* ============================================================
     * T2: 슬롯 회계 (연속성 + slots_for_entry 일치)
     * ============================================================ */
    std::printf("\nT2: ring slot accounting\n");
    {
        std::lock_guard<std::mutex> lk(s->mu);
        bool all_ok = true;
        uint64_t expect_start = 0;
        for (size_t k = 0; k < sizes.size(); k++) {
            auto it = s->ring.log_slot_map.find(abs_indices[k]);
            if (it == s->ring.log_slot_map.end()) { all_ok = false; break; }
            uint64_t want_slots = slots_for_entry(sizes[k]);
            if (it->second.num_slots != want_slots) {
                std::printf("    idx %llu: num_slots=%llu want=%llu\n",
                    static_cast<unsigned long long>(abs_indices[k]),
                    static_cast<unsigned long long>(it->second.num_slots),
                    static_cast<unsigned long long>(want_slots));
                all_ok = false;
            }
            if (it->second.start != expect_start) {
                std::printf("    idx %llu: start=%llu want=%llu\n",
                    static_cast<unsigned long long>(abs_indices[k]),
                    static_cast<unsigned long long>(it->second.start),
                    static_cast<unsigned long long>(expect_start));
                all_ok = false;
            }
            expect_start += want_slots;
        }
        check(all_ok, "log_slot_map starts are contiguous and match slots_for_entry");
        check(s->ring.tail_slot == expect_start, "tail_slot advanced to the end of the last entry");
    }

    /* ============================================================
     * T3: leader_pba_for_range
     * ============================================================ */
    std::printf("\nT3: leader_pba_for_range on the identity extent map\n");
    {
        std::lock_guard<std::mutex> lk(s->mu);
        try {
            /* 슬롯 0부터 8슬롯 = 4096바이트. identity 맵이라
             * PBA == 논리 오프셋 == RING_OFFSET + 0 */
            Server::PbaRangeResult r = s->leader_pba_for_range(0, 8);
            check(r.pba_src == RING_OFFSET, "pba_src == RING_OFFSET for start_slot=0");
            check(r.nbytes == 4096, "nbytes == 4096 for 8 slots");

            Server::PbaRangeResult r2 = s->leader_pba_for_range(8, 8);
            check(r2.pba_src == RING_OFFSET + 8 * SECTOR_SIZE,
                  "pba_src tracks start_slot offset");
        } catch (const std::exception &ex) {
            std::printf("  [FAIL] leader_pba_for_range: %s\n", ex.what());
            g_failures++;
        }
    }

    /* ============================================================
     * T4: 트리밍 후에도 절대 인덱스 -> 엔트리 매핑이 보존되는가
     * ============================================================ */
    std::printf("\nT4: index semantics survive trim_log_locked\n");
    {
        std::lock_guard<std::mutex> lk(s->mu);

        /* 트리밍 전, 각 절대 인덱스가 가리키는 term/cmd 크기를 기록 */
        uint64_t probe_idx = abs_indices[sizes.size() - 1];
        size_t before_size = s->raft.log[s->log_slice(probe_idx)].command.size();
        uint64_t oldest_before = s->oldest_log_index();

        /* 앞쪽 3개를 잘라내도록 강제 */
        s->ring.log_trim_threshold = 3;
        s->raft.last_applied = abs_indices[3];   /* 3개는 apply 완료로 간주 */
        s->trim_log_locked(/*min_match=*/abs_indices[5]);

        uint64_t oldest_after = s->oldest_log_index();
        check(oldest_after > oldest_before, "oldest_log_index advanced after trim");
        check(oldest_after == abs_indices[3],
              "trim floor respected min(min_match, last_applied)");
        check(s->raft.log[s->log_slice(probe_idx)].command.size() == before_size,
              "log_slice() still resolves the same absolute index to the same entry");
        check(s->log_slice(oldest_after) == 1,
              "oldest live index maps to slice 1 (sentinel preserved at slice 0)");
    }

    std::printf("\n%s (%d failure%s)\n",
                g_failures == 0 ? "ALL PASSED" : "FAILURES PRESENT",
                g_failures, g_failures == 1 ? "" : "s");
    return g_failures == 0 ? 0 : 1;
}
