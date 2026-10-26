/* ============================================================
 * raft_push.cpp -- push 복제 모드 (DECISIONS.md D20, 진행 중)
 *
 * push 모드에서 리더는 자기 로그를 쓸 때 push 모드 팔로워의 볼륨에도
 * 직접 쓴다. 이 파일은 그 정책의 판정과 부기를 모은다. 실제 쓰기는
 * persist_circular(raft_persist.cpp)가 이 파일의 push_write_targets /
 * note_pushed / note_push_failure를 불러 가며 한다.
 *
 * push 대상 판정 (is_push_target):
 *   - 이 노드가 push 모드이고 리더다
 *   - 팔로워가 이번 term의 AE 응답에서 push 모드라고 알렸다
 *     (push_map_hash != 0 -- 그 값은 팔로워 링의 extent 맵 해시다)
 *   - 이 호스트에 그 팔로워 볼륨의 장치가 매핑돼 있다 (-push-devices).
 *     init_storage가 매핑된 장치를 전부 열고 실패하면 기동을 멈추므로,
 *     경로가 있으면 io.push_fds[fi]도 열려 있다. 경로로 판정하는 이유는
 *     단위 테스트가 실제 장치 없이 돌게 하기 위해서다.
 *   - 그 팔로워에게서 받아 검증한 맵을 설치했고, 그 해시가 지금 알린
 *     해시와 같다 -- 링이 바뀌었으면 낡은 맵으로 쓰지 않는다.
 * 조건을 만족하는 팔로워는 **전부** 대상이다.
 *
 * 맵은 **한 번만** 받는다. 리더는 알린 해시가 설치한 맵과 다를 때만
 * need_extent_map으로 요청하고, 설치한 맵은 선거가 바뀌어도 유지한다.
 *
 * pull 건너뛰기 (push_covers): AE 배치가 그 팔로워의 push 구간에 통째로
 * 들면 append_entries_worker가 data_already_copied를 세우고, 팔로워는
 * do_pba_copy 없이 메타데이터로만 로그를 잇는다.
 *
 * OK는 AE가 대신한다 (A안, DECISIONS.md D20): 처음 설계는 AE를 push와
 * 병렬로 보내고 쓰기가 끝나면 RDMA로 OK를 따로 보내는 것이었다. 지금은
 * AE가 push 쓰기와 flush가 끝난 뒤(Lock A 뒤)에만 나가므로 AE를 받았다는
 * 것 자체가 "네 볼륨에 내구성 있게 있다"는 뜻이다. 그래서 OK 채널이 없다.
 * ============================================================ */
#include "raft_server.h"
#include "cached_fd.h"   /* ExtentCache, CachedFD::set_extents */

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <sstream>

namespace nvmeof_raft {

namespace {

/* push 쓰기가 실패한 팔로워는 이만큼 push를 쉬고 pull로 간다. 경로가 끊긴
 * 볼륨에 매 Apply마다 쓰다가 mu를 오래 잡는 것을 막는다. */
constexpr int kPushRetryCooldownMs = 1000;

/* 맵이 덮는 마지막 바이트 다음 (= 링 파일 크기여야 한다) */
uint64_t map_end_bytes(const std::vector<Extent> &extents) {
    uint64_t end = 0;
    for (const auto &e : extents) {
        end = std::max(end, static_cast<uint64_t>(e.logical) + e.length);
    }
    return end;
}

std::string hex64(uint64_t v) {
    char buf[24];
    std::snprintf(buf, sizeof(buf), "0x%016llx", static_cast<unsigned long long>(v));
    return buf;
}

} /* anonymous namespace */

bool Server::is_push_target(int fi) const {
    if (replication_mode != ReplicationMode::Push || raft.state != ServerState::Leader) {
        return false;
    }
    if (fi < 0 || static_cast<size_t>(fi) >= raft.cluster.size() || fi == raft.cluster_index) {
        return false;
    }
    const ClusterMember &m = raft.cluster[static_cast<size_t>(fi)];
    return m.push_map_hash != 0 && !m.push_device_path.empty() &&
           m.push_map_cached_hash == m.push_map_hash;
}

std::vector<int> Server::push_targets() const {
    std::vector<int> out;
    for (size_t i = 0; i < raft.cluster.size(); i++) {
        if (is_push_target(static_cast<int>(i))) {
            out.push_back(static_cast<int>(i));
        }
    }
    return out;
}

std::vector<int> Server::push_write_targets(clock_type::time_point now) const {
    std::vector<int> out;
    for (int fi : push_targets()) {
        if (now >= raft.cluster[static_cast<size_t>(fi)].push_fail_until) {
            out.push_back(fi);
        }
    }
    return out;
}

bool Server::needs_extent_map(int fi) const {
    if (replication_mode != ReplicationMode::Push || raft.state != ServerState::Leader) {
        return false;
    }
    if (fi < 0 || static_cast<size_t>(fi) >= raft.cluster.size() || fi == raft.cluster_index) {
        return false;
    }
    const ClusterMember &m = raft.cluster[static_cast<size_t>(fi)];
    /* push_map_hash는 become_leader가 0으로 리셋하므로, term의 첫 응답으로
     * 해시를 알기 전에는 요청하지 않는다. 설치한 맵과 같으면 재사용한다. */
    return !m.push_device_path.empty() && m.push_map_hash != 0 &&
           m.push_map_hash != m.push_map_cached_hash &&
           m.push_map_hash != m.push_map_rejected_hash;
}

bool Server::push_covers(int fi, uint64_t first, uint64_t last) const {
    if (!is_push_target(fi) || first > last) {
        return false;
    }
    /* 구간은 note_pushed가 쓰고 flush까지 마친 뒤에만 넓히고, 팔로워 링이
     * 바뀌었을 수 있으면 비운다 -- 그래서 안에 든 배치는 지금 맵 기준으로
     * 팔로워 장치에 내구성 있게 있다. 일부만 걸쳐도 pull한다 (배치를
     * 쪼개 보내지 않는다). */
    const ClusterMember &m = raft.cluster[static_cast<size_t>(fi)];
    return m.push_upto != 0 && m.push_from <= first && last <= m.push_upto;
}

std::string Server::push_map_reject_reason(uint64_t advertised_hash, uint64_t computed_hash,
                                           uint64_t map_end, uint64_t ring_file_bytes,
                                           const std::string &mapped_wwid,
                                           const std::string &follower_wwid) {
    if (computed_hash != advertised_hash) {
        return "map hash mismatch (advertised " + hex64(advertised_hash) +
               ", computed " + hex64(computed_hash) + ")";
    }
    if (map_end != ring_file_bytes) {
        return "map covers " + std::to_string(map_end) + " bytes but this node's ring is " +
               std::to_string(ring_file_bytes) + " bytes (-ring-pages differs?)";
    }
    if (mapped_wwid.empty() || follower_wwid.empty()) {
        return "wwid unavailable (mapped device '" + mapped_wwid + "', follower volume '" +
               follower_wwid + "') -- cannot verify -push-devices";
    }
    if (mapped_wwid != follower_wwid) {
        return "wrong device: -push-devices points at wwid " + mapped_wwid +
               " but the follower's volume is " + follower_wwid;
    }
    return "";
}

void Server::note_push_response(int fi, const AppendEntriesResponse &rsp) {
    ClusterMember &m = raft.cluster[static_cast<size_t>(fi)];

    /* 알린 해시가 바뀌었다 = 팔로워 링이 바뀌었을 수 있다. 이전 해시일 때
     * push한 구간은 새 링에 없을 수 있으므로 버린다. (become_leader의
     * 0 리셋 뒤 첫 값은 "바뀜"이 아니다) */
    if (m.push_map_hash != 0 && rsp.push_map_hash != m.push_map_hash) {
        m.push_from = 0;
        m.push_upto = 0;
    }
    m.push_map_hash = rsp.push_map_hash;

    /* push 모드 팔로워인데 이 호스트에서 그 볼륨을 볼 수 없다 -- 설정
     * 실수일 가능성이 높으므로 한 번은 알린다. 이 팔로워는 pull로 남는다. */
    if (replication_mode == ReplicationMode::Push && rsp.push_map_hash != 0 &&
        m.push_device_path.empty() && !m.push_nodev_warned) {
        m.push_nodev_warned = true;
        std::ostringstream os;
        os << "[WARN] [PUSH] follower id=" << m.id
           << " runs in push mode but -push-devices has no entry for it on this host"
           << " -- it stays on the pull path";
        std::cerr << os.str() << std::endl;
    }

    /* 맵이 실려 왔으면 검증한 뒤 그 팔로워의 장치 핸들에 설치한다 */
    if (replication_mode != ReplicationMode::Push || rsp.extent_map.empty() ||
        rsp.push_map_hash == 0) {
        return;
    }
    ExtentCache cache = ExtentCache::from_extents(rsp.extent_map);
    uint64_t computed = cache.mapping_hash();
    std::string why = push_map_reject_reason(rsp.push_map_hash, computed,
                                             map_end_bytes(rsp.extent_map),
                                             ring.file_size_bytes(),
                                             m.push_device_wwid, rsp.volume_wwid);
    if (!why.empty()) {
        if (m.push_map_rejected_hash != rsp.push_map_hash) {
            std::ostringstream os;
            os << "[ERROR] [PUSH] follower id=" << m.id << ": extent map rejected -- " << why
               << " -- it stays on the pull path";
            std::cerr << os.str() << std::endl;
        }
        m.push_map_rejected_hash = rsp.push_map_hash;
        return;
    }

    /* 운영 경로에서는 push_device_path가 있으면 init_storage가 연 핸들도
     * 있다. 단위 테스트는 장치 없이 부기만 확인하므로 null이면 건너뛴다. */
    size_t n_extents = rsp.extent_map.size();
    if (static_cast<size_t>(fi) < io.push_fds.size() && io.push_fds[static_cast<size_t>(fi)]) {
        io.push_fds[static_cast<size_t>(fi)]->set_extents(std::move(cache));
    }
    m.push_map_cached_hash = computed;
    m.push_map_rejected_hash = 0;
    m.push_from = 0;   /* 새 맵 -- 그 전에 쓴 구간은 세지 않는다 */
    m.push_upto = 0;

    std::ostringstream os;
    os << "[PUSH] follower id=" << m.id << ": extent map installed (" << n_extents
       << " extents, hash " << hex64(computed) << ", wwid " << rsp.volume_wwid << ")";
    std::cerr << os.str() << std::endl;
}

void Server::note_pushed(int fi, uint64_t first, uint64_t last) {
    ClusterMember &m = raft.cluster[static_cast<size_t>(fi)];
    /* 앞 구간 바로 다음이면 이어 붙이고, 아니면 새로 시작한다. 중간에 이
     * 팔로워에게 쓰지 않은 호출이 있었다면 그 사이 엔트리가 팔로워 장치에
     * 있다고 말할 수 없기 때문이다. 구간을 여러 개 두지 않는다 -- AE 배치는
     * next_index부터 연속이고, 끊긴 앞쪽이 필요한 뒤처진 팔로워는 pull한다. */
    if (m.push_upto != 0 && m.push_upto + 1 == first) {
        m.push_upto = last;
    } else {
        m.push_from = first;
        m.push_upto = last;
    }
}

void Server::note_push_failure(int fi, const std::string &what) {
    ClusterMember &m = raft.cluster[static_cast<size_t>(fi)];
    m.push_from = 0;
    m.push_upto = 0;
    m.push_fail_until = clock_type::now() + std::chrono::milliseconds(kPushRetryCooldownMs);
    /* 쉬는 동안에는 쓰지 않으므로 이 로그는 쉬는 구간마다 한 번만 나온다 */
    std::ostringstream os;
    os << "[WARN] [PUSH] follower id=" << m.id << ": push write failed (" << what
       << ") -- pulling for the next " << kPushRetryCooldownMs << " ms";
    std::cerr << os.str() << std::endl;
}

} /* namespace nvmeof_raft */
