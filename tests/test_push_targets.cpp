/* ============================================================
 * push 모드 판정과 부기 단위 테스트 (core/src/raft_push.cpp, DECISIONS.md D20).
 *
 * 리더가 누구의 볼륨에 직접 쓸지는 네 조건으로 갈린다: 리더 자신이 push
 * 모드인가, 팔로워가 AE 응답에서 push 모드라고 알렸는가(push_map_hash),
 * 이 호스트에 그 팔로워 볼륨 장치가 매핑돼 있는가(push_device_path),
 * 받아서 설치한 맵이 지금 알린 해시와 같은가(push_map_cached_hash).
 * 잘못 판정하면 매핑이 없는 팔로워에게 쓰려 하거나, 낡은 맵이나 엉뚱한
 * 장치로 다른 블록을 덮어쓴다. 맵 검증·설치, push 구간 기록, 실패 후
 * 쉬기, push 구간으로 pull을 건너뛸지(push_covers -- 잘못 true면 팔로워
 * 볼륨에 없는 엔트리를 있다고 믿는다)도 여기서 본다. I/O 없이 판정과
 * 부기만 본다 -- 장치 핸들
 * (io.push_fds)이 없으므로 맵 설치는 set_extents를 건너뛰고 부기만 남는다.
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_server.h"
#include "cached_fd.h"   /* ExtentCache (맵 해시 계산) */

#include <chrono>
#include <string>

using namespace nvmeof_raft;

namespace {

/* id 4,5,6 의 3노드 클러스터. 이 노드는 index 0 (id 4) 이고 push 모드 리더다.
 * 두 팔로워 모두 push 모드라고 알렸고, 이 호스트에 장치가 매핑돼 있고,
 * 알린 해시와 같은 맵을 이미 설치했다 -- 즉 둘 다 push 대상이다. */
std::unique_ptr<Server> make_push_leader() {
    auto s = std::make_unique<Server>();
    s->ring.configure(2048);
    s->raft.cluster.resize(3);
    for (size_t i = 0; i < 3; i++) {
        s->raft.cluster[i].id = static_cast<uint64_t>(i + 4);
    }
    s->raft.id = 4;
    s->raft.cluster_index = 0;
    s->raft.state = ServerState::Leader;
    s->replication_mode = Server::ReplicationMode::Push;

    s->raft.cluster[1].push_map_hash = 0x1111;
    s->raft.cluster[1].push_map_cached_hash = 0x1111;
    s->raft.cluster[1].push_device_path = "/dev/nvme5n1";
    s->raft.cluster[1].push_device_wwid = "uuid.five";
    s->raft.cluster[2].push_map_hash = 0x2222;
    s->raft.cluster[2].push_map_cached_hash = 0x2222;
    s->raft.cluster[2].push_device_path = "/dev/nvme6n1";
    s->raft.cluster[2].push_device_wwid = "uuid.six";
    return s;
}

/* 이 노드의 링 파일 전체를 한 extent로 덮는 맵과, 그 맵을 실은 응답 */
AppendEntriesResponse map_response(const Server &s, const std::string &wwid) {
    AppendEntriesResponse rsp;
    rsp.extent_map = {Extent{0, 1000000, s.ring.file_size_bytes()}};
    rsp.push_map_hash = ExtentCache::from_extents(rsp.extent_map).mapping_hash();
    rsp.volume_wwid = wwid;
    return rsp;
}

}  /* anonymous namespace */

/* ---------------- 대상 판정 ---------------- */

TEST_CASE("조건을 만족하는 팔로워는 전부 대상이다") {
    auto s = make_push_leader();
    CHECK(s->push_targets() == std::vector<int>{1, 2});
}

TEST_CASE("자기 자신은 절대 대상이 아니다") {
    auto s = make_push_leader();
    /* 자기 항목에 값이 들어 있어도 무시한다 */
    s->raft.cluster[0].push_map_hash = 0x9999;
    s->raft.cluster[0].push_map_cached_hash = 0x9999;
    s->raft.cluster[0].push_device_path = "/dev/nvme4n1";
    CHECK(s->is_push_target(0) == false);
    CHECK(s->push_targets() == std::vector<int>{1, 2});
}

TEST_CASE("리더가 push 모드가 아니면 대상이 없다") {
    auto s = make_push_leader();
    s->replication_mode = Server::ReplicationMode::DestinationSide;
    CHECK(s->push_targets().empty());
}

TEST_CASE("리더 상태가 아니면 대상이 없다") {
    auto s = make_push_leader();
    s->raft.state = ServerState::Follower;
    CHECK(s->push_targets().empty());
}

TEST_CASE("push 모드라고 알리지 않은 팔로워는 pull로 남는다") {
    auto s = make_push_leader();
    s->raft.cluster[2].push_map_hash = 0;   /* destination 모드이거나 이번 term 응답 전 */
    CHECK(s->is_push_target(2) == false);
    CHECK(s->push_targets() == std::vector<int>{1});
}

TEST_CASE("이 호스트에 장치 매핑이 없는 push 팔로워는 대상이 아니다") {
    auto s = make_push_leader();
    s->raft.cluster[1].push_device_path.clear();
    CHECK(s->is_push_target(1) == false);
    CHECK(s->push_targets() == std::vector<int>{2});
}

TEST_CASE("맵을 설치하기 전이거나 설치한 맵과 해시가 다르면 대상이 아니다") {
    auto s = make_push_leader();
    s->raft.cluster[1].push_map_cached_hash = 0;          /* 아직 못 받았다 */
    CHECK(s->is_push_target(1) == false);
    s->raft.cluster[1].push_map_cached_hash = 0x9999;     /* 팔로워 링이 바뀌었다 */
    CHECK(s->is_push_target(1) == false);
}

TEST_CASE("범위 밖 인덱스는 대상이 아니다") {
    auto s = make_push_leader();
    CHECK(s->is_push_target(-1) == false);
    CHECK(s->is_push_target(3) == false);
}

/* ---------------- 맵 요청 (need_extent_map) ---------------- */

TEST_CASE("needs_extent_map: 설치한 맵과 같으면 다시 요청하지 않는다") {
    auto s = make_push_leader();
    CHECK(s->needs_extent_map(1) == false);
}

TEST_CASE("needs_extent_map: 맵이 없거나 알린 해시가 다르면 요청한다") {
    auto s = make_push_leader();
    s->raft.cluster[1].push_map_cached_hash = 0;
    CHECK(s->needs_extent_map(1) == true);
    s->raft.cluster[2].push_map_hash = 0x3333;   /* 팔로워가 다른 링으로 재시작 */
    CHECK(s->needs_extent_map(2) == true);
}

TEST_CASE("needs_extent_map: 이미 거절한 해시는 다시 요청하지 않는다") {
    auto s = make_push_leader();
    s->raft.cluster[1].push_map_cached_hash = 0;
    s->raft.cluster[1].push_map_rejected_hash = 0x1111;
    CHECK(s->needs_extent_map(1) == false);
}

TEST_CASE("needs_extent_map: 요청할 수 없는 경우") {
    auto s = make_push_leader();
    s->raft.cluster[1].push_map_cached_hash = 0;
    SUBCASE("이번 term 에 아직 해시를 모른다") {
        s->raft.cluster[1].push_map_hash = 0;
        CHECK(s->needs_extent_map(1) == false);
    }
    SUBCASE("이 호스트에 장치 매핑이 없다") {
        s->raft.cluster[1].push_device_path.clear();
        CHECK(s->needs_extent_map(1) == false);
    }
    SUBCASE("리더가 push 모드가 아니다") {
        s->replication_mode = Server::ReplicationMode::DestinationSide;
        CHECK(s->needs_extent_map(1) == false);
    }
    SUBCASE("자기 자신") {
        CHECK(s->needs_extent_map(0) == false);
    }
}

/* ---------------- 맵 검증 ---------------- */

TEST_CASE("push_map_reject_reason") {
    const uint64_t ring = 8ull << 20;
    SUBCASE("통과") {
        CHECK(Server::push_map_reject_reason(0xAB, 0xAB, ring, ring, "uuid.x", "uuid.x").empty());
    }
    SUBCASE("해시 불일치") {
        std::string r = Server::push_map_reject_reason(0xAB, 0xCD, ring, ring, "uuid.x", "uuid.x");
        CHECK(r.find("hash mismatch") != std::string::npos);
    }
    SUBCASE("링 크기 불일치 (-ring-pages가 노드마다 다름)") {
        std::string r = Server::push_map_reject_reason(0xAB, 0xAB, ring / 2, ring, "uuid.x", "uuid.x");
        CHECK(r.find("-ring-pages") != std::string::npos);
    }
    SUBCASE("wwid를 읽지 못해 검증할 수 없다") {
        std::string r = Server::push_map_reject_reason(0xAB, 0xAB, ring, ring, "", "uuid.x");
        CHECK(r.find("wwid unavailable") != std::string::npos);
    }
    SUBCASE("-push-devices가 다른 멤버의 볼륨을 가리킨다") {
        std::string r = Server::push_map_reject_reason(0xAB, 0xAB, ring, ring, "uuid.x", "uuid.y");
        CHECK(r.find("wrong device") != std::string::npos);
    }
}

/* ---------------- 응답 처리 (note_push_response) ---------------- */

TEST_CASE("note_push_response: 검증을 통과한 맵을 설치하면 대상이 된다") {
    auto s = make_push_leader();
    s->raft.cluster[1].push_map_cached_hash = 0;
    s->raft.cluster[1].push_from = 5;   /* 맵을 새로 설치하면 이전 구간은 버린다 */
    s->raft.cluster[1].push_upto = 9;

    AppendEntriesResponse rsp = map_response(*s, "uuid.five");
    s->note_push_response(1, rsp);

    CHECK(s->raft.cluster[1].push_map_cached_hash == rsp.push_map_hash);
    CHECK(s->raft.cluster[1].push_map_hash == rsp.push_map_hash);
    CHECK(s->raft.cluster[1].push_from == 0);
    CHECK(s->raft.cluster[1].push_upto == 0);
    CHECK(s->is_push_target(1) == true);
    CHECK(s->needs_extent_map(1) == false);
}

TEST_CASE("note_push_response: 장치 wwid가 다른 맵은 설치하지 않고 다시 요청하지도 않는다") {
    auto s = make_push_leader();
    s->raft.cluster[1].push_map_cached_hash = 0;

    AppendEntriesResponse rsp = map_response(*s, "uuid.six");   /* 매핑이 엇갈렸다 */
    s->note_push_response(1, rsp);

    CHECK(s->raft.cluster[1].push_map_cached_hash == 0);
    CHECK(s->raft.cluster[1].push_map_rejected_hash == rsp.push_map_hash);
    CHECK(s->is_push_target(1) == false);
    CHECK(s->needs_extent_map(1) == false);
}

TEST_CASE("note_push_response: 알린 해시가 바뀌면 push 구간을 버리고 맵을 다시 요청한다") {
    auto s = make_push_leader();
    s->raft.cluster[2].push_from = 10;
    s->raft.cluster[2].push_upto = 20;

    AppendEntriesResponse rsp;
    rsp.push_map_hash = 0x3333;   /* 팔로워 링이 바뀌었다 (맵은 아직 안 실림) */
    s->note_push_response(2, rsp);

    CHECK(s->raft.cluster[2].push_map_hash == 0x3333);
    CHECK(s->raft.cluster[2].push_from == 0);
    CHECK(s->raft.cluster[2].push_upto == 0);
    CHECK(s->is_push_target(2) == false);
    CHECK(s->needs_extent_map(2) == true);
}

TEST_CASE("note_push_response: 0에서 처음 알게 된 해시는 바뀐 것이 아니다") {
    auto s = make_push_leader();
    s->raft.cluster[1].push_map_hash = 0;   /* become_leader 리셋 직후 */
    s->raft.cluster[1].push_from = 5;
    s->raft.cluster[1].push_upto = 7;

    AppendEntriesResponse rsp;
    rsp.push_map_hash = 0x1111;
    s->note_push_response(1, rsp);

    CHECK(s->raft.cluster[1].push_from == 5);
    CHECK(s->raft.cluster[1].push_upto == 7);
    CHECK(s->is_push_target(1) == true);   /* 캐시한 맵과 같으므로 바로 재사용 */
}

TEST_CASE("note_push_response: destination 모드 팔로워는 pull로 남는다") {
    auto s = make_push_leader();
    AppendEntriesResponse rsp;   /* push_map_hash = 0 */
    s->note_push_response(1, rsp);
    CHECK(s->is_push_target(1) == false);
}

/* ---------------- push 구간과 실패 처리 ---------------- */

TEST_CASE("note_pushed: 이어지면 붙이고 끊기면 새로 시작한다") {
    auto s = make_push_leader();
    s->note_pushed(1, 102, 110);
    CHECK(s->raft.cluster[1].push_from == 102);
    CHECK(s->raft.cluster[1].push_upto == 110);

    s->note_pushed(1, 111, 115);   /* 바로 다음 Apply */
    CHECK(s->raft.cluster[1].push_from == 102);
    CHECK(s->raft.cluster[1].push_upto == 115);

    s->note_pushed(1, 120, 125);   /* 116..119는 이 팔로워에게 쓰지 않았다 */
    CHECK(s->raft.cluster[1].push_from == 120);
    CHECK(s->raft.cluster[1].push_upto == 125);
}

TEST_CASE("note_push_failure: 구간을 버리고 쉬는 동안 쓰기 대상에서 뺀다") {
    auto s = make_push_leader();
    s->note_pushed(1, 102, 110);

    auto now = clock_type::now();
    s->note_push_failure(1, "test");
    CHECK(s->raft.cluster[1].push_from == 0);
    CHECK(s->raft.cluster[1].push_upto == 0);

    CHECK(s->push_write_targets(now) == std::vector<int>{2});
    CHECK(s->is_push_target(1) == true);   /* 대상 자격은 그대로 -- 쓰기만 쉰다 */
    CHECK(s->push_write_targets(now + std::chrono::seconds(2)) == std::vector<int>{1, 2});
}

/* ---------------- pull 건너뛰기 (push_covers) ---------------- */

TEST_CASE("push_covers: 배치가 push 구간 안에 통째로 들면 true다") {
    auto s = make_push_leader();
    s->note_pushed(1, 102, 110);
    CHECK(s->push_covers(1, 102, 110) == true);   /* 경계가 정확히 같다 */
    CHECK(s->push_covers(1, 105, 107) == true);
    CHECK(s->push_covers(1, 102, 102) == true);
    CHECK(s->push_covers(1, 110, 110) == true);
}

TEST_CASE("push_covers: 일부라도 구간 밖이면 false다 -- 그 배치는 pull한다") {
    auto s = make_push_leader();
    s->note_pushed(1, 102, 110);
    CHECK(s->push_covers(1, 101, 105) == false);   /* 앞이 걸친다 (대상이 되기 전에 쓴 엔트리) */
    CHECK(s->push_covers(1, 108, 111) == false);   /* 뒤가 걸친다 */
    CHECK(s->push_covers(1, 90, 100) == false);    /* 통째로 앞 (catch-up) */
    CHECK(s->push_covers(1, 111, 115) == false);   /* 통째로 뒤 */
    CHECK(s->push_covers(1, 106, 105) == false);   /* 빈 배치 */
}

TEST_CASE("push_covers: 이어 붙은 구간은 합친 전체를 덮는다") {
    auto s = make_push_leader();
    s->note_pushed(1, 102, 110);
    s->note_pushed(1, 111, 115);
    CHECK(s->push_covers(1, 105, 113) == true);    /* 두 Apply에 걸친 배치 */
}

TEST_CASE("push_covers: 구간이 없으면 false다") {
    auto s = make_push_leader();
    CHECK(s->push_covers(1, 1, 1) == false);       /* 아직 push한 적 없다 */
    s->note_pushed(1, 102, 110);
    s->note_push_failure(1, "test");               /* 실패하면 구간을 버린다 */
    CHECK(s->push_covers(1, 102, 110) == false);
}

TEST_CASE("push_covers: 대상이 아니면 구간이 남아 있어도 false다") {
    auto s = make_push_leader();
    s->note_pushed(1, 102, 110);
    SUBCASE("설치한 맵과 알린 해시가 다르다") {
        s->raft.cluster[1].push_map_hash = 0x9999;
        CHECK(s->push_covers(1, 102, 110) == false);
    }
    SUBCASE("리더가 아니다") {
        s->raft.state = ServerState::Follower;
        CHECK(s->push_covers(1, 102, 110) == false);
    }
    SUBCASE("리더가 push 모드가 아니다") {
        s->replication_mode = Server::ReplicationMode::DestinationSide;
        CHECK(s->push_covers(1, 102, 110) == false);
    }
    SUBCASE("다른 팔로워의 구간은 보지 않는다") {
        CHECK(s->push_covers(2, 102, 110) == false);
    }
}
