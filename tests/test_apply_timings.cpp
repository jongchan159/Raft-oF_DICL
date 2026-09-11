/* ============================================================
 * derive_apply_timings 단위 테스트 -- ApplyTimings 항등식
 *
 * HANDOFF §6.1은 이 항등식이 실측으로 오차 1.3% 안에서 성립함을 기록한다:
 *   Total ~= LHandler + LPersist + AENet + FHandler + ReplNet
 *            + StorageIO + QuorumWait
 *
 * 파생 공식을 펼치면 우변은 정확히 telescope된다:
 *   a_held + (ae_rt - r2) + (r2 - write_pba) + (write_pba - storage)
 *          + storage + (replicate - ae_rt)
 *   = a_held_ns + replicate_ns
 *
 * 즉 클램프가 걸리지 않는 입력(storage <= write_pba <= r2 <= ae_rt <=
 * replicate)에서는 **오차 0으로 성립해야 한다.** 실측의 1.3%는 Total을
 * 따로 측정하기 때문에 생기는 것이고, 파생 산술 자체는 정확하다.
 * 그 정확성을 여기서 결정론적으로 고정한다.
 * ============================================================ */
#include "third_party/doctest.h"

#include "raft_timings.h"

using namespace nvmeof_raft;

namespace {

/* 클램프가 걸리지 않는, 물리적으로 일관된 샘플 */
ReplSample ordered_sample() {
    ReplSample s;
    s.storage_copy_ns   = 186'600;   /* 스토리지 내부 pread+pwrite */
    s.write_pba_rt_ns   = 380'600;   /* + F<->S RPC */
    s.r2_ns             = 385'200;   /* + 팔로워 핸들러 부기 */
    s.ae_rt_ns          = 510'800;   /* + L<->F 전송 */
    s.mutex_ns          = 1'200;
    s.mutex_c_ns        = 400;
    s.post_rpc_wall_ns  = 5'000;
    return s;
}

ApplyWalls ordered_walls() {
    ApplyWalls w;
    w.nvme_ns         = 1'650'300;   /* O_DIRECT write + fdatasync */
    w.a_held_ns       = 1'658'900;   /* Lock A 보유 (nvme 포함) */
    w.replicate_ns    = 872'900;     /* 리더 -> 쿼럼 전체 */
    w.mutex_a_ns      = 1'200;
    w.commit_wait_ns  = 784'200;
    return w;
}

int64_t identity_rhs(const ApplyTimings &t) {
    return t.l_handler_ns + t.l_persist_ns + t.ae_net_ns + t.f_handler_ns +
           t.repl_net_ns + t.storage_io_ns + t.quorum_wait_ns;
}

}  /* anonymous namespace */

TEST_CASE("항등식 우변이 a_held + replicate 로 정확히 telescope된다") {
    ApplyWalls w = ordered_walls();
    ReplSink sink;
    sink.push(ordered_sample());
    ProfilingSink prof;
    ApplyTimings t;

    derive_apply_timings(w, &sink, prof, t);

    /* 클램프가 걸리지 않는 입력이므로 오차 0 */
    CHECK(identity_rhs(t) == w.a_held_ns + w.replicate_ns);
}

TEST_CASE("각 구간이 공식대로 계산된다") {
    ApplyWalls w = ordered_walls();
    ReplSample smp = ordered_sample();
    ReplSink sink;
    sink.push(smp);
    ProfilingSink prof;
    ApplyTimings t;

    derive_apply_timings(w, &sink, prof, t);

    CHECK(t.l_persist_ns == w.nvme_ns);
    CHECK(t.l_handler_ns == w.a_held_ns - w.nvme_ns);
    CHECK(t.ae_net_ns == smp.ae_rt_ns - smp.r2_ns);
    CHECK(t.f_handler_ns == smp.r2_ns - smp.write_pba_rt_ns);
    CHECK(t.repl_net_ns == smp.write_pba_rt_ns - smp.storage_copy_ns);
    CHECK(t.storage_io_ns == smp.storage_copy_ns);
    CHECK(t.quorum_wait_ns == w.replicate_ns - smp.ae_rt_ns);
    CHECK(t.replicate_ns == w.replicate_ns);
    CHECK(t.commit_wait_ns == w.commit_wait_ns);
    CHECK(t.pure_commit_wait_ns == w.commit_wait_ns);
}

TEST_CASE("보정값과 파생 서브스테이지") {
    ApplyWalls w = ordered_walls();
    ReplSample smp = ordered_sample();
    ReplSink sink;
    sink.push(smp);
    ProfilingSink prof;
    ApplyTimings t;

    derive_apply_timings(w, &sink, prof, t);

    /* 샘플의 mutex_ns가 0이 아니면 Lock A 대기값을 덮어쓴다 */
    CHECK(t.mutex_ns == smp.mutex_ns);
    CHECK(t.mutex_c_ns == smp.mutex_c_ns);
    CHECK(t.wait_lock_b_ns == smp.mutex_ns - smp.mutex_c_ns);

    CHECK(t.replicate_corrected_ns == w.replicate_ns - smp.mutex_ns);
    CHECK(t.quorum_wait_corrected_ns ==
          (w.replicate_ns - smp.mutex_ns) - smp.ae_rt_ns);
    CHECK(t.post_rpc_ns == smp.post_rpc_wall_ns);
    CHECK(t.wg_scheduling_ns ==
          timings_clamp0(t.quorum_wait_ns - t.post_rpc_ns - t.commit_wait_ns));
}

TEST_CASE("샘플의 mutex_ns가 0이면 Lock A 대기값을 유지한다") {
    ApplyWalls w = ordered_walls();
    ReplSample smp = ordered_sample();
    smp.mutex_ns = 0;
    ReplSink sink;
    sink.push(smp);
    ProfilingSink prof;
    ApplyTimings t;

    derive_apply_timings(w, &sink, prof, t);
    CHECK(t.mutex_ns == w.mutex_a_ns);
}

TEST_CASE("모든 파생 항이 음수가 되지 않는다 (clamp0)") {
    /* 일관성 없는 입력: 팔로워 핸들러가 리더가 잰 왕복보다 길게 나오는 등
     * 클럭 해상도 때문에 실제로 관측되는 상황이다. */
    ApplyWalls w;
    w.nvme_ns = 5'000;
    w.a_held_ns = 1'000;        /* nvme보다 작다 -> l_handler가 음수여야 하는 입력 */
    w.replicate_ns = 5;         /* ae_rt(10)보다 작다 -> quorum_wait가 음수여야 하는 입력 */
    w.mutex_a_ns = 0;
    w.commit_wait_ns = 99'999;  /* quorum_wait보다 크다 */

    ReplSample smp;
    smp.ae_rt_ns = 10;
    smp.r2_ns = 500;            /* ae_rt보다 크다 */
    smp.write_pba_rt_ns = 900;  /* r2보다 크다 */
    smp.storage_copy_ns = 5'000;/* write_pba보다 크다 */
    smp.post_rpc_wall_ns = 12'345;
    smp.mutex_c_ns = 7'777;
    smp.mutex_ns = 1;

    ReplSink sink;
    sink.push(smp);
    ProfilingSink prof;
    ApplyTimings t;

    derive_apply_timings(w, &sink, prof, t);

    CHECK(t.l_handler_ns == 0);
    CHECK(t.ae_net_ns == 0);
    CHECK(t.f_handler_ns == 0);
    CHECK(t.repl_net_ns == 0);
    CHECK(t.quorum_wait_ns == 0);
    CHECK(t.wait_lock_b_ns == 0);
    CHECK(t.replicate_corrected_ns >= 0);
    CHECK(t.quorum_wait_corrected_ns == 0);
    CHECK(t.wg_scheduling_ns == 0);
}

TEST_CASE("advance_commit_index 서브스테이지는 ProfilingSink에서 온다") {
    ApplyWalls w = ordered_walls();
    ReplSink sink;
    sink.push(ordered_sample());

    ProfilingSink prof;
    prof.aci_lock_wait_ns.store(11);
    prof.aci_quorum_ns.store(22);
    prof.aci_slot_gc_ns.store(33);
    prof.aci_apply_loop_ns.store(44);
    prof.aci_sort_ns.store(55);
    prof.aci_backpres_ns.store(66);
    prof.aci_signal_ns.store(77);

    ApplyTimings t;
    derive_apply_timings(w, &sink, prof, t);

    CHECK(t.aci_lock_wait_ns == 11);
    CHECK(t.aci_quorum_ns == 22);
    CHECK(t.aci_slot_gc_ns == 33);
    CHECK(t.aci_apply_loop_ns == 44);
    CHECK(t.aci_sort_ns == 55);
    CHECK(t.aci_backpres_ns == 66);
    CHECK(t.aci_signal_ns == 77);
}

TEST_CASE("샘플이 없으면 ProfilingSink의 폴백 값을 읽는다") {
    /* 배선 완료(2026-09-02). append_entries_worker가 data-bearing AE마다
     * sample_* 를 채우므로 이 폴백은 실제 노드에서 동작하는 경로다.
     * 예전에는 store가 0건이어서 항상 0을 읽었다. */
    ApplyWalls w = ordered_walls();
    ReplSink empty_sink;   /* push 없음 */

    ProfilingSink prof;
    prof.sample_ae_rt_ns.store(510'800);
    prof.sample_r2_ns.store(385'200);
    prof.sample_write_pba_rt_ns.store(380'600);
    prof.sample_storage_copy_ns.store(186'600);
    prof.sample_mutex_ns.store(1'200);
    prof.sample_mutex_c_ns.store(400);

    ApplyTimings t;
    derive_apply_timings(w, &empty_sink, prof, t);

    CHECK(t.ae_net_ns == 510'800 - 385'200);
    CHECK(t.storage_io_ns == 186'600);
    CHECK(t.mutex_ns == 1'200);
    /* 폴백 경로에서도 항등식은 성립한다 */
    CHECK(identity_rhs(t) == w.a_held_ns + w.replicate_ns);
}

TEST_CASE("샘플도 폴백도 없으면 전송 구간이 전부 0이 된다") {
    ApplyWalls w = ordered_walls();
    ReplSink empty_sink;
    ProfilingSink prof;   /* 전부 0 */
    ApplyTimings t;

    derive_apply_timings(w, &empty_sink, prof, t);

    CHECK(t.ae_net_ns == 0);
    CHECK(t.f_handler_ns == 0);
    CHECK(t.repl_net_ns == 0);
    CHECK(t.storage_io_ns == 0);
    /* QuorumWait이 replicate 전체를 흡수한다 -- 실제 노드에서 관측되는
     * "QuorumWait=0.0 Mutex=1656.8" 행의 반대 극단이다 */
    CHECK(t.quorum_wait_ns == w.replicate_ns);
    CHECK(identity_rhs(t) == w.a_held_ns + w.replicate_ns);
}
