#!/usr/bin/env bash
#
# build.sh — raftof_dare_hybrid 빌드 스크립트
#
# 전제 폴더 구조:
#   proto/  rpcproto.proto, rpcproto.pb.h, rpcproto.pb.cc
#   core/   Raft 알고리즘 자체 (네트워크 계층과 무관)
#   net/    네트워크 계층 (TCP, 나중에 RDMA로 교체될 부분)
#
# 사용법:
#   ./build.sh                  raft 라이브러리 오브젝트만 빌드
#   ./build.sh check            전체를 임시 바이너리로 링크해서 컴파일 오류만 확인
#   ./build.sh all              아래 바이너리 전부 빌드
#   ./build.sh node             raft_node (Raft 노드 프로세스)
#   ./build.sh client           raft_client (Client* RPC 도구 / 벤치 하네스)
#   ./build.sh blockcopy-server server_random 대응 스토리지 서버 바이너리
#   ./build.sh blkcopy-scale    raft_blkcopy_scale (WritePBABatch 처리량/포화점)
#   ./build.sh selftest         raft_selftest (persist <-> read_entry_direct 왕복 검증)
#   ./build.sh asan             raft_node_asan (ASan+UBSan, -O1 -g)
#                               스레드 수명 버그(fire-and-forget AE 워커가
#                               호출자 스택을 참조하던 문제)를 이걸로 잡았다.
#   ./build.sh regen-proto      .proto를 수정한 뒤 protoc으로 재생성
#   ./build.sh clean            build/ 삭제
#
# .proto 파일을 안 바꿨다면 protoc 설치 자체가 필요 없음 --
# 이미 있는 proto/rpcproto.pb.{h,cc}를 그대로 씀.
#
# protobuf를 시스템에 설치할 수 없는 환경(sudo 없음)에서는 .deb를 로컬에
# 풀어놓고 아래 두 변수로 넘길 수 있다:
#   PROTOBUF_SYSROOT=/path/to/sysroot ./build.sh all
#   (또는 PROTOBUF_CFLAGS / PROTOBUF_LIBS를 직접 지정)

set -euo pipefail

CXX=${CXX:-g++}
STD=c++17
BUILD_DIR=build

PROTO_DIR=proto
CORE_DIR=core/src
CORE_INC=core/include
NET_DIR=net/src
APPS_DIR=apps
NET_INC=net/include
STORAGE_DIR=storage
BLOCKIO_DIR=blockio
TESTS_DIR=tests

# core/의 .cpp 전부 (Raft 로직: 자료구조, 로컬 로그, replication,
# election, commit 판단 -- 네트워크 계층에 의존하지 않음)
CORE_SRCS=(
    "$CORE_DIR/raft_ring_helpers.cpp"
    "$CORE_DIR/raft_diagnostics.cpp"
    "$CORE_DIR/raft_persist.cpp"
    "$CORE_DIR/raft_pba.cpp"
    "$CORE_DIR/raft_commit.cpp"
    "$CORE_DIR/raft_append_entries.cpp"
    "$CORE_DIR/raft_handle_append_entries.cpp"
    "$CORE_DIR/raft_election.cpp"
    "$CORE_DIR/raft_apply.cpp"
    "$CORE_DIR/raft_lifecycle.cpp"
    "$CORE_DIR/raft_types.cpp"
)

# net/의 .cpp (rpc_call_append_entries/rpc_call_request_vote/
# blkcopy_write_pba_batch의 실제 구현. 이 파일들만 RDMA 버전으로
# 교체하면 core/는 안 건드려도 됨)
NET_SRCS=(
    "$NET_DIR/raft_rpc_client.cpp"
    "$NET_DIR/raft_blkcopy_rpc_client.cpp"
    "$NET_DIR/raft_tcp_server.cpp"
)

# blockio/ 의 .cpp -- O_DIRECT + FIEMAP 블록 I/O 계층.
# Raft를 전혀 모르고 core/ 를 include하지 않는다 (의존은 core -> blockio 한 방향).
# storage/ 와 그 net 껍데기. raft_blockcopy_server 만 링크한다 (core 불필요).
STORAGE_SRCS=(
    "$STORAGE_DIR/raft_blockcopy_server.cpp"
    "$NET_DIR/raft_blockcopy_tcp_server.cpp"
)

BLOCKIO_SRCS=(
    "$BLOCKIO_DIR/cached_fd.cpp"
)

# net/ 중 **core 도 protobuf 도 모르는** 계층 -- 프레이밍 / 소켓 / 인자 파싱.
# 세 개의 main 이 전부 링크한다. 이 그룹 덕분에 raft_client 와
# raft_blockcopy_server 가 core 를 끌어오지 않고도 헤더 밖 구현을 쓴다.
WIRE_SRCS=(
    "$NET_DIR/raft_cli.cpp"
    "$NET_DIR/raft_wire_codec.cpp"
    "$NET_DIR/raft_tcp_transport.cpp"
    "$NET_DIR/raft_rpc_listener.cpp"
)

PROTO_SRCS=(
    "$PROTO_DIR/rpcproto.pb.cc"
)

INCLUDES=(-I "$CORE_INC" -I "$NET_INC" -I "$STORAGE_DIR" -I "$BLOCKIO_DIR" -I "$PROTO_DIR")

# --- protobuf 플래그 결정 -------------------------------------------------
# 1) PROTOBUF_CFLAGS/PROTOBUF_LIBS가 있으면 그대로 사용
# 2) PROTOBUF_SYSROOT가 있으면 거기서 유도
# 3) pkg-config
# 4) 마지막 폴백 -lprotobuf
if [[ -n "${PROTOBUF_CFLAGS:-}" || -n "${PROTOBUF_LIBS:-}" ]]; then
    PB_CFLAGS="${PROTOBUF_CFLAGS:-}"
    PB_LIBS="${PROTOBUF_LIBS:--lprotobuf}"
elif [[ -n "${PROTOBUF_SYSROOT:-}" ]]; then
    PB_CFLAGS="-I${PROTOBUF_SYSROOT}/usr/include"
    PB_LIBS="-L${PROTOBUF_SYSROOT}/usr/lib/x86_64-linux-gnu -lprotobuf -Wl,-rpath,${PROTOBUF_SYSROOT}/usr/lib/x86_64-linux-gnu"
elif pkg-config --exists protobuf 2>/dev/null; then
    PB_CFLAGS="$(pkg-config --cflags protobuf)"
    PB_LIBS="$(pkg-config --libs protobuf)"
else
    PB_CFLAGS=""
    PB_LIBS="-lprotobuf"
fi

CXXFLAGS=(-std=$STD -Wall -Wextra -O2 "${INCLUDES[@]}")
LDFLAGS=(-lpthread)

# 단일 바이너리 빌드 헬퍼: build_bin <out> <src...>
build_bin() {
    local out="$1"; shift
    echo "[build.sh] linking $BUILD_DIR/$out ..."
    # shellcheck disable=SC2086
    $CXX "${CXXFLAGS[@]}" $PB_CFLAGS "$@" -o "$BUILD_DIR/$out" \
        $PB_LIBS "${LDFLAGS[@]}"
    echo "[build.sh] built: $BUILD_DIR/$out"
}

TARGET="${1:-}"

if [[ "$TARGET" == "clean" ]]; then
    rm -rf "$BUILD_DIR"
    echo "[build.sh] cleaned"
    exit 0
fi

mkdir -p "$BUILD_DIR"

if [[ "$TARGET" == "regen-proto" ]]; then
    PROTOC="${PROTOC:-protoc}"
    if command -v "$PROTOC" >/dev/null 2>&1; then
        echo "[build.sh] regenerating $PROTO_DIR/rpcproto.pb.{h,cc} from .proto ..."
        "$PROTOC" --cpp_out="$PROTO_DIR" -I "$PROTO_DIR" "$PROTO_DIR/rpcproto.proto"
    else
        echo "[build.sh] $PROTOC not found -- skip regen, using existing rpcproto.pb.{h,cc}" >&2
    fi
    exit 0
fi

# --- 개별 바이너리 타깃 ---------------------------------------------------
build_blockcopy_server() {
    build_bin raft_blockcopy_server \
        "$APPS_DIR/raft_blockcopy_server_main.cpp" \
        "${STORAGE_SRCS[@]}" "${WIRE_SRCS[@]}" "${PROTO_SRCS[@]}"
}

build_node() {
    build_bin raft_node \
        "$APPS_DIR/raft_node_main.cpp" \
        "${CORE_SRCS[@]}" "${BLOCKIO_SRCS[@]}" "${NET_SRCS[@]}" "${WIRE_SRCS[@]}" "${PROTO_SRCS[@]}"
}

build_client() {
    build_bin raft_client \
        "$APPS_DIR/raft_client_main.cpp" "${WIRE_SRCS[@]}" "${PROTO_SRCS[@]}"
}

# blockcopy 처리량/포화점 측정 도구 (지연 전용이던 raft_blkcopy_bench 를
# 2026-09-10 에 대체). raft_client 와 링크 구성이 같다 (wire + proto).
build_blkcopy_scale() {
    build_bin raft_blkcopy_scale \
        "$APPS_DIR/raft_blkcopy_scale_main.cpp" "${WIRE_SRCS[@]}" "${PROTO_SRCS[@]}"
}

# selftest는 core/ 만 링크한다. 전송이 추상 인터페이스(core/include/raft_transport.h)로
# 바뀌면서 core/가 net/의 심볼을 링크타임에 가져가지 않게 됐고, 그래서
# protobuf도 필요하지 않다.
build_selftest() {
    build_bin raft_selftest \
        "$TESTS_DIR/raft_selftest_main.cpp" \
        "${CORE_SRCS[@]}" "${BLOCKIO_SRCS[@]}"
}

# ASan/UBSan 빌드. raft_node와 같은 소스지만 -O1 -g -fsanitize로 짓는다.
# 사용법:
#   PROTOBUF_SYSROOT=... ./build.sh asan
#   (그 뒤 raft_node 대신 build/raft_node_asan을 띄우고 워크로드를 돌린다.
#    ASan 리포트는 노드의 stderr로 나온다 -> 노드 로그를 보면 된다.)
build_asan() {
    echo "[build.sh] linking $BUILD_DIR/raft_node_asan (ASan+UBSan) ..."
    # shellcheck disable=SC2086
    $CXX -std=$STD -Wall -Wextra -O1 -g \
        -fsanitize=address,undefined -fno-omit-frame-pointer \
        "${INCLUDES[@]}" $PB_CFLAGS \
        "$APPS_DIR/raft_node_main.cpp" \
        "${CORE_SRCS[@]}" "${BLOCKIO_SRCS[@]}" "${NET_SRCS[@]}" "${WIRE_SRCS[@]}" "${PROTO_SRCS[@]}" \
        -o "$BUILD_DIR/raft_node_asan" \
        $PB_LIBS "${LDFLAGS[@]}"
    echo "[build.sh] built: $BUILD_DIR/raft_node_asan"
}

case "$TARGET" in
    blockcopy-server) build_blockcopy_server; exit 0 ;;
    node)             build_node; exit 0 ;;
    client)           build_client; exit 0 ;;
    blkcopy-scale)    build_blkcopy_scale; exit 0 ;;
    selftest)         build_selftest; exit 0 ;;
    asan)             build_asan; exit 0 ;;
    all)
        build_blockcopy_server
        build_node
        build_client
        build_blkcopy_scale
        build_selftest
        echo "[build.sh] all binaries in $BUILD_DIR/"
        exit 0
        ;;
esac

# --- 기본 동작: 라이브러리 오브젝트 컴파일 (+ check 시 링크 검증) -------
ALL_SRCS=("${CORE_SRCS[@]}" "${BLOCKIO_SRCS[@]}" "${NET_SRCS[@]}" "${WIRE_SRCS[@]}" "${PROTO_SRCS[@]}")

echo "[build.sh] compiling ${#ALL_SRCS[@]} source files ..."

OBJS=()
for src in "${ALL_SRCS[@]}"; do
    base="$(basename "$src")"
    # 원래 코드는 ${base%.cpp}.o 뒤에 ${obj%.cc}.o를 또 붙여서
    # .cpp 입력에 대해 "name.o.o"를 만들고 있었다 (링크는 되지만 이름이 지저분).
    stem="${base%.cpp}"
    stem="${stem%.cc}"
    obj="$BUILD_DIR/${stem}.o"
    echo "  CXX $src"
    # shellcheck disable=SC2086
    $CXX "${CXXFLAGS[@]}" $PB_CFLAGS -c "$src" -o "$obj"
    OBJS+=("$obj")
done

echo "[build.sh] object files ready in $BUILD_DIR/"

if [[ "$TARGET" == "check" ]]; then
    echo "[build.sh] check mode: linking a throwaway binary to verify no undefined symbols"
    cat > "$BUILD_DIR/_check_main.cpp" <<'EOF'
int main() { return 0; }
EOF
    $CXX "${CXXFLAGS[@]}" -c "$BUILD_DIR/_check_main.cpp" -o "$BUILD_DIR/_check_main.o"
    # shellcheck disable=SC2086
    $CXX "${OBJS[@]}" "$BUILD_DIR/_check_main.o" -o "$BUILD_DIR/_check_link" \
        $PB_LIBS "${LDFLAGS[@]}"
    echo "[build.sh] link check passed: $BUILD_DIR/_check_link"
fi

echo "[build.sh] done."
