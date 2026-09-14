# CMakeLists.txt — raftof+dare
#
# build.sh와 **병행 유지**된다. build.sh는 여전히 동작하며 build/에 짓는다.
# CMake는 별도 디렉터리(관례상 build-cmake/)에 짓는다:
#
#   PROTOBUF_SYSROOT=/tmp/pb-sysroot cmake -S . -B build-cmake
#   cmake --build build-cmake -j
#   cd build-cmake && ctest --output-on-failure
#
# build.sh와 다른 점은 오직 "같은 소스를 두 번 컴파일하지 않는다"는 것이다.
# build.sh는 rpcproto.pb.cc(5,502줄)를 all에서 4번, core/를 2번 컴파일한다.
#
# 추가 타깃:
#   raft_node_asan   ASan+UBSan (build.sh asan 대응, 기본 빌드에서 제외)
#   raft_link_check  더미 main으로 undefined 심볼 검증 (build.sh check 대응)
#   regen-proto      protoc이 있으면 .pb.{h,cc} 재생성 (없으면 경고만)

cmake_minimum_required(VERSION 3.16)
project(raftof_dare LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)   # clangd / clang-tidy 용

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    # build.sh의 -O2에 대응
    set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

# build.sh와 동일한 경고 수준. HANDOFF §1이 "경고 0"을 보증하는 상태다.
add_compile_options(-Wall -Wextra)

# ---------------------------------------------------------------------------
# protobuf 탐색 — build.sh의 해당 절의 4단 폴백을 그대로 재현한다.
#   1) PROTOBUF_CFLAGS / PROTOBUF_LIBS 를 직접 준 경우
#   2) PROTOBUF_SYSROOT (.deb를 로컬에 풀거나 소스 빌드한 경우) — 이 서버의 주 경로
#   3) pkg-config protobuf
#   4) 마지막 폴백 -lprotobuf
# 환경변수로도, -D 캐시 변수로도 넘길 수 있다.
# ---------------------------------------------------------------------------
foreach(_v PROTOBUF_CFLAGS PROTOBUF_LIBS PROTOBUF_SYSROOT)
    if(NOT DEFINED ${_v} AND DEFINED ENV{${_v}})
        set(${_v} "$ENV{${_v}}")
    endif()
endforeach()

add_library(raft_protobuf INTERFACE)

if(PROTOBUF_CFLAGS OR PROTOBUF_LIBS)
    message(STATUS "protobuf: PROTOBUF_CFLAGS/LIBS 사용")
    separate_arguments(_pb_cflags UNIX_COMMAND "${PROTOBUF_CFLAGS}")
    if(NOT PROTOBUF_LIBS)
        set(PROTOBUF_LIBS "-lprotobuf")
    endif()
    separate_arguments(_pb_libs UNIX_COMMAND "${PROTOBUF_LIBS}")
    target_compile_options(raft_protobuf INTERFACE ${_pb_cflags})
    target_link_libraries(raft_protobuf INTERFACE ${_pb_libs})

elseif(PROTOBUF_SYSROOT)
    # build.sh는 x86_64-linux-gnu를 하드코딩한다. 여기서는 일반화하되
    # 실제로 존재하는 디렉터리를 고른다.
    set(_pb_libdirs "")
    if(CMAKE_LIBRARY_ARCHITECTURE)
        list(APPEND _pb_libdirs "${PROTOBUF_SYSROOT}/usr/lib/${CMAKE_LIBRARY_ARCHITECTURE}")
    endif()
    list(APPEND _pb_libdirs
        "${PROTOBUF_SYSROOT}/usr/lib/x86_64-linux-gnu"
        "${PROTOBUF_SYSROOT}/usr/lib64"
        "${PROTOBUF_SYSROOT}/usr/lib")
    set(_pb_libdir "")
    foreach(_d ${_pb_libdirs})
        if(EXISTS "${_d}" AND NOT _pb_libdir)
            set(_pb_libdir "${_d}")
        endif()
    endforeach()
    if(NOT _pb_libdir)
        message(FATAL_ERROR
            "PROTOBUF_SYSROOT=${PROTOBUF_SYSROOT} 안에서 라이브러리 디렉터리를 찾지 못했다.\n"
            "확인한 경로: ${_pb_libdirs}")
    endif()
    message(STATUS "protobuf: sysroot ${PROTOBUF_SYSROOT} (libdir ${_pb_libdir})")
    target_include_directories(raft_protobuf INTERFACE "${PROTOBUF_SYSROOT}/usr/include")
    # -Wl,-rpath 가 빠지면 실행 시 libprotobuf.so.NN not found 가 난다 (README §8)
    target_link_libraries(raft_protobuf INTERFACE
        "-L${_pb_libdir}" protobuf "-Wl,-rpath,${_pb_libdir}")

else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(PB QUIET protobuf)
    endif()
    if(PB_FOUND)
        message(STATUS "protobuf: pkg-config ${PB_VERSION}")
        target_include_directories(raft_protobuf INTERFACE ${PB_INCLUDE_DIRS})
        target_link_libraries(raft_protobuf INTERFACE ${PB_LINK_LIBRARIES})
    else()
        message(STATUS "protobuf: 폴백 -lprotobuf")
        target_link_libraries(raft_protobuf INTERFACE protobuf)
    endif()
endif()

find_package(Threads REQUIRED)

# ---------------------------------------------------------------------------
# 소스 목록 — build.sh의 해당 절과 동일 (glob을 쓰지 않는 것도 의도적으로 동일:
# 새 파일이 조용히 빌드에 끼어들지 않게 한다)
# ---------------------------------------------------------------------------
set(CORE_SRCS
    core/src/raft_ring_helpers.cpp
    core/src/raft_diagnostics.cpp
    core/src/raft_persist.cpp
    core/src/raft_pba.cpp
    core/src/raft_commit.cpp
    core/src/raft_append_entries.cpp
    core/src/raft_handle_append_entries.cpp
    core/src/raft_election.cpp
    core/src/raft_apply.cpp
    core/src/raft_lifecycle.cpp
    core/src/raft_types.cpp
)

# net/의 이 두 파일만 RDMA 버전으로 교체하면 core/는 안 건드려도 된다
# (현재 구조상 그렇다는 뜻이며, 진짜 인터페이스는 아니다 — DECISIONS.md 참고)
# net/ 중 **core 심볼을 필요로 하는** 것들. raft_node 전용이다.
set(NET_SRCS
    net/src/raft_rpc_client.cpp
    net/src/raft_blkcopy_rpc_client.cpp
    net/src/raft_tcp_server.cpp
)

# blockio/ -- O_DIRECT + FIEMAP 블록 I/O 계층. Raft를 전혀 모르고 core/ 를
# include하지 않는다 (의존은 core -> blockio 한 방향).
# storage/ 와 그 net 껍데기. raft_blockcopy_server 만 링크한다 (core 불필요).
# 예전에는 storage/ 가 헤더 온리라 어느 빌드에도 컴파일 대상이 없었다.
set(STORAGE_SRCS storage/raft_blockcopy_server.cpp net/src/raft_blockcopy_tcp_server.cpp)

set(BLOCKIO_SRCS blockio/cached_fd.cpp)

# net/ 중 **core 도 protobuf 도 모르는** 계층 -- 프레이밍 / 소켓 / 인자 파싱.
# 세 개의 main(raft_node / raft_client / raft_blockcopy_server)이 전부 링크한다.
# 이 그룹이 있어야 raft_client 와 raft_blockcopy_server 가 core 를 끌어오지
# 않고도 헤더 밖 구현을 쓸 수 있다.
set(WIRE_SRCS net/src/raft_cli.cpp net/src/raft_wire_codec.cpp
              net/src/raft_tcp_transport.cpp net/src/raft_rpc_listener.cpp)

set(PROTO_SRCS proto/rpcproto.pb.cc)

# 아래 다섯 디렉터리를 모든 타깃에 -I 로 준다 (build.sh 의 INCLUDES 와 동일).
# apps/ 는 없다 -- 진입점은 아무 곳에서도 include 되지 않는다.
add_library(raft_includes INTERFACE)
target_include_directories(raft_includes INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/core/include
    ${CMAKE_CURRENT_SOURCE_DIR}/net/include
    ${CMAKE_CURRENT_SOURCE_DIR}/storage
    ${CMAKE_CURRENT_SOURCE_DIR}/blockio
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
)

# ---------------------------------------------------------------------------
# OBJECT 라이브러리 — 한 번만 컴파일해서 여러 바이너리가 공유한다
# ---------------------------------------------------------------------------
add_library(raft_proto_obj OBJECT ${PROTO_SRCS})
target_link_libraries(raft_proto_obj PUBLIC raft_includes raft_protobuf)

add_library(raft_blockio_obj OBJECT ${BLOCKIO_SRCS})
target_link_libraries(raft_blockio_obj PUBLIC raft_includes)

add_library(raft_core_obj OBJECT ${CORE_SRCS})
# core/ 는 protobuf 를 전혀 모른다 (grep 확인). raft_protobuf 를 붙여 두면
# raft_unit_tests 의 링크 라인에 -lprotobuf 가 실리고, 실제로
# 의존이 안 생기는 것은 --as-needed 덕분이 된다. 빼서 그 성질을 우연이 아니라
# 빌드 그래프의 사실로 만든다.
target_link_libraries(raft_core_obj PUBLIC raft_includes Threads::Threads)

add_library(raft_storage_obj OBJECT ${STORAGE_SRCS})
target_link_libraries(raft_storage_obj PUBLIC raft_includes raft_protobuf)

add_library(raft_wire_obj OBJECT ${WIRE_SRCS})
target_link_libraries(raft_wire_obj PUBLIC raft_includes)

add_library(raft_net_obj OBJECT ${NET_SRCS})
target_link_libraries(raft_net_obj PUBLIC raft_includes raft_protobuf Threads::Threads)

# ---------------------------------------------------------------------------
# 바이너리 — 타깃별 의존성 차이를 build.sh의 해당 절 그대로 유지한다.
# raft_client / raft_blockcopy_server 는 core/net 을 링크하지 않는다.
# ---------------------------------------------------------------------------
add_executable(raft_node apps/raft_node_main.cpp)
target_link_libraries(raft_node PRIVATE
    raft_core_obj raft_blockio_obj raft_net_obj raft_wire_obj raft_proto_obj raft_includes raft_protobuf Threads::Threads)

add_executable(raft_client apps/raft_client_main.cpp)
target_link_libraries(raft_client PRIVATE
    raft_wire_obj raft_proto_obj raft_includes raft_protobuf Threads::Threads)

add_executable(raft_blockcopy_server apps/raft_blockcopy_server_main.cpp)
target_link_libraries(raft_blockcopy_server PRIVATE
    raft_storage_obj raft_wire_obj raft_proto_obj raft_includes raft_protobuf Threads::Threads)

# blockcopy 처리량/포화점 측정 도구 (지연 전용이던 raft_blkcopy_bench 를
# 2026-09-10 에 대체했다 -- DECISIONS.md 참고). raft_client 와 링크 구성이
# 같다 (wire + proto): WritePBABatch 를 직접 쏘므로 core 도, storage 구현도
# 필요 없다.
add_executable(raft_blkcopy_scale apps/raft_blkcopy_scale_main.cpp)
target_link_libraries(raft_blkcopy_scale PRIVATE
    raft_wire_obj raft_proto_obj raft_includes raft_protobuf Threads::Threads)

# --- ASan+UBSan (build.sh asan 대응) --------------------------------------
# 플래그가 달라 오브젝트를 공유할 수 없으므로 소스에서 직접 짓는다.
# 기본 빌드에서 제외: cmake --build build-cmake --target raft_node_asan
add_executable(raft_node_asan EXCLUDE_FROM_ALL
    apps/raft_node_main.cpp ${CORE_SRCS} ${BLOCKIO_SRCS} ${NET_SRCS} ${WIRE_SRCS} ${PROTO_SRCS})
target_link_libraries(raft_node_asan PRIVATE raft_includes raft_protobuf Threads::Threads)
target_compile_options(raft_node_asan PRIVATE
    -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer)
target_link_options(raft_node_asan PRIVATE -fsanitize=address,undefined)

# --- 링크 검증 (build.sh check 대응) --------------------------------------
# core/ 는 net/ 이 정의하는 심볼 5개(rpc_call_*, blkcopy_*)를 링크타임에
# 가져온다. 더미 main에 core+net+proto 를 모두 링크해 undefined 심볼이
# 없는지 확인한다. (이 링크 종속 자체가 리팩토링 대상 — DECISIONS.md 참고)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/_check_main.cpp" "int main() { return 0; }\n")
add_executable(raft_link_check EXCLUDE_FROM_ALL "${CMAKE_CURRENT_BINARY_DIR}/_check_main.cpp")
target_link_libraries(raft_link_check PRIVATE
    raft_core_obj raft_blockio_obj raft_net_obj raft_wire_obj raft_proto_obj raft_includes raft_protobuf Threads::Threads)

# --- regen-proto (build.sh regen-proto 대응) -------------------------------
# protoc이 없으면 **실패시키지 않는다** — 커밋된 .pb.{h,cc}를 그대로 쓴다.
set(_protoc_hints "")
if(PROTOBUF_SYSROOT)
    list(APPEND _protoc_hints "${PROTOBUF_SYSROOT}/usr/bin")
endif()
find_program(PROTOC_EXECUTABLE protoc HINTS ${_protoc_hints})
if(PROTOC_EXECUTABLE)
    message(STATUS "protoc: ${PROTOC_EXECUTABLE}")
    add_custom_target(regen-proto
        COMMAND ${PROTOC_EXECUTABLE} --cpp_out=${CMAKE_CURRENT_SOURCE_DIR}/proto
                -I ${CMAKE_CURRENT_SOURCE_DIR}/proto
                ${CMAKE_CURRENT_SOURCE_DIR}/proto/rpcproto.proto
        COMMENT "regenerating proto/rpcproto.pb.{h,cc}")
else()
    add_custom_target(regen-proto
        COMMAND ${CMAKE_COMMAND} -E echo
                "protoc를 찾지 못했다 -- 재생성을 건너뛰고 기존 rpcproto.pb.{h,cc}를 쓴다"
        COMMENT "regen-proto (protoc 없음)")
endif()

# ---------------------------------------------------------------------------
# 테스트 (CTest)
# ---------------------------------------------------------------------------
enable_testing()

# --- 단위 테스트 (doctest) -------------------------------------------------
# core/ 는 전송을 core/raft_transport.h의 추상 인터페이스로만 다루므로,
# 테스트는 tests/fake_transport.h의 Mock을 주입하면 된다 --
# **protobuf도 소켓도 net/ 도 링크하지 않는다.**
add_executable(raft_unit_tests
    tests/test_main.cpp
    tests/test_ring_helpers.cpp
    tests/test_extent_map.cpp
    tests/test_ae_bookkeeping.cpp
    tests/test_apply_timings.cpp
    tests/test_election.cpp
)
target_include_directories(raft_unit_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
target_link_libraries(raft_unit_tests PRIVATE
    raft_core_obj raft_blockio_obj raft_includes Threads::Threads)
# doctest 헤더는 우리 코드가 아니므로 경고를 요구하지 않는다
set_source_files_properties(tests/third_party/doctest.h PROPERTIES COMPILE_OPTIONS "-w")

add_test(NAME unit COMMAND raft_unit_tests)

# --- 컴포넌트 격리 불변식 --------------------------------------------------
# 이 네 개가 계층 규약을 **테스트로** 표현한다. 헤더에 구현을 두면 링크 관계가
# 코드 독해로만 드러나는데, 그걸 막으려고 선언/정의를 분리했고 그 결과를 여기서
# 고정한다. 깨지면 누군가 경계를 넘은 것이다.
#   unit_tests          : core 만 링크 -> protobuf 심볼이 있으면 안 된다
#   client/blockcopy    : core 를 링크하지 않음 -> Server:: 심볼이 있으면 안 된다
find_program(NM_EXECUTABLE nm)
if(NM_EXECUTABLE)
    foreach(_t raft_unit_tests)
        add_test(NAME isolation_${_t} COMMAND ${CMAKE_COMMAND}
                 -DBIN=$<TARGET_FILE:${_t}> -DNM=${NM_EXECUTABLE}
                 -DPATTERN=protobuf -DWHAT=protobuf
                 -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_absent.cmake)
    endforeach()
    foreach(_t raft_client raft_blockcopy_server raft_blkcopy_scale)
        add_test(NAME isolation_${_t} COMMAND ${CMAKE_COMMAND}
                 -DBIN=$<TARGET_FILE:${_t}> -DNM=${NM_EXECUTABLE}
                 -DPATTERN=nvmeof_raft::Server:: -DWHAT=RaftServer
                 -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_absent.cmake)
    endforeach()
endif()

