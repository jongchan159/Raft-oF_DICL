#ifndef RAFT_CLI_H
#define RAFT_CLI_H

#include <string>
#include <vector>

/* ============================================================
 * 세 개의 main(raft_node / raft_client / raft_blockcopy_server)이 공유하는
 * 인자 파싱 유틸.
 *
 * 리팩토링 전에는 다음이 세 파일에 흩어져 있었다:
 *   - next_val 람다: 7줄이 **세 벌 동일 복붙**
 *   - split: 세 벌. 그런데 raft_client_main만 빈 항목을 버려서
 *     **동작이 미묘하게 달랐다**. 여기서는 그 차이를 인자로 드러낸다
 *   - host:port 파싱: 네 벌 (raft_tcp_transport.h의 tcp_dial_http까지 포함)
 *
 * 구현은 net/src/raft_cli.cpp 에 있다. 전부 기동 시 한 번씩만 불리므로 inline 일
 * 이유가 없고, 헤더에 두면 core 를 링크하지 않는 두 바이너리
 * (raft_client / raft_blockcopy_server)까지 <sstream> 을 파싱해야 한다.
 * ============================================================ */

namespace nvmeof_raft {
namespace cli {

/* 구분자로 자른다.
 *   skip_empty=false : 빈 항목도 그대로 남긴다 (raft_node_main의 기존 동작)
 *   skip_empty=true  : 빈 항목을 버린다 (raft_client_main의 기존 동작 --
 *                      "a,b," 같은 입력의 꼬리 빈 항목을 무시하려는 것) */
std::vector<std::string> split(const std::string &s, char sep,
                                       bool skip_empty = false);

/* 구분자로 자르고 각 항목의 앞뒤 공백/탭을 떼며 빈 항목은 버린다.
 * (raft_blockcopy_server_main의 split_csv 동작 -- `-devices "a, b, c"`를
 *  받아들이기 위한 것) */
std::vector<std::string> split_trimmed(const std::string &s, char sep);

/* `-flag VALUE` 형태에서 VALUE를 꺼내고 i를 전진시킨다.
 * 값이 없으면 메시지를 찍고 프로세스를 종료한다 -- 세 main의 기존 동작
 * 그대로다(각자 람다 안에서 std::exit(1)을 불렀다). */
std::string next_arg_value(int argc, char **argv, int &i, const char *flag);

/* "host:port" 를 나눈다. 콜론은 **마지막** 것을 쓴다 (IPv6 리터럴 대비).
 * 콜론이 없으면 port = 0, host = 전체 문자열. */
struct HostPort {
    std::string host;
    int port = 0;
    bool has_port = false;   /* 콜론이 없었으면 false (호출자가 에러로 처리) */
};

HostPort parse_host_port(const std::string &addr);

/* 주소 문자열에서 포트만 (기존 raft_node_main의 port_of 대응).
 * 콜론이 없으면 0 -- 예전 port_of는 colon == npos를 검사하지 않아
 * substr(npos+1)로 전체 문자열을 atoi했고, 결과적으로 0이었다.
 * 즉 동작은 같고 정의만 명시적으로 바뀐다. */
int port_of(const std::string &addr);

} /* namespace cli */
} /* namespace nvmeof_raft */

#endif /* RAFT_CLI_H */
