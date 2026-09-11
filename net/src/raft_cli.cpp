#include "raft_cli.h"

#include <cstdio>
#include <cstdlib>
#include <sstream>

/* net/include/raft_cli.h 의 구현. 선언은 그 헤더를 볼 것.
 * 세 main(raft_node / raft_client / raft_blockcopy_server)이 전부 링크한다. */

namespace nvmeof_raft {
namespace cli {

std::vector<std::string> split(const std::string &s, char sep,
                                bool skip_empty) {   /* 기본값은 헤더에만 */
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, sep)) {
        if (skip_empty && item.empty()) {
            continue;
        }
        out.push_back(item);
    }
    return out;
}

std::vector<std::string> split_trimmed(const std::string &s, char sep) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, sep)) {
        size_t b = item.find_first_not_of(" \t");
        size_t e = item.find_last_not_of(" \t");
        if (b == std::string::npos) {
            continue;
        }
        out.push_back(item.substr(b, e - b + 1));
    }
    return out;
}

std::string next_arg_value(int argc, char **argv, int &i, const char *flag) {
    if (i + 1 >= argc) {
        std::fprintf(stderr, "%s requires a value\n", flag);
        std::exit(1);
    }
    return argv[++i];
}

HostPort parse_host_port(const std::string &addr) {
    HostPort hp;
    size_t colon = addr.rfind(':');
    if (colon == std::string::npos) {
        hp.host = addr;
        return hp;
    }
    hp.host = addr.substr(0, colon);
    hp.port = std::atoi(addr.substr(colon + 1).c_str());
    hp.has_port = true;
    return hp;
}

int port_of(const std::string &addr) {
    return parse_host_port(addr).port;
}

} /* namespace cli */
} /* namespace nvmeof_raft */
