/* doctest 러너. 테스트 케이스는 test_*.cpp에 있다.
 *
 * 예전에는 여기서 configure_ring(2048)을 한 번 불러야 했다 -- 링 크기가
 * 프로세스 전역 가변 변수였고 "프로세스당 한 번만" 제약이 있어서, 이
 * 바이너리의 모든 테스트가 하나의 링 구성을 공유해야 했다.
 * 그 전역이 Server::ring(RingLog)의 인스턴스 필드로 옮겨졌으므로 이제
 * **각 테스트가 자기 링 구성을 갖는다.** 러너는 아무 설정도 하지 않는다.
 */
#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "third_party/doctest.h"
