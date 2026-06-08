#include "velox_ffi.h"

#include <cassert>
#include <cstring>
#include <string>

int main() {
  VxSession* session = vx_session_new();
  assert(session != nullptr);

  const std::string escaped_table = "t\"name\\path";
  const std::string escaped_table_plan =
      std::string("{\"type\":\"TableScan\",\"table\":\"t\\\"name\\\\path\"}");

  char* result = nullptr;
  int rc = vx_plan_execute(session, escaped_table_plan.c_str(), &result);
  assert(rc == 0);
  assert(result != nullptr);

  std::string payload(result);
  std::string expected = std::string("table\n") + escaped_table + "\n";
  assert(payload == expected);
  vx_string_free(result);

  const std::string memory_payload_expected = "id\tname\n1\tali\"ce\\path\n";
  const std::string memory_plan =
      R"({"type":"TableScan","table":"users","storage":"memory","memory_payload":"id\tname\n1\tali\"ce\\path\n"})";

  result = nullptr;
  rc = vx_plan_execute(session, memory_plan.c_str(), &result);
  assert(rc == 0);
  assert(result != nullptr);
  assert(std::string(result) == memory_payload_expected);

  vx_string_free(result);
  vx_session_free(session);
  return 0;
}
