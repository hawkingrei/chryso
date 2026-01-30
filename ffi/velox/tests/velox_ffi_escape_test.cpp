#include "velox_ffi.h"

#include <cassert>
#include <cstring>
#include <string>

int main() {
  VxSession* session = vx_session_new();
  assert(session != nullptr);

  const std::string table = "t\"name\\path";
  const std::string plan_json =
      std::string("{\"type\":\"TableScan\",\"table\":\"t\\\"name\\\\path\"}");

  char* result = nullptr;
  int rc = vx_plan_execute(session, plan_json.c_str(), &result);
  assert(rc == 0);
  assert(result != nullptr);

  std::string payload(result);
  std::string expected = std::string("table\n") + table + "\n";
  assert(payload == expected);

  vx_string_free(result);
  vx_session_free(session);
  return 0;
}
