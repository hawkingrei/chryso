#include "velox_ffi.h"

#include <cassert>
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

  const std::string invalid_unicode_plan =
      R"({"type":"TableScan","meta":{"table":"wrong"},"table":"bad\u12g3tail"})";

  result = nullptr;
  rc = vx_plan_execute(session, invalid_unicode_plan.c_str(), &result);
  assert(rc == 0);
  assert(result != nullptr);
  assert(std::string(result) == "table\nbad?tail\n");
  vx_string_free(result);

  const std::string truncated_unicode_plan =
      R"({"type":"TableScan","table":"bad\u12)";

  result = nullptr;
  rc = vx_plan_execute(session, truncated_unicode_plan.c_str(), &result);
  assert(rc == 0);
  assert(result != nullptr);
  assert(std::string(result) == "table\nbad?\n");
  vx_string_free(result);

  const std::string surrogate_plan =
      R"({"type":"TableScan","table":"bad\uD800tail"})";

  result = nullptr;
  rc = vx_plan_execute(session, surrogate_plan.c_str(), &result);
  assert(rc == 0);
  assert(result != nullptr);
  assert(std::string(result) == "table\nbad?tail\n");
  vx_string_free(result);

  const std::string trailing_backslash_plan =
      "{\"type\":\"TableScan\",\"table\":\"bad\\";

  result = nullptr;
  rc = vx_plan_execute(session, trailing_backslash_plan.c_str(), &result);
  assert(rc == 0);
  assert(result != nullptr);
  assert(std::string(result) == "table\nbad\n");
  vx_string_free(result);

  const std::string empty_header_plan =
      R"({"type":"TableScan","table":"bad","storage":"memory","memory_payload":"\tname\n1\talice\n"})";

  result = nullptr;
  rc = vx_plan_execute(session, empty_header_plan.c_str(), &result);
  assert(rc == 2);
  assert(result == nullptr);

  vx_session_free(session);
  return 0;
}
