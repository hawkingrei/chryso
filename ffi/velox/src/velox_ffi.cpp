#include "velox_ffi.h"

#include <atomic>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <new>
#include <string>

#ifdef CHRYSO_VELOX_ARROW
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#endif

namespace {

struct Session {
  int reserved = 0;
};

std::string parse_json_string(const char* data, size_t* cursor) {
  std::string out;
  bool escape = false;
  for (; data[*cursor] != '\0'; ++(*cursor)) {
    char ch = data[*cursor];
    if (escape) {
      out.push_back(ch);
      escape = false;
      continue;
    }
    if (ch == '\\') {
      escape = true;
      continue;
    }
    if (ch == '"') {
      ++(*cursor);
      break;
    }
    out.push_back(ch);
  }
  return out;
}

std::string extract_field(const char* json, const char* key) {
  if (json == nullptr || key == nullptr) {
    return {};
  }
  std::string needle = std::string("\"") + key + "\"";
  const char* cursor = json;
  while (*cursor != '\0') {
    const char* found = std::strstr(cursor, needle.c_str());
    if (found == nullptr) {
      return {};
    }
    size_t pos = static_cast<size_t>(found - json);
    pos += needle.size();
    while (json[pos] != '\0' && std::isspace(static_cast<unsigned char>(json[pos]))) {
      ++pos;
    }
    if (json[pos] != ':') {
      cursor = found + needle.size();
      continue;
    }
    ++pos;
    while (json[pos] != '\0' && std::isspace(static_cast<unsigned char>(json[pos]))) {
      ++pos;
    }
    if (json[pos] != '"') {
      cursor = found + needle.size();
      continue;
    }
    ++pos;
    return parse_json_string(json, &pos);
  }
  return {};
}

bool has_type(const char* json, const char* type) {
  if (type == nullptr) {
    return false;
  }
  std::string value = extract_field(json, "type");
  return !value.empty() && value == type;
}

std::mutex g_error_mutex;
std::string g_last_error;

void set_last_error(const std::string& value) {
  std::lock_guard<std::mutex> guard(g_error_mutex);
  g_last_error = value;
}

}  // namespace

extern "C" {

VxSession* vx_session_new() {
  auto* session = new (std::nothrow) Session();
  if (session == nullptr) {
    set_last_error("failed to allocate session");
  }
  return reinterpret_cast<VxSession*>(session);
}

void vx_session_free(VxSession* session) {
  if (session == nullptr) {
    return;
  }
  auto* impl = reinterpret_cast<Session*>(session);
  delete impl;
}

int vx_plan_execute(VxSession* session, const char* plan_json, char** result_out) {
  if (result_out == nullptr) {
    set_last_error("result_out is null");
    return 1;
  }
  *result_out = nullptr;
  if (session == nullptr) {
    set_last_error("session is null");
    return 1;
  }
  if (plan_json == nullptr) {
    set_last_error("plan_json is null");
    return 1;
  }

  if (!has_type(plan_json, "TableScan")) {
    set_last_error("demo supports only TableScan");
    return 2;
  }

  std::string table = extract_field(plan_json, "table");
  if (table.empty()) {
    table = "unknown";
  }
  std::string payload = "table\n" + table + "\n";
  char* buffer = static_cast<char*>(std::malloc(payload.size() + 1));
  if (buffer == nullptr) {
    set_last_error("failed to allocate result buffer");
    return 2;
  }
  std::memcpy(buffer, payload.data(), payload.size());
  buffer[payload.size()] = '\0';
  *result_out = buffer;
  return 0;
}

int vx_plan_execute_arrow(
    VxSession* session,
    const char* plan_json,
    unsigned char** data_out,
    unsigned long long* len_out) {
  if (data_out == nullptr || len_out == nullptr) {
    set_last_error("data_out or len_out is null");
    return 1;
  }
  *data_out = nullptr;
  *len_out = 0;
  if (session == nullptr) {
    set_last_error("session is null");
    return 1;
  }
  if (plan_json == nullptr) {
    set_last_error("plan_json is null");
    return 1;
  }
#ifndef CHRYSO_VELOX_ARROW
  set_last_error("Arrow output disabled at build time");
  return 2;
#else
  if (!has_type(plan_json, "TableScan")) {
    set_last_error("demo supports only TableScan");
    return 2;
  }
  std::string table = extract_field(plan_json, "table");
  if (table.empty()) {
    table = "unknown";
  }

  arrow::StringBuilder builder;
  auto status = builder.Append(table);
  if (!status.ok()) {
    set_last_error(status.ToString());
    return 2;
  }
  std::shared_ptr<arrow::Array> array;
  status = builder.Finish(&array);
  if (!status.ok()) {
    set_last_error(status.ToString());
    return 2;
  }

  auto schema = arrow::schema({arrow::field("table", arrow::utf8(), false)});
  auto batch = arrow::RecordBatch::Make(schema, 1, {array});
  auto sink_result = arrow::io::BufferOutputStream::Create();
  if (!sink_result.ok()) {
    set_last_error(sink_result.status().ToString());
    return 2;
  }
  std::shared_ptr<arrow::io::BufferOutputStream> sink = *sink_result;
  auto writer_result = arrow::ipc::MakeStreamWriter(sink, schema);
  if (!writer_result.ok()) {
    set_last_error(writer_result.status().ToString());
    return 2;
  }
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer = *writer_result;
  status = writer->WriteRecordBatch(*batch);
  if (!status.ok()) {
    set_last_error(status.ToString());
    return 2;
  }
  status = writer->Close();
  if (!status.ok()) {
    set_last_error(status.ToString());
    return 2;
  }
  auto finish_result = sink->Finish();
  if (!finish_result.ok()) {
    set_last_error(finish_result.status().ToString());
    return 2;
  }
  std::shared_ptr<arrow::Buffer> buffer = *finish_result;

  auto size = buffer->size();
  auto* out = static_cast<unsigned char*>(std::malloc(size));
  if (out == nullptr) {
    set_last_error("failed to allocate arrow buffer");
    return 2;
  }
  std::memcpy(out, buffer->data(), size);
  *data_out = out;
  *len_out = static_cast<unsigned long long>(size);
  return 0;
#endif
}

const char* vx_last_error() {
  std::lock_guard<std::mutex> guard(g_error_mutex);
  static thread_local std::string tls_last_error;
  tls_last_error = g_last_error;
  return tls_last_error.c_str();
}

void vx_string_free(char* value) {
  if (value == nullptr) {
    return;
  }
  std::free(value);
}

void vx_bytes_free(unsigned char* value) {
  if (value == nullptr) {
    return;
  }
  std::free(value);
}

}  // extern "C"
