#include "velox_ffi.h"

#include <atomic>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <new>
#include <string>
#include <utility>
#include <vector>

#ifdef CHRYSO_VELOX_ARROW
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#endif

namespace {

struct Session {
  int reserved = 0;
};

std::string extract_field(const char* json, const char* key);

bool parse_hex4(const char* data, size_t cursor, uint32_t* codepoint) {
  uint32_t value = 0;
  for (size_t i = 0; i < 4; ++i) {
    char ch = data[cursor + i];
    if (ch == '\0') {
      return false;
    }
    value <<= 4;
    if (ch >= '0' && ch <= '9') {
      value += static_cast<uint32_t>(ch - '0');
      continue;
    }
    if (ch >= 'a' && ch <= 'f') {
      value += static_cast<uint32_t>(ch - 'a' + 10);
      continue;
    }
    if (ch >= 'A' && ch <= 'F') {
      value += static_cast<uint32_t>(ch - 'A' + 10);
      continue;
    }
    return false;
  }
  *codepoint = value;
  return true;
}

void append_utf8(uint32_t codepoint, std::string* out) {
  if (codepoint <= 0x7F) {
    out->push_back(static_cast<char>(codepoint));
    return;
  }
  if (codepoint <= 0x7FF) {
    out->push_back(static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F)));
    out->push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    return;
  }
  if (codepoint <= 0xFFFF) {
    out->push_back(static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F)));
    out->push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
    out->push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    return;
  }
  if (codepoint <= 0x10FFFF) {
    out->push_back(static_cast<char>(0xF0 | ((codepoint >> 18) & 0x07)));
    out->push_back(static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
    out->push_back(static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
    out->push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
    return;
  }
  out->push_back('?');
}

std::string parse_json_string(const char* data, size_t* cursor) {
  std::string out;
  size_t pos = *cursor;
  while (data[pos] != '\0') {
    char ch = data[pos++];
    if (ch == '"') {
      break;
    }
    if (ch != '\\') {
      out.push_back(ch);
      continue;
    }
    char escaped = data[pos++];
    if (escaped == '\0') {
      break;
    }
    switch (escaped) {
      case '"':
      case '\\':
      case '/':
        out.push_back(escaped);
        break;
      case 'b':
        out.push_back('\b');
        break;
      case 'f':
        out.push_back('\f');
        break;
      case 'n':
        out.push_back('\n');
        break;
      case 'r':
        out.push_back('\r');
        break;
      case 't':
        out.push_back('\t');
        break;
      case 'u': {
        uint32_t codepoint = 0;
        if (parse_hex4(data, pos, &codepoint)) {
          append_utf8(codepoint, &out);
          pos += 4;
        } else {
          out.push_back('?');
          for (size_t i = 0; i < 4 && data[pos] != '\0'; ++i) {
            ++pos;
          }
        }
        break;
      }
      default:
        out.push_back(escaped);
        break;
    }
  }
  *cursor = pos;
  return out;
}

void split_tsv_line(const std::string& line, std::vector<std::string>* fields) {
  size_t cursor = 0;
  while (cursor <= line.size()) {
    size_t next = line.find('\t', cursor);
    if (next == std::string::npos) {
      fields->push_back(line.substr(cursor));
      break;
    }
    fields->push_back(line.substr(cursor, next - cursor));
    cursor = next + 1;
  }
}

bool parse_tsv_payload(
    const std::string& payload,
    std::vector<std::string>* columns,
    std::vector<std::vector<std::string>>* rows,
    std::string* error) {
  if (payload.empty()) {
    *error = "empty memory payload";
    return false;
  }
  size_t header_end = payload.find('\n');
  std::string header =
      header_end == std::string::npos ? payload : payload.substr(0, header_end);
  split_tsv_line(header, columns);
  if (columns->empty()) {
    *error = "memory payload header is empty";
    return false;
  }

  size_t line_start = header_end == std::string::npos ? payload.size() : header_end + 1;
  while (line_start < payload.size()) {
    size_t line_end = payload.find('\n', line_start);
    std::string line = line_end == std::string::npos
        ? payload.substr(line_start)
        : payload.substr(line_start, line_end - line_start);
    std::vector<std::string> row;
    split_tsv_line(line, &row);
    if (row.size() != columns->size()) {
      *error = "memory payload row width mismatch";
      return false;
    }
    rows->push_back(std::move(row));
    if (line_end == std::string::npos) {
      break;
    }
    line_start = line_end + 1;
  }
  return true;
}

bool resolve_table_scan_payload(
    const char* plan_json,
    std::string* payload,
    std::string* error) {
  std::string storage = extract_field(plan_json, "storage");
  if (!storage.empty() && storage != "memory") {
    *error = "unsupported storage for demo: " + storage;
    return false;
  }
  std::string memory_payload = extract_field(plan_json, "memory_payload");
  if (!memory_payload.empty()) {
    *payload = std::move(memory_payload);
    return true;
  }
  std::string table = extract_field(plan_json, "table");
  if (table.empty()) {
    table = "unknown";
  }
  *payload = "table\n" + table + "\n";
  return true;
}

std::string extract_field(const char* json, const char* key) {
  if (json == nullptr || key == nullptr) {
    return {};
  }
  size_t pos = 0;
  int depth = 0;
  while (json[pos] != '\0') {
    if (json[pos] == '{') {
      ++depth;
      ++pos;
      continue;
    }
    if (json[pos] == '}') {
      if (depth > 0) {
        --depth;
      }
      ++pos;
      continue;
    }
    if (json[pos] != '"') {
      ++pos;
      continue;
    }

    ++pos;
    std::string candidate = parse_json_string(json, &pos);
    if (depth != 1) {
      continue;
    }
    while (json[pos] != '\0' && std::isspace(static_cast<unsigned char>(json[pos]))) {
      ++pos;
    }
    if (json[pos] != ':') {
      continue;
    }
    ++pos;
    while (json[pos] != '\0' && std::isspace(static_cast<unsigned char>(json[pos]))) {
      ++pos;
    }
    if (candidate != key) {
      if (json[pos] == '"') {
        ++pos;
        (void)parse_json_string(json, &pos);
      }
      continue;
    }
    if (json[pos] != '"') {
      return {};
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

  std::string payload;
  std::string error;
  if (!resolve_table_scan_payload(plan_json, &payload, &error)) {
    set_last_error(error);
    return 2;
  }

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

  std::string payload;
  std::string error;
  if (!resolve_table_scan_payload(plan_json, &payload, &error)) {
    set_last_error(error);
    return 2;
  }

  std::vector<std::string> columns;
  std::vector<std::vector<std::string>> rows;
  if (!parse_tsv_payload(payload, &columns, &rows, &error)) {
    set_last_error(error);
    return 2;
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  fields.reserve(columns.size());
  arrays.reserve(columns.size());
  for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
    fields.push_back(arrow::field(columns[col_idx], arrow::utf8(), false));
    arrow::StringBuilder builder;
    for (const auto& row : rows) {
      auto status = builder.Append(row[col_idx]);
      if (!status.ok()) {
        set_last_error(status.ToString());
        return 2;
      }
    }
    std::shared_ptr<arrow::Array> array;
    auto status = builder.Finish(&array);
    if (!status.ok()) {
      set_last_error(status.ToString());
      return 2;
    }
    arrays.push_back(std::move(array));
  }

  auto schema = arrow::schema(fields);
  auto batch =
      arrow::RecordBatch::Make(schema, static_cast<int64_t>(rows.size()), arrays);
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
  auto status = writer->WriteRecordBatch(*batch);
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
