#include "chryso_duckdb_ffi.h"

#include "duckdb.hpp"

#include <cassert>
#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

struct DuckDbSession {
  std::unique_ptr<duckdb::DuckDB> db;
  std::unique_ptr<duckdb::Connection> conn;
};

namespace {

thread_local std::string g_last_error;

void set_error(const std::string &message) {
  g_last_error = message;
}

struct JsonValue {
  enum class Type { Null, Bool, Number, String, Array, Object };

  Type type = Type::Null;
  bool boolean = false;
  double number = 0.0;
  std::string string;
  std::vector<JsonValue> array;
  std::unordered_map<std::string, JsonValue> object;
};

class JsonParser {
 public:
  JsonParser(const char *data, size_t len) : start_(data), cur_(data), end_(data + len) {}

  bool Parse(JsonValue &out) {
    SkipWs();
    out = ParseValue();
    SkipWs();
    if (HasError()) {
      return false;
    }
    if (cur_ != end_) {
      SetError("unexpected trailing data");
      return false;
    }
    return true;
  }

  bool HasError() const { return !error_.empty(); }
  const std::string &Error() const { return error_; }

 private:
  JsonValue ParseValue() {
    if (cur_ >= end_) {
      SetError("unexpected end of input");
      return JsonValue{};
    }
    char ch = *cur_;
    if (ch == '{') {
      return ParseObject();
    }
    if (ch == '[') {
      return ParseArray();
    }
    if (ch == '"') {
      JsonValue value;
      value.type = JsonValue::Type::String;
      value.string = ParseString();
      return value;
    }
    if (ch == 't') {
      if (!MatchLiteral("true")) {
        return JsonValue{};
      }
      JsonValue value;
      value.type = JsonValue::Type::Bool;
      value.boolean = true;
      return value;
    }
    if (ch == 'f') {
      if (!MatchLiteral("false")) {
        return JsonValue{};
      }
      JsonValue value;
      value.type = JsonValue::Type::Bool;
      value.boolean = false;
      return value;
    }
    if (ch == 'n') {
      if (!MatchLiteral("null")) {
        return JsonValue{};
      }
      JsonValue value;
      value.type = JsonValue::Type::Null;
      return value;
    }
    if (ch == '-' || std::isdigit(static_cast<unsigned char>(ch))) {
      JsonValue value;
      value.type = JsonValue::Type::Number;
      value.number = ParseNumber();
      return value;
    }
    SetError("unexpected token");
    return JsonValue{};
  }

  JsonValue ParseObject() {
    JsonValue value;
    value.type = JsonValue::Type::Object;
    ExpectChar('{');
    SkipWs();
    if (PeekChar('}')) {
      Advance();
      return value;
    }
    while (cur_ < end_) {
      SkipWs();
      if (!PeekChar('"')) {
        SetError("object key must be string");
        return JsonValue{};
      }
      std::string key = ParseString();
      SkipWs();
      if (!ExpectChar(':')) {
        return JsonValue{};
      }
      SkipWs();
      JsonValue child = ParseValue();
      if (HasError()) {
        return JsonValue{};
      }
      value.object.emplace(std::move(key), std::move(child));
      SkipWs();
      if (PeekChar(',')) {
        Advance();
        continue;
      }
      if (PeekChar('}')) {
        Advance();
        return value;
      }
      SetError("expected ',' or '}'");
      return JsonValue{};
    }
    SetError("unexpected end of object");
    return JsonValue{};
  }

  JsonValue ParseArray() {
    JsonValue value;
    value.type = JsonValue::Type::Array;
    ExpectChar('[');
    SkipWs();
    if (PeekChar(']')) {
      Advance();
      return value;
    }
    while (cur_ < end_) {
      SkipWs();
      JsonValue child = ParseValue();
      if (HasError()) {
        return JsonValue{};
      }
      value.array.emplace_back(std::move(child));
      SkipWs();
      if (PeekChar(',')) {
        Advance();
        continue;
      }
      if (PeekChar(']')) {
        Advance();
        return value;
      }
      SetError("expected ',' or ']'");
      return JsonValue{};
    }
    SetError("unexpected end of array");
    return JsonValue{};
  }

  std::string ParseString() {
    if (!ExpectChar('"')) {
      return "";
    }
    std::string out;
    while (cur_ < end_) {
      char ch = *cur_++;
      if (ch == '"') {
        return out;
      }
      if (ch == '\\') {
        if (cur_ >= end_) {
          SetError("invalid escape sequence");
          return "";
        }
        char esc = *cur_++;
        switch (esc) {
          case '"':
            out.push_back('"');
            break;
          case '\\':
            out.push_back('\\');
            break;
          case '/':
            out.push_back('/');
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
            int code = ParseHex4();
            if (code < 0) {
              return "";
            }
            AppendUtf8(out, static_cast<unsigned int>(code));
            break;
          }
          default:
            SetError("invalid escape sequence");
            return "";
        }
        continue;
      }
      if (static_cast<unsigned char>(ch) < 0x20) {
        SetError("invalid control character");
        return "";
      }
      out.push_back(ch);
    }
    SetError("unterminated string");
    return "";
  }

  double ParseNumber() {
    const char *start = cur_;
    char *endptr = nullptr;
    double value = std::strtod(start, &endptr);
    if (endptr == start) {
      SetError("invalid number");
      return 0.0;
    }
    cur_ = endptr;
    return value;
  }

  bool MatchLiteral(const char *literal) {
    size_t len = std::strlen(literal);
    if (static_cast<size_t>(end_ - cur_) < len) {
      SetError("unexpected end of literal");
      return false;
    }
    if (std::strncmp(cur_, literal, len) != 0) {
      SetError("invalid literal");
      return false;
    }
    cur_ += len;
    return true;
  }

  bool ExpectChar(char expected) {
    if (cur_ >= end_ || *cur_ != expected) {
      std::string message = "expected '";
      message.push_back(expected);
      message.push_back('\'');
      SetError(message);
      return false;
    }
    ++cur_;
    return true;
  }

  bool PeekChar(char expected) const {
    return cur_ < end_ && *cur_ == expected;
  }

  void Advance() {
    if (cur_ < end_) {
      ++cur_;
    }
  }

  void SkipWs() {
    while (cur_ < end_ && std::isspace(static_cast<unsigned char>(*cur_))) {
      ++cur_;
    }
  }

  void SetError(const std::string &message) {
    if (error_.empty()) {
      error_ = message;
    }
  }

  int ParseHex4() {
    int value = 0;
    for (int i = 0; i < 4; ++i) {
      if (cur_ >= end_) {
        SetError("invalid unicode escape");
        return -1;
      }
      char ch = *cur_++;
      value <<= 4;
      if (ch >= '0' && ch <= '9') {
        value += ch - '0';
      } else if (ch >= 'a' && ch <= 'f') {
        value += 10 + (ch - 'a');
      } else if (ch >= 'A' && ch <= 'F') {
        value += 10 + (ch - 'A');
      } else {
        SetError("invalid unicode escape");
        return -1;
      }
    }
    return value;
  }

  void AppendUtf8(std::string &out, unsigned int code) {
    if (code <= 0x7F) {
      out.push_back(static_cast<char>(code));
    } else if (code <= 0x7FF) {
      out.push_back(static_cast<char>(0xC0 | ((code >> 6) & 0x1F)));
      out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
    } else {
      out.push_back(static_cast<char>(0xE0 | ((code >> 12) & 0x0F)));
      out.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
      out.push_back(static_cast<char>(0x80 | (code & 0x3F)));
    }
  }

  const char *start_;
  const char *cur_;
  const char *end_;
  std::string error_;
};

const JsonValue *GetField(const JsonValue &value, const std::string &key) {
  if (value.type != JsonValue::Type::Object) {
    return nullptr;
  }
  auto it = value.object.find(key);
  if (it == value.object.end()) {
    return nullptr;
  }
  return &it->second;
}

bool GetString(const JsonValue &value, std::string &out) {
  if (value.type != JsonValue::Type::String) {
    return false;
  }
  out = value.string;
  return true;
}

bool GetBool(const JsonValue &value, bool &out) {
  if (value.type != JsonValue::Type::Bool) {
    return false;
  }
  out = value.boolean;
  return true;
}

bool GetNumber(const JsonValue &value, double &out) {
  if (value.type != JsonValue::Type::Number) {
    return false;
  }
  out = value.number;
  return true;
}

bool GetArray(const JsonValue &value, const std::vector<JsonValue> *&out) {
  if (value.type != JsonValue::Type::Array) {
    return false;
  }
  out = &value.array;
  return true;
}

struct OrderItem {
  std::string expr;
  bool asc = true;
  std::optional<bool> nulls_first;
};

enum class NodeKind {
  TableScan,
  IndexScan,
  Dml,
  Derived,
  Filter,
  Projection,
  Join,
  Aggregate,
  Distinct,
  TopN,
  Sort,
  Limit,
};

struct PlanNode {
  NodeKind kind;
  std::string table;
  std::string index;
  std::string predicate;
  std::string sql;
  size_t input = 0;
  size_t left = 0;
  size_t right = 0;
  std::string join_type;
  std::string on;
  std::vector<std::string> exprs;
  std::vector<std::string> group_exprs;
  std::vector<std::string> aggr_exprs;
  std::vector<OrderItem> order_by;
  std::optional<uint64_t> limit;
  std::optional<uint64_t> offset;
  std::string alias;
  std::vector<std::string> column_aliases;
};

struct PlanIr {
  std::vector<PlanNode> nodes;
  size_t root = 0;
};

bool ParseOrderItem(const JsonValue &value, OrderItem &out, std::string &err) {
  if (value.type != JsonValue::Type::Object) {
    err = "order item must be object";
    return false;
  }
  auto expr_val = GetField(value, "expr");
  auto asc_val = GetField(value, "asc");
  auto nulls_val = GetField(value, "nulls_first");
  if (!expr_val || !GetString(*expr_val, out.expr)) {
    err = "order item expr is required";
    return false;
  }
  if (!asc_val || !GetBool(*asc_val, out.asc)) {
    err = "order item asc is required";
    return false;
  }
  if (nulls_val) {
    if (nulls_val->type == JsonValue::Type::Null) {
      out.nulls_first.reset();
    } else {
      bool flag = false;
      if (!GetBool(*nulls_val, flag)) {
        err = "order item nulls_first must be bool or null";
        return false;
      }
      out.nulls_first = flag;
    }
  }
  return true;
}

bool ParseNode(const JsonValue &value, PlanNode &out, std::string &err) {
  if (value.type != JsonValue::Type::Object) {
    err = "plan node must be object";
    return false;
  }
  auto type_val = GetField(value, "type");
  std::string type;
  if (!type_val || !GetString(*type_val, type)) {
    err = "plan node missing type";
    return false;
  }
  auto num_field = [&](const char *key, uint64_t &target) -> bool {
    auto field = GetField(value, key);
    double number = 0.0;
    if (!field || !GetNumber(*field, number)) {
      return false;
    }
    if (number < 0 || std::floor(number) != number) {
      return false;
    }
    target = static_cast<uint64_t>(number);
    return true;
  };

  if (type == "table_scan") {
    out.kind = NodeKind::TableScan;
    auto table_val = GetField(value, "table");
    if (!table_val || !GetString(*table_val, out.table)) {
      err = "table_scan missing table";
      return false;
    }
    return true;
  }
  if (type == "index_scan") {
    out.kind = NodeKind::IndexScan;
    auto table_val = GetField(value, "table");
    auto index_val = GetField(value, "index");
    auto pred_val = GetField(value, "predicate");
    if (!table_val || !GetString(*table_val, out.table)) {
      err = "index_scan missing table";
      return false;
    }
    if (!index_val || !GetString(*index_val, out.index)) {
      err = "index_scan missing index";
      return false;
    }
    if (!pred_val || !GetString(*pred_val, out.predicate)) {
      err = "index_scan missing predicate";
      return false;
    }
    return true;
  }
  if (type == "dml") {
    out.kind = NodeKind::Dml;
    auto sql_val = GetField(value, "sql");
    if (!sql_val || !GetString(*sql_val, out.sql)) {
      err = "dml missing sql";
      return false;
    }
    return true;
  }
  if (type == "derived") {
    out.kind = NodeKind::Derived;
    uint64_t input = 0;
    if (!num_field("input", input)) {
      err = "derived missing input";
      return false;
    }
    out.input = static_cast<size_t>(input);
    auto alias_val = GetField(value, "alias");
    if (alias_val && alias_val->type == JsonValue::Type::String) {
      out.alias = alias_val->string;
    }
    auto cols_val = GetField(value, "column_aliases");
    if (cols_val && cols_val->type == JsonValue::Type::Array) {
      for (const auto &col_val : cols_val->array) {
        if (col_val.type != JsonValue::Type::String) {
          err = "derived column_aliases must be strings";
          return false;
        }
        out.column_aliases.push_back(col_val.string);
      }
    }
    return true;
  }
  if (type == "filter") {
    out.kind = NodeKind::Filter;
    uint64_t input = 0;
    if (!num_field("input", input)) {
      err = "filter missing input";
      return false;
    }
    auto pred_val = GetField(value, "predicate");
    if (!pred_val || !GetString(*pred_val, out.predicate)) {
      err = "filter missing predicate";
      return false;
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "projection") {
    out.kind = NodeKind::Projection;
    uint64_t input = 0;
    if (!num_field("input", input)) {
      err = "projection missing input";
      return false;
    }
    const std::vector<JsonValue> *exprs_val = nullptr;
    auto exprs_field = GetField(value, "exprs");
    if (!exprs_field || !GetArray(*exprs_field, exprs_val)) {
      err = "projection missing exprs";
      return false;
    }
    for (const auto &expr_val : *exprs_val) {
      if (expr_val.type != JsonValue::Type::String) {
        err = "projection exprs must be strings";
        return false;
      }
      out.exprs.push_back(expr_val.string);
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "join") {
    out.kind = NodeKind::Join;
    uint64_t left = 0;
    uint64_t right = 0;
    if (!num_field("left", left) || !num_field("right", right)) {
      err = "join missing left/right";
      return false;
    }
    auto join_val = GetField(value, "join_type");
    auto on_val = GetField(value, "on");
    if (!join_val || !GetString(*join_val, out.join_type)) {
      err = "join missing join_type";
      return false;
    }
    if (!on_val || !GetString(*on_val, out.on)) {
      err = "join missing on";
      return false;
    }
    out.left = static_cast<size_t>(left);
    out.right = static_cast<size_t>(right);
    return true;
  }
  if (type == "aggregate") {
    out.kind = NodeKind::Aggregate;
    uint64_t input = 0;
    if (!num_field("input", input)) {
      err = "aggregate missing input";
      return false;
    }
    const std::vector<JsonValue> *groups_val = nullptr;
    const std::vector<JsonValue> *aggr_val = nullptr;
    auto groups_field = GetField(value, "group_exprs");
    auto aggr_field = GetField(value, "aggr_exprs");
    if (!groups_field || !GetArray(*groups_field, groups_val)) {
      err = "aggregate missing group_exprs";
      return false;
    }
    if (!aggr_field || !GetArray(*aggr_field, aggr_val)) {
      err = "aggregate missing aggr_exprs";
      return false;
    }
    for (const auto &expr_val : *groups_val) {
      if (expr_val.type != JsonValue::Type::String) {
        err = "aggregate group_exprs must be strings";
        return false;
      }
      out.group_exprs.push_back(expr_val.string);
    }
    for (const auto &expr_val : *aggr_val) {
      if (expr_val.type != JsonValue::Type::String) {
        err = "aggregate aggr_exprs must be strings";
        return false;
      }
      out.aggr_exprs.push_back(expr_val.string);
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "distinct") {
    out.kind = NodeKind::Distinct;
    uint64_t input = 0;
    if (!num_field("input", input)) {
      err = "distinct missing input";
      return false;
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "top_n") {
    out.kind = NodeKind::TopN;
    uint64_t input = 0;
    if (!num_field("input", input)) {
      err = "top_n missing input";
      return false;
    }
    uint64_t limit = 0;
    if (!num_field("limit", limit)) {
      err = "top_n missing limit";
      return false;
    }
    const std::vector<JsonValue> *order_val = nullptr;
    auto order_field = GetField(value, "order_by");
    if (!order_field || !GetArray(*order_field, order_val)) {
      err = "top_n missing order_by";
      return false;
    }
    for (const auto &item_val : *order_val) {
      OrderItem item;
      if (!ParseOrderItem(item_val, item, err)) {
        return false;
      }
      out.order_by.emplace_back(std::move(item));
    }
    out.input = static_cast<size_t>(input);
    out.limit = limit;
    return true;
  }
  if (type == "sort") {
    out.kind = NodeKind::Sort;
    uint64_t input = 0;
    if (!num_field("input", input)) {
      err = "sort missing input";
      return false;
    }
    const std::vector<JsonValue> *order_val = nullptr;
    auto order_field = GetField(value, "order_by");
    if (!order_field || !GetArray(*order_field, order_val)) {
      err = "sort missing order_by";
      return false;
    }
    for (const auto &item_val : *order_val) {
      OrderItem item;
      if (!ParseOrderItem(item_val, item, err)) {
        return false;
      }
      out.order_by.emplace_back(std::move(item));
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "limit") {
    out.kind = NodeKind::Limit;
    uint64_t input = 0;
    if (!num_field("input", input)) {
      err = "limit missing input";
      return false;
    }
    auto limit_val = GetField(value, "limit");
    if (limit_val && limit_val->type != JsonValue::Type::Null) {
      double number = 0.0;
      if (!GetNumber(*limit_val, number) || number < 0 || std::floor(number) != number) {
        err = "limit must be integer or null";
        return false;
      }
      out.limit = static_cast<uint64_t>(number);
    }
    auto offset_val = GetField(value, "offset");
    if (offset_val && offset_val->type != JsonValue::Type::Null) {
      double number = 0.0;
      if (!GetNumber(*offset_val, number) || number < 0 || std::floor(number) != number) {
        err = "offset must be integer or null";
        return false;
      }
      out.offset = static_cast<uint64_t>(number);
    }
    out.input = static_cast<size_t>(input);
    return true;
  }

  err = "unknown plan node type: " + type;
  return false;
}

bool ParsePlan(const std::string &payload, PlanIr &out, std::string &err) {
  JsonParser parser(payload.data(), payload.size());
  JsonValue root;
  if (!parser.Parse(root)) {
    err = parser.Error();
    return false;
  }
  if (root.type != JsonValue::Type::Object) {
    err = "plan root must be object";
    return false;
  }
  auto nodes_val = GetField(root, "nodes");
  auto root_val = GetField(root, "root");
  const std::vector<JsonValue> *nodes = nullptr;
  if (!nodes_val || !GetArray(*nodes_val, nodes)) {
    err = "plan missing nodes";
    return false;
  }
  double root_num = 0.0;
  if (!root_val || !GetNumber(*root_val, root_num) || root_num < 0 ||
      std::floor(root_num) != root_num) {
    err = "plan root must be integer";
    return false;
  }
  out.root = static_cast<size_t>(root_num);
  out.nodes.clear();
  out.nodes.reserve(nodes->size());
  for (const auto &node_val : *nodes) {
    PlanNode node;
    if (!ParseNode(node_val, node, err)) {
      return false;
    }
    out.nodes.emplace_back(std::move(node));
  }
  if (out.root >= out.nodes.size()) {
    err = "plan root out of range";
    return false;
  }
  return true;
}

std::string EscapeJson(const std::string &value) {
  std::string out;
  out.reserve(value.size() + 8);
  for (char ch : value) {
    switch (ch) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      case '\b':
        out += "\\b";
        break;
      case '\f':
        out += "\\f";
        break;
      case '\n':
        out += "\\n";
        break;
      case '\r':
        out += "\\r";
        break;
      case '\t':
        out += "\\t";
        break;
      default:
        if (static_cast<unsigned char>(ch) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", ch & 0xFF);
          out += buf;
        } else {
          out.push_back(ch);
        }
        break;
    }
  }
  return out;
}

std::string RenderOrderBy(const std::vector<OrderItem> &items) {
  std::string out;
  for (size_t i = 0; i < items.size(); ++i) {
    if (i > 0) {
      out += ", ";
    }
    out += items[i].expr;
    out += items[i].asc ? " ASC" : " DESC";
    if (items[i].nulls_first.has_value()) {
      out += items[i].nulls_first.value() ? " NULLS FIRST" : " NULLS LAST";
    }
  }
  return out;
}

std::string QuoteIdent(const std::string &value) {
  std::string out;
  out.reserve(value.size() + 2);
  out.push_back('"');
  for (char ch : value) {
    if (ch == '"') {
      out.push_back('"');
    }
    out.push_back(ch);
  }
  out.push_back('"');
  return out;
}

std::string JoinExprs(const std::vector<std::string> &exprs) {
  std::string out;
  for (size_t i = 0; i < exprs.size(); ++i) {
    if (i > 0) {
      out += ", ";
    }
    out += exprs[i];
  }
  return out;
}

duckdb::JoinType ParseJoinType(const std::string &value) {
  if (value == "left") {
    return duckdb::JoinType::LEFT;
  }
  if (value == "right") {
    return duckdb::JoinType::RIGHT;
  }
  if (value == "full") {
    return duckdb::JoinType::OUTER;
  }
  return duckdb::JoinType::INNER;
}

duckdb::shared_ptr<duckdb::Relation> BuildRelation(
    const PlanIr &plan,
    size_t index,
    duckdb::Connection &conn,
    std::vector<duckdb::shared_ptr<duckdb::Relation>> &cache,
    std::vector<bool> &built,
    std::string &err) {
  if (index >= plan.nodes.size()) {
    err = "plan index out of range";
    return nullptr;
  }
  if (built[index]) {
    return cache[index];
  }
  const PlanNode &node = plan.nodes[index];
  duckdb::shared_ptr<duckdb::Relation> rel;
  switch (node.kind) {
    case NodeKind::TableScan:
      rel = conn.Table(node.table);
      break;
    case NodeKind::IndexScan: {
      rel = conn.Table(node.table);
      if (!node.predicate.empty()) {
        rel = rel->Filter(node.predicate);
      }
      break;
    }
    case NodeKind::Derived: {
      auto input = BuildRelation(plan, node.input, conn, cache, built, err);
      if (!input) {
        return nullptr;
      }
      rel = input;
      if (!node.column_aliases.empty()) {
        const auto &columns = rel->Columns();
        if (columns.size() != node.column_aliases.size()) {
          err = "derived column_aliases size mismatch";
          return nullptr;
        }
        std::vector<std::string> projections;
        projections.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
          projections.push_back(
              QuoteIdent(columns[i].Name()) + " AS " +
              QuoteIdent(node.column_aliases[i]));
        }
        rel = rel->Project(JoinExprs(projections));
      }
      if (!node.alias.empty()) {
        rel = rel->Alias(node.alias);
      }
      break;
    }
    case NodeKind::Filter: {
      auto input = BuildRelation(plan, node.input, conn, cache, built, err);
      if (!input) {
        return nullptr;
      }
      rel = input->Filter(node.predicate);
      break;
    }
    case NodeKind::Projection: {
      auto input = BuildRelation(plan, node.input, conn, cache, built, err);
      if (!input) {
        return nullptr;
      }
      rel = input->Project(JoinExprs(node.exprs));
      break;
    }
    case NodeKind::Join: {
      auto left = BuildRelation(plan, node.left, conn, cache, built, err);
      if (!left) {
        return nullptr;
      }
      auto right = BuildRelation(plan, node.right, conn, cache, built, err);
      if (!right) {
        return nullptr;
      }
      rel = left->Join(right, node.on, ParseJoinType(node.join_type));
      break;
    }
    case NodeKind::Aggregate: {
      auto input = BuildRelation(plan, node.input, conn, cache, built, err);
      if (!input) {
        return nullptr;
      }
      rel = input->Aggregate(JoinExprs(node.aggr_exprs), JoinExprs(node.group_exprs));
      break;
    }
    case NodeKind::Distinct: {
      auto input = BuildRelation(plan, node.input, conn, cache, built, err);
      if (!input) {
        return nullptr;
      }
      rel = input->Distinct();
      break;
    }
    case NodeKind::TopN: {
      auto input = BuildRelation(plan, node.input, conn, cache, built, err);
      if (!input) {
        return nullptr;
      }
      rel = input->Order(RenderOrderBy(node.order_by))
                ->Limit(static_cast<duckdb::idx_t>(node.limit.value_or(0)));
      break;
    }
    case NodeKind::Sort: {
      auto input = BuildRelation(plan, node.input, conn, cache, built, err);
      if (!input) {
        return nullptr;
      }
      rel = input->Order(RenderOrderBy(node.order_by));
      break;
    }
    case NodeKind::Limit: {
      auto input = BuildRelation(plan, node.input, conn, cache, built, err);
      if (!input) {
        return nullptr;
      }
      duckdb::idx_t limit = std::numeric_limits<duckdb::idx_t>::max();
      duckdb::idx_t offset = 0;
      if (node.limit.has_value()) {
        limit = static_cast<duckdb::idx_t>(node.limit.value());
      }
      if (node.offset.has_value()) {
        offset = static_cast<duckdb::idx_t>(node.offset.value());
      }
      rel = input->Limit(limit, offset);
      break;
    }
    case NodeKind::Dml:
      err = "dml node cannot be used as relation input";
      return nullptr;
  }
  built[index] = true;
  cache[index] = rel;
  return rel;
}

std::string ResultToJson(duckdb::QueryResult &result) {
  std::string out;
  const auto &names = result.names;
  out += "{\"columns\":[";
  for (size_t i = 0; i < names.size(); ++i) {
    if (i > 0) {
      out += ",";
    }
    out += "\"";
    out += EscapeJson(names[i]);
    out += "\"";
  }
  out += "],\"rows\":[";
  bool first_row = true;
  while (true) {
    auto chunk = result.Fetch();
    if (!chunk || chunk->size() == 0) {
      break;
    }
    for (duckdb::idx_t row = 0; row < chunk->size(); ++row) {
      if (!first_row) {
        out += ",";
      }
      first_row = false;
      out += "[";
      for (duckdb::idx_t col = 0; col < static_cast<duckdb::idx_t>(names.size()); ++col) {
        if (col > 0) {
          out += ",";
        }
        auto value = chunk->data[col].GetValue(row);
        out += "\"";
        out += EscapeJson(value.ToString());
        out += "\"";
      }
      out += "]";
    }
  }
  out += "]}";
  return out;
}

bool WriteResult(char **result_out, const std::string &payload) {
  if (!result_out) {
    set_error("result_out is null");
    return false;
  }
  char *buffer = static_cast<char *>(std::malloc(payload.size() + 1));
  if (!buffer) {
    set_error("failed to allocate result buffer");
    return false;
  }
  std::memcpy(buffer, payload.c_str(), payload.size());
  buffer[payload.size()] = '\0';
  *result_out = buffer;
  return true;
}

}  // namespace

extern "C" {

DuckDbSession *chryso_duckdb_session_new() {
  try {
    auto session = std::make_unique<DuckDbSession>();
    session->db = std::make_unique<duckdb::DuckDB>(nullptr);
    session->conn = std::make_unique<duckdb::Connection>(*session->db);
    return session.release();
  } catch (const std::exception &ex) {
    set_error(ex.what());
    return nullptr;
  }
}

void chryso_duckdb_session_free(DuckDbSession *session) {
  delete session;
}

int chryso_duckdb_plan_execute(
    DuckDbSession *session,
    const unsigned char *plan_ptr,
    unsigned long long plan_len,
    char **result_out) {
  if (!session || !session->conn) {
    set_error("duckdb session is null");
    return -1;
  }
  if (!plan_ptr || plan_len == 0) {
    set_error("plan payload is empty");
    return -1;
  }
  std::string payload(reinterpret_cast<const char *>(plan_ptr), plan_len);
  PlanIr plan;
  std::string err;
  if (!ParsePlan(payload, plan, err)) {
    set_error("plan parse failed: " + err);
    return -1;
  }

  const PlanNode &root = plan.nodes[plan.root];
  duckdb::unique_ptr<duckdb::QueryResult> result;
  if (root.kind == NodeKind::Dml) {
    result = session->conn->Query(root.sql);
  } else {
    std::vector<duckdb::shared_ptr<duckdb::Relation>> cache(plan.nodes.size());
    std::vector<bool> built(plan.nodes.size(), false);
    auto rel = BuildRelation(plan, plan.root, *session->conn, cache, built, err);
    if (!rel) {
      set_error("plan build failed: " + err);
      return -1;
    }
    result = rel->Execute();
  }
  if (!result) {
    set_error("duckdb returned null result");
    return -1;
  }
  if (result->HasError()) {
    set_error(result->GetError());
    return -1;
  }
  auto json = ResultToJson(*result);
  if (!WriteResult(result_out, json)) {
    return -1;
  }
  return 0;
}

const char *chryso_duckdb_last_error() {
  return g_last_error.c_str();
}

void chryso_duckdb_string_free(char *value) {
  std::free(value);
}

}  // extern "C"
