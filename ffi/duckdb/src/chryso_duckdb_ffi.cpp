#include "chryso_duckdb_ffi.h"

#include "duckdb.hpp"
#include "yyjson.hpp"

#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
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

struct JsonDocDeleter {
  void operator()(duckdb_yyjson::yyjson_doc *doc) const {
    if (doc) {
      duckdb_yyjson::yyjson_doc_free(doc);
    }
  }
};

bool ReadString(duckdb_yyjson::yyjson_val *val, std::string &out, std::string &err) {
  if (!val || !duckdb_yyjson::yyjson_is_str(val)) {
    err = "expected string";
    return false;
  }
  const char *ptr = duckdb_yyjson::yyjson_get_str(val);
  if (!ptr) {
    err = "string value is null";
    return false;
  }
  auto len = duckdb_yyjson::yyjson_get_len(val);
  out.assign(ptr, len);
  return true;
}

bool ReadBool(duckdb_yyjson::yyjson_val *val, bool &out, std::string &err) {
  if (!val || !duckdb_yyjson::yyjson_is_bool(val)) {
    err = "expected bool";
    return false;
  }
  out = duckdb_yyjson::yyjson_get_bool(val);
  return true;
}

bool ReadIndex(duckdb_yyjson::yyjson_val *val, uint64_t &out, std::string &err) {
  if (!val || !duckdb_yyjson::yyjson_is_int(val)) {
    err = "expected integer";
    return false;
  }
  if (duckdb_yyjson::yyjson_is_sint(val)) {
    auto value = duckdb_yyjson::yyjson_get_sint(val);
    if (value < 0) {
      err = "integer must be non-negative";
      return false;
    }
    out = static_cast<uint64_t>(value);
    return true;
  }
  out = duckdb_yyjson::yyjson_get_uint(val);
  return true;
}

bool ReadStringArray(duckdb_yyjson::yyjson_val *val, std::vector<std::string> &out, std::string &err) {
  if (!val || !duckdb_yyjson::yyjson_is_arr(val)) {
    err = "expected array";
    return false;
  }
  auto count = duckdb_yyjson::yyjson_arr_size(val);
  out.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    auto item = duckdb_yyjson::yyjson_arr_get(val, i);
    std::string value;
    if (!ReadString(item, value, err)) {
      err = "array element must be string";
      return false;
    }
    out.push_back(std::move(value));
  }
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

bool ParseOrderItem(duckdb_yyjson::yyjson_val *value, OrderItem &out, std::string &err) {
  if (!value || !duckdb_yyjson::yyjson_is_obj(value)) {
    err = "order item must be object";
    return false;
  }
  auto expr_val = duckdb_yyjson::yyjson_obj_get(value, "expr");
  auto asc_val = duckdb_yyjson::yyjson_obj_get(value, "asc");
  auto nulls_val = duckdb_yyjson::yyjson_obj_get(value, "nulls_first");
  if (!ReadString(expr_val, out.expr, err)) {
    err = "order item expr is required";
    return false;
  }
  if (!ReadBool(asc_val, out.asc, err)) {
    err = "order item asc is required";
    return false;
  }
  if (nulls_val) {
    if (duckdb_yyjson::yyjson_is_null(nulls_val)) {
      out.nulls_first.reset();
    } else {
      bool flag = false;
      if (!ReadBool(nulls_val, flag, err)) {
        err = "order item nulls_first must be bool or null";
        return false;
      }
      out.nulls_first = flag;
    }
  }
  return true;
}

bool ParseNode(duckdb_yyjson::yyjson_val *value, PlanNode &out, std::string &err) {
  if (!value || !duckdb_yyjson::yyjson_is_obj(value)) {
    err = "plan node must be object";
    return false;
  }
  auto type_val = duckdb_yyjson::yyjson_obj_get(value, "type");
  std::string type;
  if (!ReadString(type_val, type, err)) {
    err = "plan node missing type";
    return false;
  }

  if (type == "table_scan") {
    out.kind = NodeKind::TableScan;
    auto table_val = duckdb_yyjson::yyjson_obj_get(value, "table");
    if (!ReadString(table_val, out.table, err)) {
      err = "table_scan missing table";
      return false;
    }
    return true;
  }
  if (type == "index_scan") {
    out.kind = NodeKind::IndexScan;
    auto table_val = duckdb_yyjson::yyjson_obj_get(value, "table");
    auto index_val = duckdb_yyjson::yyjson_obj_get(value, "index");
    auto pred_val = duckdb_yyjson::yyjson_obj_get(value, "predicate");
    if (!ReadString(table_val, out.table, err)) {
      err = "index_scan missing table";
      return false;
    }
    if (!ReadString(index_val, out.index, err)) {
      err = "index_scan missing index";
      return false;
    }
    if (!ReadString(pred_val, out.predicate, err)) {
      err = "index_scan missing predicate";
      return false;
    }
    return true;
  }
  if (type == "dml") {
    out.kind = NodeKind::Dml;
    auto sql_val = duckdb_yyjson::yyjson_obj_get(value, "sql");
    if (!ReadString(sql_val, out.sql, err)) {
      err = "dml missing sql";
      return false;
    }
    return true;
  }
  if (type == "derived") {
    out.kind = NodeKind::Derived;
    uint64_t input = 0;
    auto input_val = duckdb_yyjson::yyjson_obj_get(value, "input");
    if (!ReadIndex(input_val, input, err)) {
      err = "derived missing input";
      return false;
    }
    out.input = static_cast<size_t>(input);
    auto alias_val = duckdb_yyjson::yyjson_obj_get(value, "alias");
    if (alias_val && !duckdb_yyjson::yyjson_is_null(alias_val)) {
      if (!ReadString(alias_val, out.alias, err)) {
        err = "derived alias must be string";
        return false;
      }
    }
    auto cols_val = duckdb_yyjson::yyjson_obj_get(value, "column_aliases");
    if (cols_val && !duckdb_yyjson::yyjson_is_null(cols_val)) {
      if (!ReadStringArray(cols_val, out.column_aliases, err)) {
        err = "derived column_aliases must be strings";
        return false;
      }
    }
    return true;
  }
  if (type == "filter") {
    out.kind = NodeKind::Filter;
    uint64_t input = 0;
    auto input_val = duckdb_yyjson::yyjson_obj_get(value, "input");
    if (!ReadIndex(input_val, input, err)) {
      err = "filter missing input";
      return false;
    }
    auto pred_val = duckdb_yyjson::yyjson_obj_get(value, "predicate");
    if (!ReadString(pred_val, out.predicate, err)) {
      err = "filter missing predicate";
      return false;
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "projection") {
    out.kind = NodeKind::Projection;
    uint64_t input = 0;
    auto input_val = duckdb_yyjson::yyjson_obj_get(value, "input");
    if (!ReadIndex(input_val, input, err)) {
      err = "projection missing input";
      return false;
    }
    auto exprs_field = duckdb_yyjson::yyjson_obj_get(value, "exprs");
    if (!ReadStringArray(exprs_field, out.exprs, err)) {
      err = "projection missing exprs";
      return false;
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "join") {
    out.kind = NodeKind::Join;
    uint64_t left = 0;
    uint64_t right = 0;
    auto left_val = duckdb_yyjson::yyjson_obj_get(value, "left");
    auto right_val = duckdb_yyjson::yyjson_obj_get(value, "right");
    if (!ReadIndex(left_val, left, err) || !ReadIndex(right_val, right, err)) {
      err = "join missing left/right";
      return false;
    }
    auto join_val = duckdb_yyjson::yyjson_obj_get(value, "join_type");
    auto on_val = duckdb_yyjson::yyjson_obj_get(value, "on");
    if (!ReadString(join_val, out.join_type, err)) {
      err = "join missing join_type";
      return false;
    }
    if (!ReadString(on_val, out.on, err)) {
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
    auto input_val = duckdb_yyjson::yyjson_obj_get(value, "input");
    if (!ReadIndex(input_val, input, err)) {
      err = "aggregate missing input";
      return false;
    }
    auto groups_field = duckdb_yyjson::yyjson_obj_get(value, "group_exprs");
    auto aggr_field = duckdb_yyjson::yyjson_obj_get(value, "aggr_exprs");
    if (!ReadStringArray(groups_field, out.group_exprs, err)) {
      err = "aggregate missing group_exprs";
      return false;
    }
    if (!ReadStringArray(aggr_field, out.aggr_exprs, err)) {
      err = "aggregate missing aggr_exprs";
      return false;
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "distinct") {
    out.kind = NodeKind::Distinct;
    uint64_t input = 0;
    auto input_val = duckdb_yyjson::yyjson_obj_get(value, "input");
    if (!ReadIndex(input_val, input, err)) {
      err = "distinct missing input";
      return false;
    }
    out.input = static_cast<size_t>(input);
    return true;
  }
  if (type == "top_n") {
    out.kind = NodeKind::TopN;
    uint64_t input = 0;
    auto input_val = duckdb_yyjson::yyjson_obj_get(value, "input");
    if (!ReadIndex(input_val, input, err)) {
      err = "top_n missing input";
      return false;
    }
    uint64_t limit = 0;
    auto limit_val = duckdb_yyjson::yyjson_obj_get(value, "limit");
    if (!ReadIndex(limit_val, limit, err)) {
      err = "top_n missing limit";
      return false;
    }
    auto order_field = duckdb_yyjson::yyjson_obj_get(value, "order_by");
    if (!order_field || !duckdb_yyjson::yyjson_is_arr(order_field)) {
      err = "top_n missing order_by";
      return false;
    }
    auto order_count = duckdb_yyjson::yyjson_arr_size(order_field);
    for (size_t i = 0; i < order_count; ++i) {
      auto item_val = duckdb_yyjson::yyjson_arr_get(order_field, i);
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
    auto input_val = duckdb_yyjson::yyjson_obj_get(value, "input");
    if (!ReadIndex(input_val, input, err)) {
      err = "sort missing input";
      return false;
    }
    auto order_field = duckdb_yyjson::yyjson_obj_get(value, "order_by");
    if (!order_field || !duckdb_yyjson::yyjson_is_arr(order_field)) {
      err = "sort missing order_by";
      return false;
    }
    auto order_count = duckdb_yyjson::yyjson_arr_size(order_field);
    for (size_t i = 0; i < order_count; ++i) {
      auto item_val = duckdb_yyjson::yyjson_arr_get(order_field, i);
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
    auto input_val = duckdb_yyjson::yyjson_obj_get(value, "input");
    if (!ReadIndex(input_val, input, err)) {
      err = "limit missing input";
      return false;
    }
    auto limit_val = duckdb_yyjson::yyjson_obj_get(value, "limit");
    if (limit_val && !duckdb_yyjson::yyjson_is_null(limit_val)) {
      uint64_t limit = 0;
      if (!ReadIndex(limit_val, limit, err)) {
        err = "limit must be integer or null";
        return false;
      }
      out.limit = limit;
    }
    auto offset_val = duckdb_yyjson::yyjson_obj_get(value, "offset");
    if (offset_val && !duckdb_yyjson::yyjson_is_null(offset_val)) {
      uint64_t offset = 0;
      if (!ReadIndex(offset_val, offset, err)) {
        err = "offset must be integer or null";
        return false;
      }
      out.offset = offset;
    }
    out.input = static_cast<size_t>(input);
    return true;
  }

  err = "unknown plan node type: " + type;
  return false;
}

bool ParsePlan(const std::string &payload, PlanIr &out, std::string &err) {
  duckdb_yyjson::yyjson_read_flag flags = duckdb_yyjson::YYJSON_READ_ALLOW_INVALID_UNICODE;
  std::unique_ptr<duckdb_yyjson::yyjson_doc, JsonDocDeleter> doc(
      duckdb_yyjson::yyjson_read(payload.data(), payload.size(), flags));
  if (!doc) {
    err = "json parse failed";
    return false;
  }
  auto root = duckdb_yyjson::yyjson_doc_get_root(doc.get());
  if (!root || !duckdb_yyjson::yyjson_is_obj(root)) {
    err = "plan root must be object";
    return false;
  }
  auto nodes_val = duckdb_yyjson::yyjson_obj_get(root, "nodes");
  auto root_val = duckdb_yyjson::yyjson_obj_get(root, "root");
  if (!nodes_val || !duckdb_yyjson::yyjson_is_arr(nodes_val)) {
    err = "plan missing nodes";
    return false;
  }
  uint64_t root_index = 0;
  if (!ReadIndex(root_val, root_index, err)) {
    err = "plan root must be integer";
    return false;
  }
  out.root = static_cast<size_t>(root_index);
  out.nodes.clear();
  auto node_count = duckdb_yyjson::yyjson_arr_size(nodes_val);
  out.nodes.reserve(node_count);
  for (size_t i = 0; i < node_count; ++i) {
    auto node_val = duckdb_yyjson::yyjson_arr_get(nodes_val, i);
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
  try {
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
  } catch (const std::exception &ex) {
    set_error(ex.what());
    return -1;
  } catch (...) {
    set_error("unknown duckdb exception");
    return -1;
  }
}

char *chryso_duckdb_last_error() {
  if (g_last_error.empty()) {
    return nullptr;
  }
  char *buffer = static_cast<char *>(std::malloc(g_last_error.size() + 1));
  if (!buffer) {
    return nullptr;
  }
  std::memcpy(buffer, g_last_error.c_str(), g_last_error.size() + 1);
  return buffer;
}

void chryso_duckdb_string_free(char *value) {
  std::free(value);
}

}  // extern "C"
