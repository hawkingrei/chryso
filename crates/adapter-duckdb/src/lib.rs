use ::duckdb::params_from_iter;
use ::duckdb::types::Value as DuckValue;
use chryso_adapter::{ExecutorAdapter, ParamValue, QueryResult};
use chryso_core::ast::Statement;
use chryso_core::error::{ChrysoError, ChrysoResult};
use chryso_metadata::{ColumnStats, StatsCache, StatsProvider, TableStats};
use chryso_parser::{ParserConfig, SimpleParser, SqlParser};
use chryso_planner::PhysicalPlan;
use std::cell::RefCell;

pub struct DuckDbAdapter {
    conn: RefCell<::duckdb::Connection>,
}

impl DuckDbAdapter {
    pub fn new() -> Self {
        let conn = crate::duckdb::connect().expect("duckdb connect failed");
        Self {
            conn: RefCell::new(conn),
        }
    }

    pub fn try_new() -> ChrysoResult<Self> {
        let conn = crate::duckdb::connect()?;
        Ok(Self {
            conn: RefCell::new(conn),
        })
    }

    pub fn execute_sql(&self, sql: &str) -> ChrysoResult<()> {
        let conn = self.conn.borrow();
        conn.execute(sql, [])
            .map_err(|err| ChrysoError::new(format!("duckdb execute failed: {err}")))?;
        Ok(())
    }

    pub fn analyze_table(&self, table: &str, cache: &mut StatsCache) -> ChrysoResult<()> {
        self.execute_sql(&format!("analyze {table}"))?;
        let conn = self.conn.borrow();
        let row_count = query_i64(&conn, &format!("select count(*) from {table}"))?;
        cache.insert_table_stats(
            table,
            TableStats {
                row_count: row_count as f64,
            },
        );
        let columns = fetch_columns(&conn, table)?;
        for column in columns {
            let sql = format!(
                "select count(distinct {column}), coalesce(sum(case when {column} is null then 1 else 0 end), 0) from {table}"
            );
            let (distinct_count, nulls) = query_i64_pair(&conn, &sql)?;
            let null_fraction = if row_count == 0 {
                0.0
            } else {
                nulls as f64 / row_count as f64
            };
            cache.insert_column_stats(
                table,
                &column,
                ColumnStats {
                    distinct_count: distinct_count.max(1) as f64,
                    null_fraction,
                },
            );
        }
        Ok(())
    }

    fn execute_with_duckdb(&self, plan: &PhysicalPlan) -> ChrysoResult<QueryResult> {
        let conn = self.conn.borrow();
        if let PhysicalPlan::Dml { sql } = plan {
            if Self::is_query_sql(sql) {
                return Self::query_with_sql(&conn, sql);
            }
            let affected = conn
                .execute(sql, [])
                .map_err(|err| ChrysoError::new(format!("duckdb execute failed: {err}")))?;
            return Ok(QueryResult {
                columns: vec!["rows_affected".to_string()],
                rows: vec![vec![affected.to_string()]],
            });
        }
        let sql = physical_to_sql(plan);
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|err| ChrysoError::new(format!("duckdb prepare failed: {err}")))?;
        let mut rows_iter = stmt
            .query([])
            .map_err(|err| ChrysoError::new(format!("duckdb query failed: {err}")))?;
        let columns = rows_iter
            .as_ref()
            .map(|stmt| stmt.column_names())
            .unwrap_or_default();
        let mut rows = Vec::new();
        while let Some(row) = rows_iter
            .next()
            .map_err(|err| ChrysoError::new(format!("duckdb row error: {err}")))?
        {
            let mut values = Vec::new();
            for idx in 0..columns.len() {
                let value: DuckValue = row
                    .get(idx)
                    .map_err(|err| ChrysoError::new(format!("duckdb value error: {err}")))?;
                values.push(format_duck_value(&value));
            }
            rows.push(values);
        }
        Ok(QueryResult { columns, rows })
    }

    fn execute_with_duckdb_params(
        &self,
        plan: &PhysicalPlan,
        params: &[ParamValue],
    ) -> ChrysoResult<QueryResult> {
        if let PhysicalPlan::Dml { .. } = plan {
            return Err(ChrysoError::new(
                "parameter binding not supported for DML in demo",
            ));
        }
        let sql = physical_to_sql(plan);
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|err| ChrysoError::new(format!("duckdb prepare failed: {err}")))?;
        let duck_params = crate::duckdb::params_to_values(params);
        let mut rows_iter = stmt
            .query(params_from_iter(duck_params.iter()))
            .map_err(|err| ChrysoError::new(format!("duckdb query failed: {err}")))?;
        let columns = rows_iter
            .as_ref()
            .map(|stmt| stmt.column_names())
            .unwrap_or_default();
        let mut rows = Vec::new();
        while let Some(row) = rows_iter
            .next()
            .map_err(|err| ChrysoError::new(format!("duckdb row error: {err}")))?
        {
            let mut values = Vec::new();
            for idx in 0..columns.len() {
                let value: DuckValue = row
                    .get(idx)
                    .map_err(|err| ChrysoError::new(format!("duckdb value error: {err}")))?;
                values.push(format_duck_value(&value));
            }
            rows.push(values);
        }
        Ok(QueryResult { columns, rows })
    }

    fn query_with_sql(conn: &::duckdb::Connection, sql: &str) -> ChrysoResult<QueryResult> {
        let mut stmt = conn
            .prepare(sql)
            .map_err(|err| ChrysoError::new(format!("duckdb prepare failed: {err}")))?;
        let mut rows_iter = stmt
            .query([])
            .map_err(|err| ChrysoError::new(format!("duckdb query failed: {err}")))?;
        let columns = rows_iter
            .as_ref()
            .map(|stmt| stmt.column_names())
            .unwrap_or_default();
        let mut rows = Vec::new();
        while let Some(row) = rows_iter
            .next()
            .map_err(|err| ChrysoError::new(format!("duckdb row error: {err}")))?
        {
            let mut values = Vec::new();
            for idx in 0..columns.len() {
                let value: DuckValue = row
                    .get(idx)
                    .map_err(|err| ChrysoError::new(format!("duckdb value error: {err}")))?;
                values.push(format_duck_value(&value));
            }
            rows.push(values);
        }
        Ok(QueryResult { columns, rows })
    }

    fn is_query_sql(sql: &str) -> bool {
        let parser = SimpleParser::new(ParserConfig::default());
        if let Ok(statement) = parser.parse(sql) {
            return statement_returns_rows(&statement);
        }
        let Some(keyword) = first_keyword(sql) else {
            return false;
        };
        matches!(keyword.as_str(), "select" | "with" | "explain")
    }
}

fn first_keyword(sql: &str) -> Option<String> {
    let mut chars = sql.chars().peekable();
    loop {
        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }
        let first = chars.peek().copied();
        let second = chars.clone().nth(1);
        match (first, second) {
            (Some('-'), Some('-')) => {
                chars.next();
                chars.next();
                while let Some(ch) = chars.next() {
                    if ch == '\n' {
                        break;
                    }
                }
                continue;
            }
            (Some('/'), Some('*')) => {
                chars.next();
                chars.next();
                while let Some(ch) = chars.next() {
                    if ch == '*' && matches!(chars.peek(), Some('/')) {
                        chars.next();
                        break;
                    }
                }
                continue;
            }
            _ => break,
        }
    }
    let mut keyword = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_ascii_alphabetic() || ch == '_' {
            keyword.push(ch.to_ascii_lowercase());
            chars.next();
        } else {
            break;
        }
    }
    if keyword.is_empty() {
        None
    } else {
        Some(keyword)
    }
}

fn statement_returns_rows(statement: &Statement) -> bool {
    match statement {
        Statement::Select(_) | Statement::SetOp { .. } | Statement::Explain(_) => true,
        Statement::With(with) => statement_returns_rows(&with.statement),
        Statement::Insert(insert) => !insert.returning.is_empty(),
        Statement::Update(update) => !update.returning.is_empty(),
        Statement::Delete(delete) => !delete.returning.is_empty(),
        _ => false,
    }
}

impl StatsProvider for DuckDbAdapter {
    fn load_stats(
        &self,
        tables: &[String],
        _columns: &[(String, String)],
        cache: &mut StatsCache,
    ) -> ChrysoResult<()> {
        for table in tables {
            self.analyze_table(table, cache)?;
        }
        Ok(())
    }
}

fn format_duck_value(value: &DuckValue) -> String {
    match value {
        DuckValue::Null => "null".to_string(),
        DuckValue::Boolean(v) => v.to_string(),
        DuckValue::TinyInt(v) => v.to_string(),
        DuckValue::SmallInt(v) => v.to_string(),
        DuckValue::Int(v) => v.to_string(),
        DuckValue::BigInt(v) => v.to_string(),
        DuckValue::HugeInt(v) => v.to_string(),
        DuckValue::UTinyInt(v) => v.to_string(),
        DuckValue::USmallInt(v) => v.to_string(),
        DuckValue::UInt(v) => v.to_string(),
        DuckValue::UBigInt(v) => v.to_string(),
        DuckValue::Float(v) => v.to_string(),
        DuckValue::Double(v) => v.to_string(),
        DuckValue::Decimal(v) => v.to_string(),
        DuckValue::Timestamp(_, v) => v.to_string(),
        DuckValue::Text(v) => v.clone(),
        DuckValue::Blob(v) => format!("{v:?}"),
        DuckValue::Date32(v) => v.to_string(),
        DuckValue::Time64(_, v) => v.to_string(),
        DuckValue::Interval {
            months,
            days,
            nanos,
        } => {
            format!("interval({months},{days},{nanos})")
        }
        DuckValue::List(items) => {
            let rendered = items.iter().map(format_duck_value).collect::<Vec<_>>();
            format!("[{}]", rendered.join(", "))
        }
        DuckValue::Enum(v) => v.clone(),
        DuckValue::Struct(values) => format!("{values:?}"),
        DuckValue::Array(values) => {
            let rendered = values.iter().map(format_duck_value).collect::<Vec<_>>();
            format!("[{}]", rendered.join(", "))
        }
        DuckValue::Map(values) => format!("{values:?}"),
        DuckValue::Union(value) => format_duck_value(value),
    }
}

fn fetch_columns(conn: &::duckdb::Connection, table: &str) -> ChrysoResult<Vec<String>> {
    let mut stmt = conn
        .prepare(&format!("pragma table_info('{table}')"))
        .map_err(|err| ChrysoError::new(format!("duckdb prepare failed: {err}")))?;
    let mut rows = stmt
        .query([])
        .map_err(|err| ChrysoError::new(format!("duckdb query failed: {err}")))?;
    let mut columns = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|err| ChrysoError::new(format!("duckdb row error: {err}")))?
    {
        let name: String = row
            .get(1)
            .map_err(|err| ChrysoError::new(format!("duckdb value error: {err}")))?;
        columns.push(name);
    }
    Ok(columns)
}

fn query_i64(conn: &::duckdb::Connection, sql: &str) -> ChrysoResult<i64> {
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(|err| ChrysoError::new(format!("duckdb query failed: {err}")))
}

fn query_i64_pair(conn: &::duckdb::Connection, sql: &str) -> ChrysoResult<(i64, i64)> {
    conn.query_row(sql, [], |row| {
        let a: i64 = row.get(0)?;
        let b: i64 = row.get(1)?;
        Ok((a, b))
    })
    .map_err(|err| ChrysoError::new(format!("duckdb query failed: {err}")))
}

impl ExecutorAdapter for DuckDbAdapter {
    fn execute(&self, plan: &PhysicalPlan) -> ChrysoResult<QueryResult> {
        self.validate_plan(plan)?;
        self.execute_with_duckdb(plan)
    }

    fn execute_with_params(
        &self,
        plan: &PhysicalPlan,
        params: &[ParamValue],
    ) -> ChrysoResult<QueryResult> {
        self.validate_plan(plan)?;
        self.execute_with_duckdb_params(plan, params)
    }
}

pub fn physical_to_sql(plan: &PhysicalPlan) -> String {
    match plan {
        PhysicalPlan::TableScan { table } => format!("select * from {table}"),
        PhysicalPlan::IndexScan {
            table,
            index: _,
            predicate,
        } => format!("select * from {table} where {}", predicate.to_sql()),
        PhysicalPlan::Dml { sql } => sql.clone(),
        PhysicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => {
            let base = physical_to_sql(input);
            if column_aliases.is_empty() {
                format!("select * from ({base}) as {alias}")
            } else {
                format!(
                    "select * from ({base}) as {alias} ({})",
                    column_aliases.join(", ")
                )
            }
        }
        PhysicalPlan::Filter { predicate, input } => {
            let base = physical_to_sql(input);
            format!("{base} where {}", predicate.to_sql())
        }
        PhysicalPlan::Projection { exprs, input } => {
            let base = physical_to_sql(input);
            let projection = exprs
                .iter()
                .map(|expr| expr.to_sql())
                .collect::<Vec<_>>()
                .join(", ");
            format!("select {projection} from ({base}) as t")
        }
        PhysicalPlan::Join {
            join_type,
            algorithm: _,
            left,
            right,
            on,
        } => {
            let left_sql = physical_to_sql(left);
            let right_sql = physical_to_sql(right);
            let join = match join_type {
                chryso_core::ast::JoinType::Inner => "join",
                chryso_core::ast::JoinType::Left => "left join",
                chryso_core::ast::JoinType::Right => "right join",
                chryso_core::ast::JoinType::Full => "full join",
            };
            format!(
                "select * from ({left_sql}) as l {join} ({right_sql}) as r on {}",
                on.to_sql()
            )
        }
        PhysicalPlan::Aggregate {
            group_exprs,
            aggr_exprs,
            input,
        } => {
            let base = physical_to_sql(input);
            let mut select_list = Vec::new();
            select_list.extend(group_exprs.iter().map(|expr| expr.to_sql()));
            select_list.extend(aggr_exprs.iter().map(|expr| expr.to_sql()));
            let select_list = if select_list.is_empty() {
                "*".to_string()
            } else {
                select_list.join(", ")
            };
            if group_exprs.is_empty() {
                format!("select {select_list} from ({base}) as t")
            } else {
                let group_list = group_exprs
                    .iter()
                    .map(|expr| expr.to_sql())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("select {select_list} from ({base}) as t group by {group_list}")
            }
        }
        PhysicalPlan::Distinct { input } => {
            let base = physical_to_sql(input);
            format!("select distinct * from ({base}) as t")
        }
        PhysicalPlan::TopN {
            order_by,
            limit,
            input,
        } => {
            let base = physical_to_sql(input);
            let order_list = order_by
                .iter()
                .map(|item| {
                    let dir = if item.asc { "asc" } else { "desc" };
                    let mut rendered = format!("{} {dir}", item.expr.to_sql());
                    if let Some(nulls_first) = item.nulls_first {
                        if nulls_first {
                            rendered.push_str(" nulls first");
                        } else {
                            rendered.push_str(" nulls last");
                        }
                    }
                    rendered
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("select * from ({base}) as t order by {order_list} limit {limit}")
        }
        PhysicalPlan::Sort { order_by, input } => {
            let base = physical_to_sql(input);
            let order_list = order_by
                .iter()
                .map(|item| {
                    let dir = if item.asc { "asc" } else { "desc" };
                    let mut rendered = format!("{} {dir}", item.expr.to_sql());
                    if let Some(nulls_first) = item.nulls_first {
                        if nulls_first {
                            rendered.push_str(" nulls first");
                        } else {
                            rendered.push_str(" nulls last");
                        }
                    }
                    rendered
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("select * from ({base}) as t order by {order_list}")
        }
        PhysicalPlan::Limit {
            limit,
            offset,
            input,
        } => {
            let base = physical_to_sql(input);
            match (limit, offset) {
                (Some(limit), Some(offset)) => {
                    format!("select * from ({base}) as t limit {limit} offset {offset}")
                }
                (Some(limit), None) => format!("select * from ({base}) as t limit {limit}"),
                (None, Some(offset)) => format!("select * from ({base}) as t offset {offset}"),
                (None, None) => format!("select * from ({base}) as t"),
            }
        }
    }
}

pub mod duckdb {
    use ::duckdb::Connection;
    use chryso_planner::PhysicalPlan;

    pub fn connect() -> chryso_core::error::ChrysoResult<Connection> {
        Connection::open_in_memory().map_err(|err| {
            chryso_core::error::ChrysoError::new(format!("duckdb open failed: {err}"))
        })
    }

    pub fn physical_to_sql(plan: &PhysicalPlan) -> String {
        crate::physical_to_sql(plan)
    }

    pub fn params_to_values(params: &[super::ParamValue]) -> Vec<duckdb::types::Value> {
        params
            .iter()
            .map(|param| match param {
                super::ParamValue::Int(value) => duckdb::types::Value::BigInt(*value),
                super::ParamValue::Float(value) => duckdb::types::Value::Double(*value),
                super::ParamValue::Bool(value) => duckdb::types::Value::Boolean(*value),
                super::ParamValue::String(value) => duckdb::types::Value::Text(value.clone()),
                super::ParamValue::Null => duckdb::types::Value::Null,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::DuckDbAdapter;
    use chryso_parser::{Dialect, ParserConfig, SimpleParser, SqlParser};
    use chryso_parser_yacc::YaccParser;
    use chryso_planner::PhysicalPlan;

    fn assert_sql_parses(sql: &str) {
        let parser = SimpleParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        parser.parse(sql).expect("simple parse");
        let yacc_parser = YaccParser::new(ParserConfig {
            dialect: Dialect::Postgres,
        });
        yacc_parser.parse(sql).expect("yacc parse");
    }

    #[test]
    fn physical_to_sql_snapshot() {
        let plan = PhysicalPlan::Limit {
            limit: Some(10),
            offset: Some(5),
            input: Box::new(PhysicalPlan::Sort {
                order_by: vec![chryso_core::ast::OrderByExpr {
                    expr: chryso_core::ast::Expr::Identifier("id".to_string()),
                    asc: false,
                    nulls_first: None,
                }],
                input: Box::new(PhysicalPlan::TableScan {
                    table: "users".to_string(),
                }),
            }),
        };
        let sql = super::physical_to_sql(&plan);
        assert!(
            sql.contains("order by id desc"),
            "expected order by in sql: {sql}"
        );
        assert!(
            sql.contains("limit 10 offset 5"),
            "expected limit/offset: {sql}"
        );
    }

    #[test]
    fn physical_to_sql_index_scan() {
        let plan = PhysicalPlan::IndexScan {
            table: "users".to_string(),
            index: "idx_users_id".to_string(),
            predicate: chryso_core::ast::Expr::BinaryOp {
                left: Box::new(chryso_core::ast::Expr::Identifier("id".to_string())),
                op: chryso_core::ast::BinaryOperator::Eq,
                right: Box::new(chryso_core::ast::Expr::Literal(
                    chryso_core::ast::Literal::Number(1.0),
                )),
            },
        };
        let sql = super::physical_to_sql(&plan);
        assert!(sql.contains("from users"));
        assert!(sql.contains("where id = 1"));
    }

    #[test]
    fn physical_sql_is_parseable_by_simple_parser() {
        let plan = PhysicalPlan::Projection {
            exprs: vec![chryso_core::ast::Expr::Identifier("id".to_string())],
            input: Box::new(PhysicalPlan::Filter {
                predicate: chryso_core::ast::Expr::BinaryOp {
                    left: Box::new(chryso_core::ast::Expr::Identifier("region".to_string())),
                    op: chryso_core::ast::BinaryOperator::Eq,
                    right: Box::new(chryso_core::ast::Expr::Literal(
                        chryso_core::ast::Literal::String("us".to_string()),
                    )),
                },
                input: Box::new(PhysicalPlan::TableScan {
                    table: "sales".to_string(),
                }),
            }),
        };
        let sql = super::physical_to_sql(&plan);
        assert_sql_parses(&sql);
    }

    #[test]
    fn physical_sql_join_parseable_by_yacc() {
        let plan = PhysicalPlan::Join {
            join_type: chryso_core::ast::JoinType::Inner,
            algorithm: chryso_planner::JoinAlgorithm::Hash,
            left: Box::new(PhysicalPlan::TableScan {
                table: "users".to_string(),
            }),
            right: Box::new(PhysicalPlan::TableScan {
                table: "orders".to_string(),
            }),
            on: chryso_core::ast::Expr::BinaryOp {
                left: Box::new(chryso_core::ast::Expr::Identifier("l.id".to_string())),
                op: chryso_core::ast::BinaryOperator::Eq,
                right: Box::new(chryso_core::ast::Expr::Identifier("r.user_id".to_string())),
            },
        };
        let sql = super::physical_to_sql(&plan);
        assert_sql_parses(&sql);
    }

    #[test]
    fn physical_sql_aggregate_parseable_by_yacc() {
        let plan = PhysicalPlan::Aggregate {
            group_exprs: vec![chryso_core::ast::Expr::Identifier("id".to_string())],
            aggr_exprs: Vec::new(),
            input: Box::new(PhysicalPlan::TableScan {
                table: "sales".to_string(),
            }),
        };
        let sql = super::physical_to_sql(&plan);
        assert_sql_parses(&sql);
    }

    #[test]
    fn physical_sql_topn_parseable_by_yacc() {
        let plan = PhysicalPlan::TopN {
            order_by: vec![chryso_core::ast::OrderByExpr {
                expr: chryso_core::ast::Expr::Identifier("id".to_string()),
                asc: true,
                nulls_first: Some(false),
            }],
            limit: 5,
            input: Box::new(PhysicalPlan::TableScan {
                table: "users".to_string(),
            }),
        };
        let sql = super::physical_to_sql(&plan);
        assert_sql_parses(&sql);
    }

    #[test]
    fn new_adapter_connects() {
        let _ = DuckDbAdapter::try_new().expect("duckdb adapter");
    }
}
