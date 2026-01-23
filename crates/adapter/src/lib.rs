use corundum_core::error::{CorundumError, CorundumResult};
#[cfg(feature = "duckdb")]
use corundum_core::ast::Statement;
#[cfg(feature = "duckdb")]
use corundum_metadata::{ColumnStats, StatsCache, StatsProvider, TableStats};
#[cfg(feature = "duckdb")]
use ::duckdb::params_from_iter;
#[cfg(feature = "duckdb")]
use ::duckdb::types::Value as DuckValue;
#[cfg(feature = "duckdb")]
use corundum_parser::{ParserConfig, SimpleParser, SqlParser};
use corundum_planner::PhysicalPlan;
use std::cell::RefCell;

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone)]
pub enum ParamValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Null,
}

#[derive(Debug, Clone, Default)]
pub struct AdapterCapabilities {
    pub joins: bool,
    pub aggregates: bool,
    pub distinct: bool,
    pub topn: bool,
    pub sort: bool,
    pub limit: bool,
    pub offset: bool,
}

impl AdapterCapabilities {
    pub fn all() -> Self {
        Self {
            joins: true,
            aggregates: true,
            distinct: true,
            topn: true,
            sort: true,
            limit: true,
            offset: true,
        }
    }

    pub fn supports_plan(&self, plan: &PhysicalPlan) -> bool {
        match plan {
            PhysicalPlan::TableScan { .. } => true,
            PhysicalPlan::IndexScan { .. } => true,
            PhysicalPlan::Dml { .. } => true,
            PhysicalPlan::Derived { input, .. } => self.supports_plan(input),
            PhysicalPlan::Filter { input, .. } => self.supports_plan(input),
            PhysicalPlan::Projection { input, .. } => self.supports_plan(input),
            PhysicalPlan::Join { left, right, .. } => {
                self.joins && self.supports_plan(left) && self.supports_plan(right)
            }
            PhysicalPlan::Aggregate { input, .. } => self.aggregates && self.supports_plan(input),
            PhysicalPlan::Distinct { input } => self.distinct && self.supports_plan(input),
            PhysicalPlan::TopN { input, .. } => self.topn && self.supports_plan(input),
            PhysicalPlan::Sort { input, .. } => self.sort && self.supports_plan(input),
            PhysicalPlan::Limit { limit, offset, input } => {
                let offset_ok = if offset.is_some() { self.offset } else { true };
                let limit_ok = if limit.is_some() { self.limit } else { true };
                offset_ok && limit_ok && self.supports_plan(input)
            }
        }
    }
}

pub trait ExecutorAdapter {
    fn execute(&self, plan: &PhysicalPlan) -> CorundumResult<QueryResult>;

    fn execute_with_params(
        &self,
        plan: &PhysicalPlan,
        params: &[ParamValue],
    ) -> CorundumResult<QueryResult> {
        if params.is_empty() {
            self.execute(plan)
        } else {
            Err(CorundumError::new(
                "adapter does not support parameter binding",
            ))
        }
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::all()
    }

    fn validate_plan(&self, plan: &PhysicalPlan) -> CorundumResult<()> {
        if self.capabilities().supports_plan(plan) {
            Ok(())
        } else {
            Err(CorundumError::new(
                "adapter does not support required plan operators",
            ))
        }
    }
}

#[derive(Debug, Clone)]
pub struct MockAdapter {
    result: QueryResult,
    last_plan: RefCell<Option<String>>,
    capabilities: AdapterCapabilities,
}

impl MockAdapter {
    pub fn new() -> Self {
        Self {
            result: QueryResult {
                columns: vec!["demo".to_string()],
                rows: vec![vec!["ok".to_string()]],
            },
            last_plan: RefCell::new(None),
            capabilities: AdapterCapabilities::all(),
        }
    }

    pub fn with_result(result: QueryResult) -> Self {
        Self {
            result,
            last_plan: RefCell::new(None),
            capabilities: AdapterCapabilities::all(),
        }
    }

    pub fn with_capabilities(mut self, capabilities: AdapterCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    pub fn last_plan(&self) -> Option<String> {
        self.last_plan.borrow().clone()
    }
}

impl Default for MockAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutorAdapter for MockAdapter {
    fn execute(&self, plan: &PhysicalPlan) -> CorundumResult<QueryResult> {
        self.validate_plan(plan)?;
        *self.last_plan.borrow_mut() = Some(plan.explain(0));
        Ok(self.result.clone())
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.capabilities.clone()
    }
}

pub struct DuckDbAdapter {
    #[cfg(feature = "duckdb")]
    conn: RefCell<::duckdb::Connection>,
}

impl DuckDbAdapter {
    pub fn new() -> Self {
        #[cfg(feature = "duckdb")]
        {
            let conn = crate::duckdb::connect().expect("duckdb connect failed");
            Self {
                conn: RefCell::new(conn),
            }
        }
        #[cfg(not(feature = "duckdb"))]
        {
            Self {}
        }
    }

    pub fn try_new() -> CorundumResult<Self> {
        #[cfg(feature = "duckdb")]
        {
            let conn = crate::duckdb::connect()?;
            Ok(Self {
                conn: RefCell::new(conn),
            })
        }
        #[cfg(not(feature = "duckdb"))]
        {
            Err(CorundumError::new(
                "duckdb feature is disabled; enable with --features duckdb",
            ))
        }
    }

    #[cfg(feature = "duckdb")]
    pub fn execute_sql(&self, sql: &str) -> CorundumResult<()> {
        let conn = self.conn.borrow();
        conn.execute(sql, [])
            .map_err(|err| CorundumError::new(format!("duckdb execute failed: {err}")))?;
        Ok(())
    }

    #[cfg(feature = "duckdb")]
    pub fn analyze_table(&self, table: &str, cache: &mut StatsCache) -> CorundumResult<()> {
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

    #[cfg(feature = "duckdb")]
    fn execute_with_duckdb(&self, plan: &PhysicalPlan) -> CorundumResult<QueryResult> {
        let conn = self.conn.borrow();
        if let PhysicalPlan::Dml { sql } = plan {
            if Self::is_query_sql(sql) {
                return Self::query_with_sql(&conn, sql);
            }
            let affected = conn
                .execute(sql, [])
                .map_err(|err| CorundumError::new(format!("duckdb execute failed: {err}")))?;
            return Ok(QueryResult {
                columns: vec!["rows_affected".to_string()],
                rows: vec![vec![affected.to_string()]],
            });
        }
        let sql = crate::physical_to_sql(plan);
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|err| CorundumError::new(format!("duckdb prepare failed: {err}")))?;
        let mut rows_iter = stmt
            .query([])
            .map_err(|err| CorundumError::new(format!("duckdb query failed: {err}")))?;
        let columns = rows_iter
            .as_ref()
            .map(|stmt| stmt.column_names())
            .unwrap_or_default();
        let mut rows = Vec::new();
        while let Some(row) = rows_iter
            .next()
            .map_err(|err| CorundumError::new(format!("duckdb row error: {err}")))? {
            let mut values = Vec::new();
            for idx in 0..columns.len() {
                let value: DuckValue = row
                    .get(idx)
                    .map_err(|err| CorundumError::new(format!("duckdb value error: {err}")))?;
                values.push(format_duck_value(&value));
            }
            rows.push(values);
        }
        Ok(QueryResult { columns, rows })
    }

    #[cfg(feature = "duckdb")]
    fn execute_with_duckdb_params(
        &self,
        plan: &PhysicalPlan,
        params: &[ParamValue],
    ) -> CorundumResult<QueryResult> {
        if let PhysicalPlan::Dml { .. } = plan {
            return Err(CorundumError::new(
                "parameter binding not supported for DML in demo",
            ));
        }
        let sql = crate::physical_to_sql(plan);
        let conn = self.conn.borrow();
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|err| CorundumError::new(format!("duckdb prepare failed: {err}")))?;
        let duck_params = crate::duckdb::params_to_values(params);
        let mut rows_iter = stmt
            .query(params_from_iter(duck_params.iter()))
            .map_err(|err| CorundumError::new(format!("duckdb query failed: {err}")))?;
        let columns = rows_iter
            .as_ref()
            .map(|stmt| stmt.column_names())
            .unwrap_or_default();
        let mut rows = Vec::new();
        while let Some(row) = rows_iter
            .next()
            .map_err(|err| CorundumError::new(format!("duckdb row error: {err}")))? {
            let mut values = Vec::new();
            for idx in 0..columns.len() {
                let value: DuckValue = row
                    .get(idx)
                    .map_err(|err| CorundumError::new(format!("duckdb value error: {err}")))?;
                values.push(format_duck_value(&value));
            }
            rows.push(values);
        }
        Ok(QueryResult { columns, rows })
    }

    #[cfg(feature = "duckdb")]
    fn query_with_sql(
        conn: &::duckdb::Connection,
        sql: &str,
    ) -> CorundumResult<QueryResult> {
        let mut stmt = conn
            .prepare(sql)
            .map_err(|err| CorundumError::new(format!("duckdb prepare failed: {err}")))?;
        let mut rows_iter = stmt
            .query([])
            .map_err(|err| CorundumError::new(format!("duckdb query failed: {err}")))?;
        let columns = rows_iter
            .as_ref()
            .map(|stmt| stmt.column_names())
            .unwrap_or_default();
        let mut rows = Vec::new();
        while let Some(row) = rows_iter
            .next()
            .map_err(|err| CorundumError::new(format!("duckdb row error: {err}")))? {
            let mut values = Vec::new();
            for idx in 0..columns.len() {
                let value: DuckValue = row
                    .get(idx)
                    .map_err(|err| CorundumError::new(format!("duckdb value error: {err}")))?;
                values.push(format_duck_value(&value));
            }
            rows.push(values);
        }
        Ok(QueryResult { columns, rows })
    }

    #[cfg(feature = "duckdb")]
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

#[cfg(feature = "duckdb")]
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

#[cfg(feature = "duckdb")]
fn statement_returns_rows(statement: &Statement) -> bool {
    match statement {
        Statement::Select(_)
        | Statement::SetOp { .. }
        | Statement::Explain(_) => true,
        Statement::With(with) => statement_returns_rows(&with.statement),
        Statement::Insert(insert) => !insert.returning.is_empty(),
        Statement::Update(update) => !update.returning.is_empty(),
        Statement::Delete(delete) => !delete.returning.is_empty(),
        _ => false,
    }
}

#[cfg(feature = "duckdb")]
impl StatsProvider for DuckDbAdapter {
    fn load_stats(
        &self,
        tables: &[String],
        _columns: &[(String, String)],
        cache: &mut StatsCache,
    ) -> CorundumResult<()> {
        for table in tables {
            self.analyze_table(table, cache)?;
        }
        Ok(())
    }
}

#[cfg(feature = "duckdb")]
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
        DuckValue::Interval { months, days, nanos } => {
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

#[cfg(feature = "duckdb")]
fn fetch_columns(conn: &::duckdb::Connection, table: &str) -> CorundumResult<Vec<String>> {
    let mut stmt = conn
        .prepare(&format!("pragma table_info('{table}')"))
        .map_err(|err| CorundumError::new(format!("duckdb prepare failed: {err}")))?;
    let mut rows = stmt
        .query([])
        .map_err(|err| CorundumError::new(format!("duckdb query failed: {err}")))?;
    let mut columns = Vec::new();
    while let Some(row) = rows
        .next()
        .map_err(|err| CorundumError::new(format!("duckdb row error: {err}")))? {
        let name: String = row
            .get(1)
            .map_err(|err| CorundumError::new(format!("duckdb value error: {err}")))?;
        columns.push(name);
    }
    Ok(columns)
}

#[cfg(feature = "duckdb")]
fn query_i64(conn: &::duckdb::Connection, sql: &str) -> CorundumResult<i64> {
    conn.query_row(sql, [], |row| row.get(0))
        .map_err(|err| CorundumError::new(format!("duckdb query failed: {err}")))
}

#[cfg(feature = "duckdb")]
fn query_i64_pair(conn: &::duckdb::Connection, sql: &str) -> CorundumResult<(i64, i64)> {
    conn.query_row(sql, [], |row| {
        let a: i64 = row.get(0)?;
        let b: i64 = row.get(1)?;
        Ok((a, b))
    })
    .map_err(|err| CorundumError::new(format!("duckdb query failed: {err}")))
}

impl ExecutorAdapter for DuckDbAdapter {
    fn execute(&self, plan: &PhysicalPlan) -> CorundumResult<QueryResult> {
        self.validate_plan(plan)?;
        #[cfg(feature = "duckdb")]
        {
            self.execute_with_duckdb(plan)
        }
        #[cfg(not(feature = "duckdb"))]
        {
            let _ = plan;
            Err(CorundumError::new(
                "duckdb feature is disabled; enable with --features duckdb",
            ))
        }
    }

    fn execute_with_params(
        &self,
        plan: &PhysicalPlan,
        params: &[ParamValue],
    ) -> CorundumResult<QueryResult> {
        self.validate_plan(plan)?;
        #[cfg(feature = "duckdb")]
        {
            self.execute_with_duckdb_params(plan, params)
        }
        #[cfg(not(feature = "duckdb"))]
        {
            let _ = (plan, params);
            Err(CorundumError::new(
                "duckdb feature is disabled; enable with --features duckdb",
            ))
        }
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
                corundum_core::ast::JoinType::Inner => "join",
                corundum_core::ast::JoinType::Left => "left join",
                corundum_core::ast::JoinType::Right => "right join",
                corundum_core::ast::JoinType::Full => "full join",
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

#[cfg(feature = "duckdb")]
pub mod duckdb {
    use corundum_planner::PhysicalPlan;
    use duckdb::Connection;

    pub fn connect() -> corundum_core::error::CorundumResult<Connection> {
        Connection::open_in_memory()
            .map_err(|err| corundum_core::error::CorundumError::new(format!("duckdb open failed: {err}")))
    }

    pub fn physical_to_sql(plan: &PhysicalPlan) -> String {
        crate::physical_to_sql(plan)
    }

    pub fn params_to_values(params: &[crate::ParamValue]) -> Vec<duckdb::types::Value> {
        params
            .iter()
            .map(|param| match param {
                crate::ParamValue::Int(value) => duckdb::types::Value::BigInt(*value),
                crate::ParamValue::Float(value) => {
                    duckdb::types::Value::Double(*value)
                }
                crate::ParamValue::Bool(value) => duckdb::types::Value::Boolean(*value),
                crate::ParamValue::String(value) => {
                    duckdb::types::Value::Text(value.clone())
                }
                crate::ParamValue::Null => duckdb::types::Value::Null,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{AdapterCapabilities, ExecutorAdapter, MockAdapter, ParamValue, QueryResult};
    use corundum_planner::PhysicalPlan;

    #[test]
    fn mock_adapter_records_plan() {
        let adapter = MockAdapter::new();
        let plan = PhysicalPlan::TableScan {
            table: "users".to_string(),
        };
        adapter.execute(&plan).expect("execute");
        let recorded = adapter.last_plan().expect("recorded");
        assert!(recorded.contains("TableScan"));
    }

    #[test]
    fn capabilities_reject_join() {
        let plan = PhysicalPlan::Join {
            join_type: corundum_core::ast::JoinType::Inner,
            algorithm: corundum_planner::JoinAlgorithm::Hash,
            left: Box::new(PhysicalPlan::TableScan {
                table: "t1".to_string(),
            }),
            right: Box::new(PhysicalPlan::TableScan {
                table: "t2".to_string(),
            }),
            on: corundum_core::ast::Expr::Identifier("t1.id = t2.id".to_string()),
        };
        let caps = AdapterCapabilities {
            joins: false,
            ..AdapterCapabilities::all()
        };
        let adapter = MockAdapter::new().with_capabilities(caps);
        let err = adapter.execute(&plan).expect_err("should reject join");
        assert!(err.to_string().contains("does not support"));
    }

    #[test]
    fn execute_with_params_rejects_on_mock() {
        let adapter = MockAdapter::new();
        let plan = PhysicalPlan::TableScan {
            table: "users".to_string(),
        };
        let err = adapter
            .execute_with_params(&plan, &[ParamValue::Int(1)])
            .expect_err("should reject params");
        assert!(err.to_string().contains("parameter"));
    }

    #[test]
    fn mock_adapter_custom_result() {
        let adapter = MockAdapter::with_result(QueryResult {
            columns: vec!["id".to_string()],
            rows: vec![vec!["1".to_string()]],
        });
        let plan = PhysicalPlan::TableScan {
            table: "users".to_string(),
        };
        let result = adapter.execute(&plan).expect("execute");
        assert_eq!(result.columns, vec!["id".to_string()]);
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn physical_to_sql_snapshot() {
        let plan = PhysicalPlan::Limit {
            limit: Some(10),
            offset: Some(5),
            input: Box::new(PhysicalPlan::Sort {
                order_by: vec![corundum_core::ast::OrderByExpr {
                    expr: corundum_core::ast::Expr::Identifier("id".to_string()),
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
        assert!(sql.contains("limit 10 offset 5"), "expected limit/offset: {sql}");
    }

    #[test]
    fn physical_to_sql_index_scan() {
        let plan = PhysicalPlan::IndexScan {
            table: "users".to_string(),
            index: "idx_users_id".to_string(),
            predicate: corundum_core::ast::Expr::BinaryOp {
                left: Box::new(corundum_core::ast::Expr::Identifier("id".to_string())),
                op: corundum_core::ast::BinaryOperator::Eq,
                right: Box::new(corundum_core::ast::Expr::Literal(corundum_core::ast::Literal::Number(1.0))),
            },
        };
        let sql = super::physical_to_sql(&plan);
        assert!(sql.contains("from users"));
        assert!(sql.contains("where id = 1"));
    }
}
