use ::duckdb::params_from_iter;
use ::duckdb::types::Value as DuckValue;
use chryso_adapter::{ExecutorAdapter, ParamValue, QueryResult};
use chryso_core::ast::{OrderByExpr, Statement};
use chryso_core::error::{ChrysoError, ChrysoResult};
use chryso_metadata::{ColumnStats, StatsCache, StatsProvider, TableStats};
use chryso_parser::{ParserConfig, SimpleParser, SqlParser};
use chryso_planner::PhysicalPlan;
use std::cell::RefCell;
use std::collections::HashMap;

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
        let summarized = try_summarize_column_stats(&conn, table, row_count).unwrap_or_default();
        for column in columns {
            let stats = match summarized.get(&column) {
                Some(stats) => stats.clone(),
                None => load_column_stats_fallback(&conn, table, &column, row_count)?,
            };
            cache.insert_column_stats(table, &column, stats);
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

fn load_column_stats_fallback(
    conn: &::duckdb::Connection,
    table: &str,
    column: &str,
    row_count: i64,
) -> ChrysoResult<ColumnStats> {
    let sql = format!(
        "select count(distinct {column}), coalesce(sum(case when {column} is null then 1 else 0 end), 0) from {table}"
    );
    let (distinct_count, nulls) = query_i64_pair(conn, &sql)?;
    let null_fraction = if row_count == 0 {
        0.0
    } else {
        nulls as f64 / row_count as f64
    };
    Ok(ColumnStats {
        distinct_count: distinct_count.max(1) as f64,
        null_fraction,
    })
}

#[derive(Debug, Clone, Copy)]
struct SummarizeColumnIndexes {
    column_name: usize,
    approx_unique: Option<usize>,
    null_percentage: Option<usize>,
    non_null_count: Option<usize>,
}

fn try_summarize_column_stats(
    conn: &::duckdb::Connection,
    table: &str,
    row_count: i64,
) -> ChrysoResult<HashMap<String, ColumnStats>> {
    let mut stmt = conn
        .prepare(&format!("summarize {table}"))
        .map_err(|err| ChrysoError::new(format!("duckdb prepare failed: {err}")))?;
    let mut rows = stmt
        .query([])
        .map_err(|err| ChrysoError::new(format!("duckdb query failed: {err}")))?;
    let column_names = rows
        .as_ref()
        .map(|statement| statement.column_names())
        .unwrap_or_default();
    let indexes = match infer_summarize_column_indexes(&column_names) {
        Some(indexes) => indexes,
        None => return Ok(HashMap::new()),
    };
    let mut stats = HashMap::new();
    while let Some(row) = rows
        .next()
        .map_err(|err| ChrysoError::new(format!("duckdb row error: {err}")))?
    {
        let column_value: DuckValue = row
            .get(indexes.column_name)
            .map_err(|err| ChrysoError::new(format!("duckdb value error: {err}")))?;
        let Some(column_name) = duck_value_to_text(&column_value) else {
            continue;
        };
        let column_name = column_name.trim();
        if column_name.is_empty() {
            continue;
        }

        let distinct_count = indexes
            .approx_unique
            .and_then(|index| row.get::<usize, DuckValue>(index).ok())
            .and_then(|value| duck_value_to_f64(&value));

        let null_fraction = if row_count == 0 {
            Some(0.0)
        } else if let Some(index) = indexes.null_percentage {
            row.get::<usize, DuckValue>(index)
                .ok()
                .and_then(|value| parse_null_fraction(&value))
        } else if let Some(index) = indexes.non_null_count {
            row.get::<usize, DuckValue>(index)
                .ok()
                .and_then(|value| duck_value_to_f64(&value))
                .map(|non_null| {
                    let value = (row_count as f64 - non_null) / row_count as f64;
                    value.clamp(0.0, 1.0)
                })
        } else {
            None
        };

        if let (Some(distinct_count), Some(null_fraction)) = (distinct_count, null_fraction) {
            stats.insert(
                column_name.to_string(),
                ColumnStats {
                    distinct_count: distinct_count.max(1.0),
                    null_fraction: null_fraction.clamp(0.0, 1.0),
                },
            );
        }
    }
    Ok(stats)
}

fn infer_summarize_column_indexes(column_names: &[String]) -> Option<SummarizeColumnIndexes> {
    let mut column_name = None;
    let mut approx_unique = None;
    let mut null_percentage = None;
    let mut non_null_count = None;

    for (index, name) in column_names.iter().enumerate() {
        let normalized = name.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "column_name" | "column" | "columnname" => column_name = Some(index),
            "approx_unique" | "approx_count_distinct" | "distinct_count" | "unique" => {
                approx_unique = Some(index)
            }
            "null_percentage" | "null_percent" | "null_fraction" => null_percentage = Some(index),
            "count" | "non_null" | "nonnull_count" | "non_null_count" => {
                non_null_count = Some(index)
            }
            _ => {}
        }
    }

    column_name.map(|column_name| SummarizeColumnIndexes {
        column_name,
        approx_unique,
        null_percentage,
        non_null_count,
    })
}

fn duck_value_to_text(value: &DuckValue) -> Option<String> {
    match value {
        DuckValue::Null => None,
        DuckValue::Text(text) | DuckValue::Enum(text) => Some(text.clone()),
        other => Some(format_duck_value(other)),
    }
}

fn duck_value_to_f64(value: &DuckValue) -> Option<f64> {
    match value {
        DuckValue::Null => None,
        DuckValue::Boolean(value) => Some(if *value { 1.0 } else { 0.0 }),
        DuckValue::TinyInt(value) => Some(*value as f64),
        DuckValue::SmallInt(value) => Some(*value as f64),
        DuckValue::Int(value) => Some(*value as f64),
        DuckValue::BigInt(value) => Some(*value as f64),
        DuckValue::HugeInt(value) => Some(*value as f64),
        DuckValue::UTinyInt(value) => Some(*value as f64),
        DuckValue::USmallInt(value) => Some(*value as f64),
        DuckValue::UInt(value) => Some(*value as f64),
        DuckValue::UBigInt(value) => Some(*value as f64),
        DuckValue::Float(value) => Some(*value as f64),
        DuckValue::Double(value) => Some(*value),
        DuckValue::Decimal(value) => value.to_string().parse::<f64>().ok(),
        DuckValue::Text(value) | DuckValue::Enum(value) => parse_numeric_text(value),
        _ => None,
    }
}

fn parse_numeric_text(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed
        .trim_end_matches('%')
        .replace(',', "")
        .replace('_', "");
    normalized.parse::<f64>().ok()
}

fn parse_null_fraction(value: &DuckValue) -> Option<f64> {
    let raw = duck_value_to_f64(value)?;
    if raw.is_nan() {
        return None;
    }
    let fraction = if raw > 1.0 { raw / 100.0 } else { raw };
    Some(fraction.clamp(0.0, 1.0))
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

#[derive(Debug, Clone)]
struct SelectSql {
    select_list: Vec<String>,
    from: String,
    where_clause: Option<String>,
    order_by: Vec<String>,
    limit: Option<u64>,
    offset: Option<u64>,
    distinct: bool,
}

impl SelectSql {
    fn new(from: String) -> Self {
        Self {
            select_list: vec!["*".to_string()],
            from,
            where_clause: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
            distinct: false,
        }
    }

    fn is_wildcard_select(&self) -> bool {
        self.select_list.len() == 1 && self.select_list[0] == "*"
    }

    fn has_limit_or_offset(&self) -> bool {
        self.limit.is_some() || self.offset.is_some()
    }

    fn render(&self) -> String {
        let select_list = if self.select_list.is_empty() {
            "*".to_string()
        } else {
            self.select_list.join(", ")
        };
        let distinct = if self.distinct { "distinct " } else { "" };
        let mut sql = format!("select {distinct}{select_list} from {}", self.from);
        if let Some(where_clause) = &self.where_clause {
            sql.push_str(&format!(" where {where_clause}"));
        }
        if !self.order_by.is_empty() {
            sql.push_str(&format!(" order by {}", self.order_by.join(", ")));
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" limit {limit}"));
        }
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" offset {offset}"));
        }
        sql
    }
}

fn render_order_by(order_by: &[OrderByExpr]) -> Vec<String> {
    order_by
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
        .collect()
}

fn lower_select_chain(plan: &PhysicalPlan) -> Option<SelectSql> {
    match plan {
        PhysicalPlan::TableScan { table } => Some(SelectSql::new(table.clone())),
        PhysicalPlan::IndexScan {
            table,
            index: _,
            predicate,
        } => {
            let mut select = SelectSql::new(table.clone());
            select.where_clause = Some(predicate.to_sql());
            Some(select)
        }
        PhysicalPlan::Derived {
            input,
            alias,
            column_aliases,
        } => {
            let base = physical_to_sql(input);
            let from = if column_aliases.is_empty() {
                format!("({base}) as {alias}")
            } else {
                format!("({base}) as {alias} ({})", column_aliases.join(", "))
            };
            Some(SelectSql::new(from))
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
            let from = format!(
                "({left_sql}) as l {join} ({right_sql}) as r on {}",
                on.to_sql()
            );
            Some(SelectSql::new(from))
        }
        PhysicalPlan::Filter { predicate, input } => {
            let mut select = lower_select_chain(input)?;
            if select.distinct || select.has_limit_or_offset() {
                return None;
            }
            let rendered = predicate.to_sql();
            select.where_clause = Some(match select.where_clause {
                Some(existing) => format!("({existing}) and ({rendered})"),
                None => rendered,
            });
            Some(select)
        }
        PhysicalPlan::Projection { exprs, input } => {
            let mut select = lower_select_chain(input)?;
            if select.distinct || select.has_limit_or_offset() || !select.is_wildcard_select() {
                return None;
            }
            select.select_list = exprs.iter().map(|expr| expr.to_sql()).collect();
            Some(select)
        }
        PhysicalPlan::Distinct { input } => {
            let mut select = lower_select_chain(input)?;
            select.distinct = true;
            Some(select)
        }
        PhysicalPlan::Sort { order_by, input } => {
            let mut select = lower_select_chain(input)?;
            if select.has_limit_or_offset() {
                return None;
            }
            select.order_by = render_order_by(order_by);
            Some(select)
        }
        PhysicalPlan::TopN {
            order_by,
            limit,
            input,
        } => {
            let mut select = lower_select_chain(input)?;
            if select.has_limit_or_offset() || !select.order_by.is_empty() {
                return None;
            }
            select.order_by = render_order_by(order_by);
            select.limit = Some(*limit);
            Some(select)
        }
        PhysicalPlan::Limit {
            limit,
            offset,
            input,
        } => {
            let mut select = lower_select_chain(input)?;
            if select.has_limit_or_offset() {
                return None;
            }
            select.limit = *limit;
            select.offset = *offset;
            Some(select)
        }
        PhysicalPlan::Aggregate { .. } | PhysicalPlan::Dml { .. } => None,
    }
}

pub fn physical_to_sql(plan: &PhysicalPlan) -> String {
    if let Some(select) = lower_select_chain(plan) {
        return select.render();
    }
    physical_to_sql_fallback(plan)
}

fn physical_to_sql_fallback(plan: &PhysicalPlan) -> String {
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
            format!("select * from ({base}) as t where {}", predicate.to_sql())
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
            let order_list = render_order_by(order_by).join(", ");
            format!("select * from ({base}) as t order by {order_list} limit {limit}")
        }
        PhysicalPlan::Sort { order_by, input } => {
            let base = physical_to_sql(input);
            let order_list = render_order_by(order_by).join(", ");
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
