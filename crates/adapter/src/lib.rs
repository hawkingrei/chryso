use corundum_core::error::{CorundumError, CorundumResult};
#[cfg(feature = "duckdb")]
use ::duckdb::params_from_iter;
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

pub struct DuckDbAdapter;

impl DuckDbAdapter {
    pub fn new() -> Self {
        Self
    }
}

impl ExecutorAdapter for DuckDbAdapter {
    fn execute(&self, plan: &PhysicalPlan) -> CorundumResult<QueryResult> {
        self.validate_plan(plan)?;
        #[cfg(feature = "duckdb")]
        {
            execute_with_duckdb(plan)
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
            execute_with_duckdb_params(plan, params)
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

#[cfg(feature = "duckdb")]
fn execute_with_duckdb(plan: &PhysicalPlan) -> CorundumResult<QueryResult> {
    let conn = crate::duckdb::connect()?;
    if let PhysicalPlan::Dml { sql } = plan {
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
    let columns = stmt
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    let mut rows_iter = stmt
        .query([])
        .map_err(|err| CorundumError::new(format!("duckdb query failed: {err}")))?;
    while let Some(row) = rows_iter
        .next()
        .map_err(|err| CorundumError::new(format!("duckdb row error: {err}")))? {
        let mut values = Vec::new();
        for idx in 0..columns.len() {
            let value: String = row
                .get(idx)
                .map_err(|err| CorundumError::new(format!("duckdb value error: {err}")))?;
            values.push(value);
        }
        rows.push(values);
    }
    Ok(QueryResult { columns, rows })
}

#[cfg(feature = "duckdb")]
fn execute_with_duckdb_params(
    plan: &PhysicalPlan,
    params: &[ParamValue],
) -> CorundumResult<QueryResult> {
    if let PhysicalPlan::Dml { .. } = plan {
        return Err(CorundumError::new(
            "parameter binding not supported for DML in demo",
        ));
    }
    let sql = crate::physical_to_sql(plan);
    let conn = crate::duckdb::connect()?;
    let mut stmt = conn
        .prepare(&sql)
        .map_err(|err| CorundumError::new(format!("duckdb prepare failed: {err}")))?;
    let duck_params = crate::duckdb::params_to_values(params);
    let columns = stmt
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    let mut rows_iter = stmt
        .query(params_from_iter(duck_params.iter()))
        .map_err(|err| CorundumError::new(format!("duckdb query failed: {err}")))?;
    while let Some(row) = rows_iter
        .next()
        .map_err(|err| CorundumError::new(format!("duckdb row error: {err}")))? {
        let mut values = Vec::new();
        for idx in 0..columns.len() {
            let value: String = row
                .get(idx)
                .map_err(|err| CorundumError::new(format!("duckdb value error: {err}")))?;
            values.push(value);
        }
        rows.push(values);
    }
    Ok(QueryResult { columns, rows })
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
                    format!("{} {dir}", item.expr.to_sql())
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
                    format!("{} {dir}", item.expr.to_sql())
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
