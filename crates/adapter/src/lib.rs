use chryso_core::error::{ChrysoError, ChrysoResult};
use chryso_planner::PhysicalPlan;
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
            PhysicalPlan::Limit {
                limit,
                offset,
                input,
            } => {
                let offset_ok = if offset.is_some() { self.offset } else { true };
                let limit_ok = if limit.is_some() { self.limit } else { true };
                offset_ok && limit_ok && self.supports_plan(input)
            }
        }
    }
}

pub trait ExecutorAdapter {
    fn execute(&self, plan: &PhysicalPlan) -> ChrysoResult<QueryResult>;

    fn execute_with_params(
        &self,
        plan: &PhysicalPlan,
        params: &[ParamValue],
    ) -> ChrysoResult<QueryResult> {
        if params.is_empty() {
            self.execute(plan)
        } else {
            Err(ChrysoError::new(
                "adapter does not support parameter binding",
            ))
        }
    }

    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::all()
    }

    fn validate_plan(&self, plan: &PhysicalPlan) -> ChrysoResult<()> {
        if self.capabilities().supports_plan(plan) {
            Ok(())
        } else {
            Err(ChrysoError::new(
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
    fn execute(&self, plan: &PhysicalPlan) -> ChrysoResult<QueryResult> {
        self.validate_plan(plan)?;
        *self.last_plan.borrow_mut() = Some(plan.explain(0));
        Ok(self.result.clone())
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.capabilities.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{AdapterCapabilities, ExecutorAdapter, MockAdapter, ParamValue, QueryResult};
    use chryso_planner::PhysicalPlan;

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
            join_type: chryso_core::ast::JoinType::Inner,
            algorithm: chryso_planner::JoinAlgorithm::Hash,
            left: Box::new(PhysicalPlan::TableScan {
                table: "t1".to_string(),
            }),
            right: Box::new(PhysicalPlan::TableScan {
                table: "t2".to_string(),
            }),
            on: chryso_core::ast::Expr::Identifier("t1.id = t2.id".to_string()),
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
}
