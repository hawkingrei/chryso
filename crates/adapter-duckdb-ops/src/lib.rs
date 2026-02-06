mod ffi;
mod plan;

use chryso_adapter::{AdapterCapabilities, ExecutorAdapter, QueryResult};
#[cfg(not(feature = "duckdb-ops-ffi"))]
use chryso_core::error::ChrysoError;
use chryso_core::error::ChrysoResult;
use chryso_planner::PhysicalPlan;

#[derive(Debug)]
pub struct DuckDbOpsAdapter {
    capabilities: AdapterCapabilities,
    session: ffi::DuckDbOpsSession,
}

impl DuckDbOpsAdapter {
    pub fn try_new() -> ChrysoResult<Self> {
        Ok(Self {
            capabilities: AdapterCapabilities::all(),
            session: ffi::DuckDbOpsSession::new()?,
        })
    }

    pub fn execute_binary(&self, plan: &PhysicalPlan) -> ChrysoResult<QueryResult> {
        self.validate_plan(plan)?;
        let payload = plan::plan_to_bytes(plan)?;
        self.session.execute_plan(&payload)
    }
}

impl ExecutorAdapter for DuckDbOpsAdapter {
    fn execute(&self, plan: &PhysicalPlan) -> ChrysoResult<QueryResult> {
        self.execute_binary(plan)
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.capabilities.clone()
    }
}

#[cfg(not(feature = "duckdb-ops-ffi"))]
impl DuckDbOpsAdapter {
    pub fn ffi_disabled_error() -> ChrysoError {
        ChrysoError::new(
            "duckdb ops adapter requires feature \"duckdb-ops-ffi\" (or workspace feature \"duckdb-ops-ffi\")",
        )
    }
}
