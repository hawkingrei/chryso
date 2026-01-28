mod ffi;
mod plan;

use chryso_adapter::{AdapterCapabilities, ExecutorAdapter, QueryResult};
#[cfg(not(feature = "velox-ffi"))]
use chryso_core::error::ChrysoError;
use chryso_core::error::ChrysoResult;
use chryso_planner::PhysicalPlan;

#[derive(Debug)]
pub struct VeloxAdapter {
    capabilities: AdapterCapabilities,
}

impl VeloxAdapter {
    pub fn try_new() -> ChrysoResult<Self> {
        Ok(Self {
            capabilities: AdapterCapabilities {
                joins: false,
                aggregates: false,
                distinct: false,
                topn: false,
                sort: false,
                limit: true,
                offset: false,
            },
        })
    }

    pub fn execute_arrow(&self, plan: &PhysicalPlan) -> ChrysoResult<Vec<u8>> {
        self.validate_plan(plan)?;
        let plan_ir = plan::plan_to_ir(plan);
        #[cfg(feature = "velox-ffi")]
        {
            return ffi::execute_plan_arrow(&plan_ir);
        }
        #[cfg(not(feature = "velox-ffi"))]
        {
            let _ = plan_ir;
            Err(ChrysoError::new(
                "velox adapter requires feature \"velox\" at the workspace level",
            ))
        }
    }
}

impl ExecutorAdapter for VeloxAdapter {
    fn execute(&self, plan: &PhysicalPlan) -> ChrysoResult<QueryResult> {
        self.validate_plan(plan)?;
        let plan_ir = plan::plan_to_ir(plan);
        #[cfg(feature = "velox-ffi")]
        {
            return ffi::execute_plan(&plan_ir);
        }
        #[cfg(not(feature = "velox-ffi"))]
        {
            let _ = plan_ir;
            Err(ChrysoError::new(
                "velox adapter requires feature \"velox\" at the workspace level",
            ))
        }
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.capabilities.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::plan::plan_to_ir;
    use chryso_planner::PhysicalPlan;

    #[test]
    fn plan_to_ir_renders_table_scan() {
        let plan = PhysicalPlan::TableScan {
            table: "t".to_string(),
        };
        let ir = plan_to_ir(&plan);
        assert!(ir.contains("TableScan"));
        assert!(ir.contains("\"table\":\"t\""));
    }
}
