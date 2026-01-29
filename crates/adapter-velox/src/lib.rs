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
            ffi::execute_plan_arrow(&plan_ir)
        }
        #[cfg(not(feature = "velox-ffi"))]
        {
            let _ = plan_ir;
            Err(ChrysoError::new(
                "velox adapter requires feature \"velox-ffi\" (or workspace feature \"velox\")",
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
            ffi::execute_plan(&plan_ir)
        }
        #[cfg(not(feature = "velox-ffi"))]
        {
            let _ = plan_ir;
            Err(ChrysoError::new(
                "velox adapter requires feature \"velox-ffi\" (or workspace feature \"velox\")",
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
    use chryso_core::ast::{BinaryOperator, Expr, Literal};
    use chryso_planner::PhysicalPlan;
    use serde_json::Value;

    #[test]
    fn plan_to_ir_renders_table_scan() {
        let plan = PhysicalPlan::TableScan {
            table: "t\"\\name".to_string(),
        };
        let ir = plan_to_ir(&plan);
        let parsed: Value = serde_json::from_str(&ir).expect("ir should be valid JSON");
        assert_eq!(parsed["type"], "TableScan");
        assert_eq!(parsed["table"], "t\"\\name");
    }

    #[test]
    fn plan_to_ir_handles_nested_plan_and_escaping() {
        let predicate = Expr::BinaryOp {
            left: Box::new(Expr::Identifier("col\"a".to_string())),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Literal(Literal::String("v\\\"".to_string()))),
        };
        let plan = PhysicalPlan::Filter {
            predicate,
            input: Box::new(PhysicalPlan::TableScan {
                table: "tab\"le".to_string(),
            }),
        };
        let ir = plan_to_ir(&plan);
        let parsed: Value = serde_json::from_str(&ir).expect("ir should be valid JSON");
        assert_eq!(parsed["type"], "Filter");
        assert_eq!(parsed["input"]["type"], "TableScan");
        assert_eq!(parsed["input"]["table"], "tab\"le");
        assert!(parsed["predicate"].as_str().unwrap().contains("col\"a"));
    }
}
