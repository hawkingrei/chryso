mod ffi;
mod plan;

use chryso_adapter::{AdapterCapabilities, ExecutorAdapter, QueryResult};
use chryso_core::error::ChrysoError;
use chryso_core::error::ChrysoResult;
use chryso_planner::PhysicalPlan;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug)]
pub struct VeloxAdapter {
    capabilities: AdapterCapabilities,
    memory_tables: Mutex<HashMap<String, String>>,
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
            memory_tables: Mutex::new(HashMap::new()),
        })
    }

    pub fn register_memory_table(
        &self,
        table: impl Into<String>,
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    ) -> ChrysoResult<()> {
        let payload = encode_memory_payload(&columns, &rows)?;
        let mut tables = self
            .memory_tables
            .lock()
            .map_err(|_| ChrysoError::new("memory table registry lock poisoned"))?;
        tables.insert(table.into(), payload);
        Ok(())
    }

    fn plan_to_ir(&self, plan: &PhysicalPlan) -> ChrysoResult<String> {
        let tables = self
            .memory_tables
            .lock()
            .map_err(|_| ChrysoError::new("memory table registry lock poisoned"))?;
        if tables.is_empty() {
            return Ok(plan::plan_to_ir(plan));
        }
        Ok(plan::plan_to_ir_with_memory_payload(plan, &|table| {
            tables.get(table).cloned()
        }))
    }

    pub fn execute_arrow(&self, plan: &PhysicalPlan) -> ChrysoResult<Vec<u8>> {
        self.validate_plan(plan)?;
        let plan_ir = self.plan_to_ir(plan)?;
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
        let plan_ir = self.plan_to_ir(plan)?;
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

fn encode_memory_payload(columns: &[String], rows: &[Vec<String>]) -> ChrysoResult<String> {
    if columns.is_empty() {
        return Err(ChrysoError::new(
            "memory table requires at least one column",
        ));
    }
    for col in columns {
        if col.is_empty() {
            return Err(ChrysoError::new("memory column names must not be empty"));
        }
        if col.contains('\t') || col.contains('\n') || col.contains('\r') {
            return Err(ChrysoError::new(
                "memory column names must not contain tab/newline characters",
            ));
        }
    }
    for row in rows {
        if row.len() != columns.len() {
            return Err(ChrysoError::new(
                "memory row width does not match column count",
            ));
        }
        for cell in row {
            if cell.contains('\t') || cell.contains('\n') || cell.contains('\r') {
                return Err(ChrysoError::new(
                    "memory payload cells must not contain tab/newline characters",
                ));
            }
        }
    }
    let mut payload = String::new();
    if let Some((first, rest)) = columns.split_first() {
        payload.push_str(first);
        for col in rest {
            payload.push('\t');
            payload.push_str(col);
        }
    }
    payload.push('\n');
    for row in rows {
        if let Some((first, rest)) = row.split_first() {
            payload.push_str(first);
            for cell in rest {
                payload.push('\t');
                payload.push_str(cell);
            }
        }
        payload.push('\n');
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::plan::{plan_to_ir, plan_to_ir_with_memory_payload};
    use super::{VeloxAdapter, encode_memory_payload};
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
        assert_eq!(parsed["storage"], "memory");
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
        let predicate = parsed["predicate"].as_str().unwrap();
        assert!(predicate.contains("\"col\"\"a\""));
        assert!(predicate.contains("'v\\\"'"));
    }

    #[test]
    fn plan_to_ir_includes_memory_payload_when_registered() {
        let plan = PhysicalPlan::TableScan {
            table: "users".to_string(),
        };
        let ir = plan_to_ir_with_memory_payload(&plan, &|table| {
            if table == "users" {
                Some("id\tname\n1\talice\n".to_string())
            } else {
                None
            }
        });
        let parsed: Value = serde_json::from_str(&ir).expect("ir should be valid JSON");
        assert_eq!(parsed["type"], "TableScan");
        assert_eq!(parsed["storage"], "memory");
        assert_eq!(parsed["memory_payload"], "id\tname\n1\talice\n");
    }

    #[test]
    fn register_memory_table_updates_plan_ir() {
        let adapter = VeloxAdapter::try_new().expect("adapter");
        adapter
            .register_memory_table(
                "users",
                vec!["id".to_string(), "name".to_string()],
                vec![vec!["1".to_string(), "alice".to_string()]],
            )
            .expect("register");
        let ir = adapter
            .plan_to_ir(&PhysicalPlan::TableScan {
                table: "users".to_string(),
            })
            .expect("plan ir");
        let parsed: Value = serde_json::from_str(&ir).expect("ir should be valid JSON");
        assert_eq!(parsed["memory_payload"], "id\tname\n1\talice\n");
    }

    #[test]
    fn encode_memory_payload_rejects_invalid_rows() {
        let err = encode_memory_payload(
            &["id".to_string(), "name".to_string()],
            &[vec!["1".to_string()]],
        )
        .expect_err("should fail");
        assert!(err.to_string().contains("row width"));
    }

    #[test]
    fn encode_memory_payload_rejects_control_chars() {
        let err = encode_memory_payload(
            &["id".to_string(), "name".to_string()],
            &[vec!["1".to_string(), "alice\nbob".to_string()]],
        )
        .expect_err("should fail");
        assert!(err.to_string().contains("tab/newline"));
    }

    #[test]
    fn encode_memory_payload_rejects_empty_column_names() {
        let err = encode_memory_payload(&["".to_string()], &[vec!["1".to_string()]])
            .expect_err("should fail");
        assert!(err.to_string().contains("must not be empty"));
    }
}
