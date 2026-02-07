#![cfg(feature = "duckdb-ops-ffi")]

use chryso_adapter::ExecutorAdapter;
use chryso_adapter_duckdb_ops::DuckDbOpsAdapter;
use chryso_planner::PhysicalPlan;

#[test]
fn derived_column_aliases_rename_columns() {
    let adapter = DuckDbOpsAdapter::try_new().expect("adapter");

    let create = PhysicalPlan::Dml {
        sql: "create table t(id int, name varchar)".to_string(),
    };
    adapter.execute(&create).expect("create table");

    let insert = PhysicalPlan::Dml {
        sql: "insert into t values (1, 'alice')".to_string(),
    };
    adapter.execute(&insert).expect("insert");

    let plan = PhysicalPlan::Derived {
        input: Box::new(PhysicalPlan::TableScan {
            table: "t".to_string(),
        }),
        alias: "u".to_string(),
        column_aliases: vec!["uid".to_string(), "uname".to_string()],
    };
    let result = adapter.execute(&plan).expect("execute");
    assert_eq!(result.columns, vec!["uid".to_string(), "uname".to_string()]);
    assert_eq!(result.rows.len(), 1);
}
