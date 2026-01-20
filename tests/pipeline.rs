#[cfg(feature = "test-utils")]
use corundum::parser::Dialect;
#[cfg(feature = "test-utils")]
use corundum::test_support::execute;

#[cfg(feature = "test-utils")]
#[test]
fn pipeline_executes_with_test_helpers() {
    let sql = "select id from users where id = 1";
    let run = execute(sql, Dialect::Postgres).expect("execute");
    assert!(run.logical_explain.contains("LogicalProject"));
    assert!(run.physical_explain.contains("Project"));
    assert_eq!(run.result.rows.len(), 1);
}
