# Testing Strategy

## Goals
- Unit tests should validate the full pipeline: parse -> logical -> optimize -> physical -> execute.
- Tests should be able to assert on explain output and execution results.
- Tests must run without external engines by default.

## Test Helpers
`corundum::test_support` provides:
- `execute(sql, dialect)` returns logical/physical explains plus execution results.
- `execute_with_adapter(sql, dialect, adapter)` allows integration with real engines.
- `explain(sql, dialect)` returns logical/physical explain strings only.

These helpers are compiled for unit tests and can be enabled for integration tests with the
`test-utils` feature.

## Running Tests
- Unit tests:
  - `cargo test`
- Parser-focused unit tests:
  - `cargo test -p corundum-parser`
- Integration tests using test helpers:
  - `cargo test --features test-utils`
- Plan snapshot tests:
  - `cargo test --test plan_snapshot`

## Snapshot Recording
To update `tests/testdata/plan/case1/out.json`, run:
```
CORUNDUM_RECORD=1 cargo test --test plan_snapshot
```

## Example (Integration Test)
```rust
use corundum::parser::Dialect;
use corundum::test_support::execute;

#[test]
fn pipeline_is_explainable() {
    let sql = "select id from users where id = 1";
    let run = execute(sql, Dialect::Postgres).expect("execute");
    assert!(run.logical_explain.contains("LogicalProject"));
    assert!(run.physical_explain.contains("Project"));
}
```
