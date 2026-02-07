# Adapter Guide

## Overview
Adapters translate physical plans into engine-specific execution. The trait lives in
`crates/adapter/src/lib.rs`.

## Required Methods
- `execute(plan)` runs the plan and returns `QueryResult`.

## Optional Methods
- `execute_with_params(plan, params)` for parameter binding.
- `capabilities()` to declare supported operators.
- `validate_plan(plan)` to fail fast on unsupported plans.

## Adding an Adapter
1. Implement `ExecutorAdapter` for your engine.
2. Provide plan lowering in `chryso-adapter-duckdb::physical_to_sql` or engine APIs.
3. Add feature flags if the adapter brings heavy dependencies.

## DuckDB Operator Adapter
The `chryso-adapter-duckdb-ops` crate defines a JSON plan IR and a C++ FFI bridge
that builds DuckDB relational operators directly (no SQL). The FFI library lives
under `ffi/duckdb` and expects a DuckDB submodule by default.

In the DuckDB ops path, `IndexScan` currently degrades to `TableScan + Filter`. The `index` field is optional and only expresses planning intent; it is not mapped to a DuckDB index access path yet.

## Testing
Use `MockAdapter` to validate plan generation and to test fallback behavior.

## CI Notes
Velox CI builds the FFI in exec-only mode by default (no Arrow) to reduce build time.
If you need Arrow IPC output, enable Arrow support in the workflow via `CHRYSO_VELOX_USE_ARROW`.
Exec-only mode does not control Arrow support.
