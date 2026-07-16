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

## DuckDB Adapter
The `chryso-adapter-duckdb` crate lowers physical plans into SQL and executes them in DuckDB.
Operator-level FFI is not exposed; DuckDB integration is only available through the adapter.
`IndexScan` currently degrades to `TableScan + Filter` by emitting a `WHERE` predicate. The `index`
field is optional and only expresses planning intent; it is not mapped to a DuckDB index access path yet.
The optimizer can represent `Exchange` for distribution enforcement, but the DuckDB adapter reports
`exchange: false` and rejects such plans at capability validation. This keeps a distributed planning
property from being silently erased at execution time.

## Velox Adapter
The Velox plan serializer represents `Exchange` in its JSON IR for future engine integration. The
current executor capability remains `exchange: false`, so execution rejects distributed plans until
the runtime transport and partitioning contract is implemented.

## Testing
Use `MockAdapter` to validate plan generation and to test fallback behavior.

## CI Notes
Velox CI builds the FFI in exec-only mode by default (no Arrow) to reduce build time.
If you need Arrow IPC output, enable Arrow support in the workflow via `CHRYSO_VELOX_USE_ARROW`.
Exec-only mode does not control Arrow support.
