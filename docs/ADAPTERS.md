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
2. Provide plan lowering in `adapter::physical_to_sql` or engine APIs.
3. Add feature flags if the adapter brings heavy dependencies.

## Testing
Use `MockAdapter` to validate plan generation and to test fallback behavior.
