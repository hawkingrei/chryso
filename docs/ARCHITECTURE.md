# Corundum Architecture (Calcite-Style Optimizer + Parser)

## Goals
- Calcite-like planning pipeline: SQL -> AST -> Logical Plan -> Cascades Optimizer -> Physical Plan -> Executor Adapter.
- Support multiple SQL dialects (initially PostgreSQL + MySQL).
- Pluggable rules and cost models for logical/physical optimization.
- Adapter layer for multiple execution engines (DuckDB first, then Velox/others).
- Rust-first implementation with a clean API boundary for future C++/Go/Java bindings.

## Module Layout
```
crates/
  core/               SQL AST, errors, formatting helpers
  parser/             Dialect-aware parser interface + tokenizer/parser skeleton
  planner/            Logical/physical plan node definitions + builder
  optimizer/          Cascades skeleton (memo, rules, cost)
  metadata/           Statistics, catalog, analyze hooks
  adapter/            Executor adapters (DuckDB, mock)
src/
  lib.rs              Public re-exports (facade crate)
  main.rs             Demo pipeline
```

## Planning Pipeline
1. **Parser** (`parser::SqlParser`)
   - Input: SQL string + dialect config
   - Output: `ast::Statement`
2. **Logical Plan Builder** (`planner::PlanBuilder`)
   - Input: AST
   - Output: `planner::LogicalPlan`
3. **Optimizer (Cascades)**
   - Input: `LogicalPlan`, `StatsCache`, rule set
   - Output: `PhysicalPlan`
4. **Executor Adapter** (`adapter::ExecutorAdapter`)
   - Input: `PhysicalPlan`
   - Output: `QueryResult`

## Cascades Design (Planned)
Core concepts:
- **Memo**: stores logically equivalent expressions in groups.
- **Group**: a set of equivalent logical expressions.
- **GroupExpr**: operator + children group refs.
- **Rules**: transform logical -> logical, or logical -> physical.
- **Cost Model**: evaluates physical alternatives based on statistics.

Expected workflow:
1. Insert initial logical tree into memo.
2. Explore logical rules to expand equivalent alternatives.
3. Implement physical rules to produce physical operators.
4. Enumerate, cost, and pick lowest-cost plan.

Current skeleton includes `optimizer::memo`, `optimizer::rules`, and `optimizer::cost` with a
unit-cost model to keep the pipeline runnable while the rule system evolves.

Logical/physical nodes currently include scan, filter, projection, join, aggregate, sort, and limit.

Physical implementation rules live in `optimizer::physical_rules`, translating logical nodes into
physical ones through a simple rule set.

Physical properties are represented in `optimizer::properties` (currently only ordering).

Cardinality estimation skeleton lives in `optimizer::estimation`.

Catalog and analyze hooks live in `metadata::catalog` and `metadata::analyze`.

Name resolution and type utilities live in `metadata::validate`, `metadata::type_inference`, and
`metadata::type_coercion`.

Typed logical explain and costed physical explain live in `planner::LogicalPlan::explain_typed`
and `planner::PhysicalPlan::explain_costed`.

Adapter capabilities and parameter binding live in `adapter`, with a mock adapter supporting
plan validation and recorded plans for tests.

Benchmarks live under `benches/` for parser and optimizer throughput.

FFI notes live in `docs/FFI.md`, with C ABI skeleton under `ffi/` and bindings placeholders
under `bindings/`.

Diagnostics utilities live in `diagnostics` and error codes in `error::ErrorCode`. SQL formatting
helpers live in `corundum-core::sql_format`, and plan diffing utilities live in `corundum-planner::plan_diff`.

Join algorithms are modeled in `planner::JoinAlgorithm` (hash/nested loop) and index scans in
`LogicalPlan::IndexScan`/`PhysicalPlan::IndexScan`. Property enforcement lives in
`optimizer::enforcer`.

Function registry lives in `metadata::functions`, with window functions represented in the AST.
Top-N rewrites are handled by optimizer rules producing `LogicalPlan::TopN`/`PhysicalPlan::TopN`.

## Statistics & Analyze
`metadata::StatsCache` stores table/column stats. `ANALYZE` will populate:
- Row count
- Distinct count
- Null fraction
- Histogram (later)

These feed the cost model and rule decisions (e.g., join order, index selection).

## Dialect Strategy
- Dialect is a first-class config.
- Parser emits a common AST with dialect tags where needed.
- Logical layer remains dialect-agnostic.

Parser roadmap lives in `docs/PARSER.md`.

## Executor Adapter Strategy
Adapters should expose:
- Plan translation (physical plan -> engine-specific API)
- Execution and row materialization
- Capability flags (e.g., supported operators)

DuckDB adapter is the first target, but the interface is designed so a Velox adapter can be added without altering the optimizer core.

## Language Boundary
Rust core stays stable and minimal in public API. For C++/Go/Java:
- Provide a C ABI facade (ffi crate) for AST/plan creation and optimization.
- Keep serialization formats stable for plan exchange.

## Demo
The current demo flow (in `src/main.rs`) parses a SELECT statement, builds a logical plan, converts it to a physical plan, and runs it through a mock adapter. DuckDB execution is gated behind the `duckdb` feature flag.

## Testing
See `docs/TESTING.md` for helper APIs that execute the pipeline and expose explain output for assertions.
