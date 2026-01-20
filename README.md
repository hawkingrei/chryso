# Corundum

Corundum is a Calcite-style SQL parser + optimizer engine in Rust. It focuses on a clean, modular architecture (AST -> logical plan -> Cascades optimizer -> physical plan -> adapter), with DuckDB as the first execution backend.

## Goals
- SQL parser with multi-dialect support (PostgreSQL + MySQL first).
- Cascades optimizer with logical/physical rules and cost-based search.
- Statistics collection via ANALYZE and a lightweight catalog interface.
- Adapter-based execution so the optimizer can target DuckDB, Velox, and others.

## Status
- Parser, planner, and a minimal Cascades optimizer skeleton are implemented.
- DuckDB adapter can translate physical plans to SQL and execute simple queries/DML.
- CI builds and tests with and without the DuckDB feature.

## Workspace Layout
```
crates/
  core/       AST, errors, formatting helpers
  parser/     Dialect-aware parser
  planner/    Logical/physical plan definitions + builder
  optimizer/  Cascades skeleton (memo, rules, cost)
  metadata/   Catalog, stats cache, analyze hooks
  adapter/    Execution adapters (DuckDB, mock)
src/
  lib.rs      Facade crate re-exports
  main.rs     Demo pipeline
```

## Quick Start

Build and run the mock adapter demo:
```bash
cargo run
```

Run the DuckDB demo:
```bash
cargo run --example duckdb_demo --features duckdb
```

## Tests
Run all tests:
```bash
cargo test
```

Run tests with DuckDB enabled:
```bash
cargo test --features duckdb
```

Plan snapshot tests:
- Inputs: `tests/testdata/plan/case1/in.json`
- Expected outputs: `tests/testdata/plan/case1/out.json`

Record snapshots:
```bash
CORUNDUM_RECORD=1 cargo test --test plan_snapshot
```

## Demo (CLI examples)
Run the built-in mock adapter pipeline (prints logical/physical plans and a mocked result set):
```bash
cargo run
```

Run the DuckDB adapter demo (executes DDL/DML and a select via DuckDB):
```bash
cargo run --example duckdb_demo --features duckdb
```

## Docs
- `docs/ARCHITECTURE.md` for the planning pipeline and module overview.
- `docs/RULES.md` for rule conventions and existing rule set.
- `docs/ADAPTERS.md` for adapter design.
- `docs/ROADMAP.md` for the 100-step plan and TODOs (including Bazel).

## Roadmap (Highlights)
See `docs/ROADMAP.md` for the full 100-step plan. Key milestones:
- **Parser**: expand dialect coverage (Postgres/MySQL) and improve AST compatibility.
- **Logical rewrites**: add expression-level rewrites (constant folding, predicate simplification).
- **Cascades**: richer rule library, join order enumeration, and memo pruning.
- **Costing**: integrate ANALYZE stats into cardinality + cost models.
- **Physical planning**: add enforcers and required physical properties.
- **Adapters**: stabilize DuckDB translation, then layer in Velox and others.

## License
TBD.
