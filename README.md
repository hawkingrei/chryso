# Chryso

Chryso is a Calcite-style SQL parser + optimizer engine in Rust. It focuses on a clean, modular pipeline (AST -> logical plan -> Cascades optimizer -> physical plan -> adapter) and keeps the core engine execution-agnostic.

## Features
- Multi-dialect SQL parsing (Postgres + MySQL first)
- Cascades optimizer with logical and physical rules
- Cost and statistics hooks via ANALYZE and a lightweight catalog
- Adapter-based execution (DuckDB first, Velox next)

## Status
- Parser, planner, and core Cascades skeleton are implemented
- DuckDB adapter translates physical plans to SQL and executes simple queries/DML
- CI builds and tests with and without the DuckDB feature
- Velox CI builds the FFI in exec-only mode (no Arrow) to keep the pipeline lean

## Workspace Layout
```
crates/
  core/       AST, errors, formatting helpers
  parser/     Dialect-aware parser
  planner/    Logical/physical plan definitions + builder
  optimizer/  Cascades skeleton (memo, rules, cost)
  metadata/   Catalog, stats cache, analyze hooks
  adapter/    Execution adapters (DuckDB, mock)
  parser_yacc/ Yacc scaffold (validation)
src/
  lib.rs      Facade crate re-exports
  bin/
    chryso-cli.rs
```

## Quick Start

Run the CLI (requires DuckDB feature):
```bash
cargo run --bin chryso-cli --features duckdb
```

Run the DuckDB demo:
```bash
cargo run --example duckdb_demo --features duckdb
```

## Tests

All tests:
```bash
cargo test
```

With DuckDB:
```bash
cargo test --features duckdb
```

Velox exec-only build (FFI):
```bash
cmake -S ffi/velox -B ffi/velox/build \
  -DCHRYSO_VELOX_USE_SUBMODULE=ON \
  -DCHRYSO_VELOX_USE_ARROW=OFF \
  -DCHRYSO_ARROW_STRICT_VERSION=OFF \
  -DCHRYSO_VELOX_BUILD_TESTS=ON \
  -DCHRYSO_VELOX_EXEC_ONLY=ON
cmake --build ffi/velox/build --parallel
```

Plan snapshot tests:
- Inputs: `tests/testdata/plan/case1/in.json`
- Expected outputs: `tests/testdata/plan/case1/out.json`

Record snapshots:
```bash
CHRYSO_RECORD=1 cargo test --test plan_snapshot
```

## Docs
- `docs/ARCHITECTURE.md` for the planning pipeline and module overview
- `docs/RULES.md` for rule conventions and current rule set
- `docs/ADAPTERS.md` for adapter design
- `docs/ROADMAP.md` for milestones and TODOs

## Roadmap (Highlights)
- Parser: expand dialect coverage and AST compatibility
- Logical rewrites: constant folding and predicate simplification
- Cascades: richer rule library and join order enumeration
- Costing: integrate ANALYZE stats into cost models
- Physical planning: enforcers and required properties
- Adapters: stabilize DuckDB, then add Velox

## License
TBD.
