# Chryso

Chryso is an HTAP optimizer engine in Rust. It aims to provide a Calcite-style
planning core for hybrid transactional/analytical systems: parse SQL, build a
dialect-neutral logical plan, explore alternatives with a Cascades optimizer,
choose a costed physical plan, and lower that plan into execution engines such
as DuckDB, Velox, or future storage/compute adapters.

The project keeps the optimizer engine execution-agnostic. SQL dialect support,
statistics collection, cost modeling, physical properties, and executor adapters
are explicit extension points so Chryso can evolve from a runnable demo pipeline
into a reusable HTAP planning layer.

## Features
- Multi-dialect SQL front-end, with PostgreSQL and MySQL compatibility first.
- Dialect-neutral logical planning for scans, filters, projections, joins,
  aggregates, sorting, limits, subqueries, and DML/DDL hooks.
- Cascades-style optimizer with memo exploration, logical rewrites, physical
  implementation rules, property enforcement, join ordering, and tracing.
- Cost and statistics hooks via `ANALYZE`, a lightweight catalog, cardinality
  estimation, and cost profiles.
- Execution-adapter boundary for HTAP backends, starting with DuckDB and Velox.
- Stable facade API and FFI/bindings scaffolding for embedding the optimizer in
  other systems.

## Status
- Parser, planner, metadata, and Cascades optimizer crates are implemented.
- Logical and physical plans cover the main MVP operators needed for HTAP query
  planning.
- DuckDB adapter translates physical plans to SQL and executes simple
  queries/DML.
- Velox adapter and FFI are available behind feature gates, with CI coverage for
  an exec-only build.
- CI builds and tests the core workspace with and without optional adapters.

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

Build with Bazel:
```bash
bazel build //:chryso
bazel build //:chryso_cli
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
- `docs/BAZEL.md` for Bazel targets and scope
- `docs/ROADMAP.md` for milestones and TODOs

## Roadmap (Highlights)
- HTAP planning model: make transactional and analytical execution trade-offs
  visible in physical properties, costs, and adapter capabilities.
- Parser: expand PostgreSQL/MySQL dialect coverage and AST compatibility.
- Logical rewrites: deepen predicate inference, subquery decorrelation, and
  projection/column pruning.
- Cascades: improve rule scheduling, join order enumeration, and explainable
  optimizer traces.
- Costing: integrate richer `ANALYZE` statistics, histograms, calibration data,
  and backend-specific cost profiles.
- Physical planning: strengthen distribution, ordering, parallelism, and
  enforcer handling for mixed OLTP/OLAP engines.
- Adapters: stabilize DuckDB, mature Velox, and keep the adapter API open for
  future HTAP storage/compute backends.

## License
TBD.
