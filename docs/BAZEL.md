# Bazel Build

This repository now provides Bazel entrypoints for the core Rust workspace targets.

## Prerequisites

- Bazel with Bzlmod enabled (`.bazelrc` already sets `--enable_bzlmod`).
- Network access for first-time dependency fetch via `rules_rust` and `crate_universe`.

## Build Targets

- `//:chryso` - facade Rust library (default features).
- `//:chryso_duckdb` - facade Rust library with `duckdb` feature.
- `//:chryso_tune` - tuning binary.
- `//:chryso_cli` - CLI binary built against `duckdb` feature.
- `//crates/core:chryso_core`
- `//crates/parser:chryso_parser`
- `//crates/metadata:chryso_metadata`
- `//crates/planner:chryso_planner`
- `//crates/optimizer:chryso_optimizer`
- `//crates/adapter:chryso_adapter`
- `//crates/adapter-duckdb:chryso_adapter_duckdb`
- `//crates/adapter-duckdb:chryso_adapter_duckdb_bundled`
- `//crates/adapter-velox:chryso_adapter_velox`

## Example Commands

```bash
bazel build //:chryso
bazel build //:chryso_duckdb
bazel build //:chryso_cli
bazel build //crates/optimizer:chryso_optimizer
```

## Current Scope

- Bazel coverage focuses on the Rust workspace crates above.
- `crates/parser_yacc` is intentionally excluded for now because it requires build-script code generation (`lrlex`/`lrpar` outputs in `OUT_DIR`).
- `ffi/` is intentionally excluded for now because it requires dedicated `cdylib/staticlib` and C++ toolchain integration.

