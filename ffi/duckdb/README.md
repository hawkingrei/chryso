# DuckDB Ops FFI

This folder builds the DuckDB operator FFI used by `crates/adapter-duckdb-ops`.
It expects a vendored DuckDB submodule by default.

## Submodule setup

Run once at repo root:

```bash
git submodule update --init --recursive
```

The DuckDB submodule should live at `ffi/duckdb/duckdb`.

## Build via cargo

Enable the feature and let `build.rs` compile the FFI:

```bash
cargo build -p chryso-adapter-duckdb-ops --features duckdb-ops-ffi
```

### Environment overrides

- `CHRYSO_DUCKDB_FFI_DIR`: path to a prebuilt `chryso_duckdb_ffi` library.
- `CHRYSO_DUCKDB_USE_SUBMODULE`: `ON` or `OFF` (default: `ON`).
