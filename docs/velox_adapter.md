# Velox Adapter (C++/FFI)

This document describes how to wire Chryso to Velox using a C ABI bridge.

## Overview

- Rust builds `PhysicalPlan` and converts it to a minimal JSON plan IR.
- The Velox adapter calls a C ABI exposed by `ffi/velox`.
- The C ABI creates a Velox session, builds a plan, executes, and returns results as TSV.

## Repo Layout

- `crates/adapter-velox`: Rust adapter and plan IR mapping.
- `ffi/velox`: C++ bridge library that exports a C ABI.
- `src/sql_utils.rs`: shared SQL splitting for CLI/tools.

## Submodule Setup

From repo root:

```bash
git submodule update --init --recursive
```

## Build (macOS/Linux)

```bash
cmake -S ffi/velox -B ffi/velox/build -DCHRYSO_VELOX_USE_SUBMODULE=ON
cmake --build ffi/velox/build --parallel
```

This builds `libchryso_velox_ffi` which must be discoverable by the Rust build.

## Rust Build

```bash
cargo build -p chryso-adapter-velox --features velox-ffi
```

Or at workspace level:

```bash
cargo build --features velox
```

## Demo

Build the C++ shim, then run the demo example:

```bash
scripts/build_velox_ffi.sh ffi/velox/build OFF ON OFF

export CHRYSO_VELOX_FFI_DIR="$(pwd)/ffi/velox/build"
export DYLD_LIBRARY_PATH="$CHRYSO_VELOX_FFI_DIR:$DYLD_LIBRARY_PATH"   # macOS
export LD_LIBRARY_PATH="$CHRYSO_VELOX_FFI_DIR:$LD_LIBRARY_PATH"       # Linux

cargo run --example velox_demo --features velox
```

Expected output (demo stub):

```
[["table"]]
[["demo_table"]]
```

Arrow IPC output is written to `velox_demo.arrow` in the repo root.

If you must enforce Arrow 15.x, set `-DCHRYSO_ARROW_STRICT_VERSION=ON` and ensure Arrow 15 is installed.

## Runtime Troubleshooting (macOS)

If you see:

```
dyld: Library not loaded: @rpath/libchryso_velox_ffi.dylib
```

Use one of the following:

1) Set `DYLD_LIBRARY_PATH` before running:

```bash
DYLD_LIBRARY_PATH="$(pwd)/ffi/velox/build" cargo run --example velox_demo --features velox
```

2) Or add an rpath to the built binary:

```bash
install_name_tool -add_rpath "$(pwd)/ffi/velox/build" target/debug/examples/velox_demo
```

## C ABI Contract

```c
VxSession* vx_session_new();
void vx_session_free(VxSession* session);
int vx_plan_execute(VxSession* session, const char* plan_json, char** result_out);
int vx_plan_execute_arrow(VxSession* session, const char* plan_json, unsigned char** data_out, unsigned long long* len_out);
const char* vx_last_error();
void vx_string_free(char* value);
void vx_bytes_free(unsigned char* value);
```

### Return codes

- `0`: success, `result_out` contains a TSV payload (`header\nrow1\nrow2...`).
- `1`: invalid argument.
- `2`: execution error; `vx_last_error()` provides a message.

Arrow IPC output (`vx_plan_execute_arrow`) returns a stream formatted payload. The caller owns the returned buffer and must free it with `vx_bytes_free`.

## Plan IR (MVP)

The Rust adapter currently emits a JSON-shaped string for the following nodes:

- `TableScan`
- `IndexScan`
- `Filter`
- `Projection`
- `Limit`
- `Sort`
- `TopN`
- `Aggregate`
- `Distinct`
- `Join`
- `Derived`

This IR is intentionally minimal; the C++ side should parse it and build Velox operators.

## Next Steps

1) Replace the stub `vx_plan_execute` with a Velox PlanBuilder implementation.
2) Return results via Arrow or TSV (TSV is used for MVP).
3) Expand type handling and expression translation.

## TODO

- Evaluate Arrow C Data Interface (zero-copy) output path once IPC demo stabilizes.
