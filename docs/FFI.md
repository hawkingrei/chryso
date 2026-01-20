# FFI Stability Notes

## Goals
- Keep core Rust APIs ergonomic while offering stable C ABI surfaces.
- Ensure serialized plan/expr formats remain backward compatible.

## Stability Guidelines
- Expose only opaque handles through C ABI.
- Version all serialized schemas and keep decoding backward compatible.
- Add new fields with defaults; avoid breaking enum renumbering.

## Crate Layout
- `ffi/` holds the C ABI crate.
- `ffi/include/` contains generated C headers (future).
