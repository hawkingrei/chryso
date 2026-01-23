# Chryso Agents Notes

## Project Idea
- Build a Calcite-style parser + optimizer engine in Rust, with a stable core API.
- Support PostgreSQL + MySQL dialects first; aim for broad AST compatibility.
- Implement a full Cascades optimizer: logical rules, physical rules, cost-based search.
- Collect statistics via ANALYZE for cost modeling.
- Translate logical/physical plans into execution engine APIs.
- Start with DuckDB adapter; keep architecture open for Velox and others.

## Architecture Decisions (Early)
- Pipeline: SQL -> AST -> Logical Plan -> Cascades Optimizer -> Physical Plan -> Executor Adapter.
- Keep optimizer engine-agnostic; adapters handle engine-specific translation.
- Rust-first with a future C ABI boundary for C++/Go/Java bindings.
- Feature-gate heavy adapters (e.g., DuckDB) to keep core lightweight.

## Lessons/Experience So Far
- Start with minimal demo pipeline to validate module boundaries early.
- Keep parser dialect config explicit to avoid cross-dialect leakage.
- Prefer stable public API re-exports in `src/lib.rs` to ease future bindings.
- Provide test helpers that execute the full pipeline and return explain strings for assertions.
- Gate reusable test helpers behind a feature for integration tests.
- Workspace split into core/parser/planner/optimizer/metadata/adapter crates to keep dependencies acyclic.
- Skimmed Calcite/Doris READMEs for module layout cues; keep optimizer/planner boundaries explicit.
- When writing PR bodies or docs, use real line breaks instead of literal `\\n` because GitHub CLI does not render `\\n` as newlines.
