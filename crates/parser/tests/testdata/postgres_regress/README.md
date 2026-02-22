# PostgreSQL Regress Subset

This directory vendors a curated subset of upstream PostgreSQL regress SQL files
for parser coverage expansion.

## Source

Default source path in this repository:

`.cache/postgres/src/test/regress/sql`

Selection manifest:

`crates/parser/tests/testdata/postgres_regress/files.txt`

## Sync

Use the helper script to sync files listed in `files.txt`:

```bash
./scripts/sync_postgres_regress_subset.sh
```

Or pass a custom upstream SQL directory:

```bash
./scripts/sync_postgres_regress_subset.sh /path/to/postgres/src/test/regress/sql
```

## Scope

The corresponding test (`tests/postgres_regress_subset.rs`) extracts `SELECT` / `WITH`
statements and tracks parser coverage against this subset.

Some statements are intentionally skipped when current AST shape cannot represent
them yet (for example expression-based LIMIT/OFFSET values).

To validate more than parser acceptance, use the execution subset test
(`tests/postgres_regress_exec_subset.rs`) with `duckdb` feature. That layer runs
curated cases with schema setup, input data inserts, query execution, and expected
result-row assertions for both `SimpleParser` and `YaccParser`.
