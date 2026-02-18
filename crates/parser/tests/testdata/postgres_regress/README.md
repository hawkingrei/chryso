# PostgreSQL Regress Subset

This directory vendors a curated subset of SQL files from PostgreSQL regress tests.

## Source

- Upstream local cache path: `.cache/postgres/src/test/regress/sql`
- Selection manifest: `crates/parser/tests/testdata/postgres_regress/files.txt`

## Sync

Run:

```bash
scripts/sync_postgres_regress_subset.sh
```

Optional custom source path:

```bash
scripts/sync_postgres_regress_subset.sh /path/to/postgres/src/test/regress/sql
```

## Scope

- These files are parser corpus inputs, not execution correctness tests.
- Integration test `tests/postgres_regress_subset.rs` extracts `SELECT` / `WITH` statements and reports parser coverage.
- Some statements are intentionally skipped when current AST shape cannot represent them yet (for example expression LIMIT/OFFSET values).
