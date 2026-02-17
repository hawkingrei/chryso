# PostgreSQL Regress Subset for Chryso

This directory stores a curated subset of upstream PostgreSQL regression SQL files
for parser coverage expansion.

## Source

Default source path in this repository:

`.cache/postgres/src/test/regress/sql`

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

The corresponding test (`tests/postgres_regress_subset.rs`) currently extracts
`SELECT`/`WITH` statements and tracks parser coverage against this subset.
To expand coverage, append more SQL files into `files.txt` and rerun sync.
