#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="${1:-$repo_root/.cache/postgres/src/test/regress/sql}"
manifest="$repo_root/crates/parser/tests/testdata/postgres_regress/files.txt"
dest_dir="$repo_root/crates/parser/tests/testdata/postgres_regress/sql"

mkdir -p "$dest_dir"
rm -f "$dest_dir"/*.sql

while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  cp "$source_dir/$file" "$dest_dir/$file"
done < "$manifest"

echo "synced postgres regress subset into $dest_dir"
