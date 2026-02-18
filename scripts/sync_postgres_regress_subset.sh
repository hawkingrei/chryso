#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="${1:-$repo_root/.cache/postgres/src/test/regress/sql}"
manifest="$repo_root/crates/parser/tests/testdata/postgres_regress/files.txt"
dest_dir="$repo_root/crates/parser/tests/testdata/postgres_regress/sql"

mkdir -p "$dest_dir"
rm -f "$dest_dir"/*.sql

while IFS= read -r line; do
  trimmed="${line#"${line%%[![:space:]]*}"}"
  [[ -z "$trimmed" ]] && continue
  [[ "$trimmed" == \#* ]] && continue

  src="$source_dir/$trimmed"
  if [[ ! -f "$src" ]]; then
    echo "missing source file listed in manifest: $src" >&2
    exit 1
  fi

  cp "$src" "$dest_dir/$trimmed"
done < "$manifest"

echo "synced postgres regress subset to $dest_dir"
