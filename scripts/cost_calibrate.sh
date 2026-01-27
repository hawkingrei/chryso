#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: cost_calibrate.sh <queries_dir> <output_dir> [dialect]"
  exit 1
fi

queries_dir="$1"
output_dir="$2"
dialect="${3:-postgres}"

mkdir -p "$output_dir"
out_file="$output_dir/costs.csv"

cargo run --bin chryso-tune -- --queries "$queries_dir" --dialect "$dialect" --out "$out_file"
echo "wrote $out_file"
