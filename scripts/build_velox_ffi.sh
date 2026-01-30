#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: build_velox_ffi.sh <build_dir> [submodule=ON|OFF] [arrow=ON|OFF] [strict_arrow=ON|OFF]"
  exit 1
fi

build_dir="$1"
use_submodule="${2:-OFF}"
use_arrow="${3:-ON}"
strict_arrow="${4:-OFF}"

cmake -S ffi/velox -B "$build_dir" \
  -DCHRYSO_VELOX_USE_SUBMODULE="$use_submodule" \
  -DCHRYSO_VELOX_USE_ARROW="$use_arrow" \
  -DCHRYSO_ARROW_STRICT_VERSION="$strict_arrow"

if [[ -n "${CMAKE_BUILD_PARALLEL_LEVEL:-}" ]]; then
  cmake --build "$build_dir" --parallel "$CMAKE_BUILD_PARALLEL_LEVEL"
else
  cmake --build "$build_dir" --parallel
fi
