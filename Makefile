.PHONY: fmt build build-examples test test-duckdb velox-ffi-config velox-ffi-build velox-ffi-test

VELOX_BUILD_DIR ?= ffi/velox/build
ABS_VELOX_BUILD_DIR := $(abspath $(VELOX_BUILD_DIR))
VELOX_FFI_DIR ?= $(ABS_VELOX_BUILD_DIR)

fmt:
	cargo fmt --all -- --check

build:
	cargo build

build-examples:
	cargo build --examples

test:
	cargo test
	cargo test --features duckdb
	$(MAKE) velox-ffi-test

test-duckdb:
	cargo test --features duckdb

velox-ffi-config:
	cmake -S ffi/velox -B $(ABS_VELOX_BUILD_DIR) \
		-DCHRYSO_VELOX_USE_SUBMODULE=ON \
		-DCHRYSO_VELOX_USE_ARROW=OFF \
		-DCHRYSO_ARROW_STRICT_VERSION=OFF \
		-DCHRYSO_VELOX_BUILD_TESTS=ON \
		-DCHRYSO_VELOX_EXEC_ONLY=ON

velox-ffi-build: velox-ffi-config
	cmake --build $(ABS_VELOX_BUILD_DIR) --parallel

velox-ffi-test: velox-ffi-build
	ctest --test-dir $(ABS_VELOX_BUILD_DIR) -R chryso_velox_ffi_escape_test
	CHRYSO_VELOX_FFI_DIR=$(VELOX_FFI_DIR) \
	DYLD_LIBRARY_PATH=$(VELOX_FFI_DIR):$${DYLD_LIBRARY_PATH} \
	cargo test -p chryso-adapter-velox --features velox-ffi
