cmake_minimum_required(VERSION 3.20)
project(chryso_velox_ffi LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(CHRYSO_VELOX_USE_SUBMODULE "Use vendored Velox submodule" ON)
option(CHRYSO_VELOX_USE_ARROW "Enable Arrow IPC output" ON)
option(CHRYSO_ARROW_STRICT_VERSION "Require Arrow C++ 15.x" OFF)
option(CHRYSO_VELOX_BUILD_TESTS "Build Chryso Velox FFI tests" OFF)
option(CHRYSO_VELOX_EXEC_ONLY "Build only Velox exec/expression/aggregates" OFF)

if(CHRYSO_VELOX_USE_SUBMODULE)
  if(CHRYSO_VELOX_EXEC_ONLY)
    set(VELOX_BUILD_TESTING OFF CACHE BOOL "" FORCE)
    set(VELOX_BUILD_TEST_UTILS OFF CACHE BOOL "" FORCE)
    set(VELOX_BUILD_VECTOR_TEST_UTILS OFF CACHE BOOL "" FORCE)
    set(VELOX_BUILD_PYTHON_PACKAGE OFF CACHE BOOL "" FORCE)
    set(VELOX_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    set(VELOX_BUILD_BENCHMARKS_BASIC OFF CACHE BOOL "" FORCE)
    set(VELOX_BUILD_RUNNER OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_EXEC ON CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_EXPRESSION ON CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_AGGREGATES ON CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_PRESTO_FUNCTIONS ON CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_SPARK_FUNCTIONS OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_HIVE_CONNECTOR OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_TPCH_CONNECTOR OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_TPCDS_CONNECTOR OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_S3 OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_GCS OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_ABFS OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_HDFS OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_PARQUET OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_GEO OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_REMOTE_FUNCTIONS OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_DUCKDB OFF CACHE BOOL "" FORCE)
    set(VELOX_ENABLE_PARSE OFF CACHE BOOL "" FORCE)
  endif()
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/velox/CMakeLists.txt")
    message(FATAL_ERROR "Velox submodule not found. Run: git submodule update --init --recursive")
  endif()
  add_subdirectory(velox)
endif()

if(CHRYSO_VELOX_USE_ARROW)
  find_package(Arrow REQUIRED)
  if(CHRYSO_ARROW_STRICT_VERSION)
    if(NOT Arrow_VERSION MATCHES "^15\\.")
      message(FATAL_ERROR "Arrow 15.x is required when CHRYSO_ARROW_STRICT_VERSION=ON")
    endif()
  endif()
endif()

add_library(chryso_velox_ffi SHARED
  src/velox_ffi.cpp
)

target_include_directories(chryso_velox_ffi
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_definitions(chryso_velox_ffi PRIVATE CHRYSO_VELOX_FFI=1)

if(TARGET velox)
  target_link_libraries(chryso_velox_ffi PRIVATE velox)
endif()

if(CHRYSO_VELOX_USE_ARROW)
  target_link_libraries(chryso_velox_ffi PRIVATE Arrow::arrow_shared)
  target_compile_definitions(chryso_velox_ffi PRIVATE CHRYSO_VELOX_ARROW=1)
endif()

if(CHRYSO_VELOX_BUILD_TESTS)
  enable_testing()
  add_executable(chryso_velox_ffi_escape_test
    tests/velox_ffi_escape_test.cpp
  )
  target_link_libraries(chryso_velox_ffi_escape_test PRIVATE chryso_velox_ffi)
  add_test(NAME chryso_velox_ffi_escape_test COMMAND chryso_velox_ffi_escape_test)
endif()
