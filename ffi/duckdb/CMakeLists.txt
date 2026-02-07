cmake_minimum_required(VERSION 3.16)
project(chryso_duckdb_ffi LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(CHRYSO_DUCKDB_USE_SUBMODULE "Use vendored DuckDB submodule" ON)

if(CHRYSO_DUCKDB_USE_SUBMODULE)
  if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/duckdb/CMakeLists.txt")
    message(FATAL_ERROR "DuckDB submodule not found. Run: git submodule update --init --recursive")
  endif()
  set(DUCKDB_BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
  set(DUCKDB_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
  set(DUCKDB_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
  set(DUCKDB_BUILD_LOADABLE_EXTENSION OFF CACHE BOOL "" FORCE)
  add_subdirectory(duckdb)
endif()

add_library(chryso_duckdb_ffi SHARED
  src/chryso_duckdb_ffi.cpp
)

target_include_directories(chryso_duckdb_ffi PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(TARGET duckdb)
  target_include_directories(chryso_duckdb_ffi PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/src/include
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/yyjson/include
  )
  target_link_libraries(chryso_duckdb_ffi PRIVATE duckdb)
endif()
