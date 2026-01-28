cmake_minimum_required(VERSION 3.20)
project(chryso_velox_ffi LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(CHRYSO_VELOX_USE_SUBMODULE "Use vendored Velox submodule" ON)
option(CHRYSO_VELOX_USE_ARROW "Enable Arrow IPC output" ON)
option(CHRYSO_ARROW_STRICT_VERSION "Require Arrow C++ 15.x" OFF)

if(CHRYSO_VELOX_USE_SUBMODULE)
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
