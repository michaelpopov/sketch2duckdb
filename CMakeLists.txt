cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME sketch2)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

if(NOT DEFINED ENV{SKETCH2_ROOT} OR "$ENV{SKETCH2_ROOT}" STREQUAL "")
  message(FATAL_ERROR "SKETCH2_ROOT environment variable must be set")
endif()

set(SKETCH2_ROOT $ENV{SKETCH2_ROOT})
include_directories("${SKETCH2_ROOT}/install/include")

find_library(SKETCH2_LIBRARY
  NAMES sketch2
  HINTS "${SKETCH2_ROOT}/install/lib"
  NO_DEFAULT_PATH)

if(NOT SKETCH2_LIBRARY)
  message(FATAL_ERROR "Could not find libsketch2 in ${SKETCH2_ROOT}/install/lib")
endif()

set(EXTENSION_SOURCES
  src/sketch2_bitset_filter.cpp
  src/sketch2_extension.cpp
  src/sketch2_dataset.cpp
  src/sketch2_knn.cpp
  src/sketch2_open.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${SKETCH2_LIBRARY})
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${SKETCH2_LIBRARY})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
