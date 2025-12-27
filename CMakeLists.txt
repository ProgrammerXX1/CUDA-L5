cmake_minimum_required(VERSION 3.16)
project(l5_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(L5_USE_FETCHCONTENT "Fetch dependencies if not found" ON)
option(L5_BUILD_TOOLS "Build CLI tools (l5_build/l5_search/l5_validate)" ON)
option(L5_BUILD_TESTS "Build tests" ON)
option(L5_BUILD_SERVICE "Build HTTP service" ON)

include(FetchContent)

# -----------------------------
# Dependencies
# -----------------------------

# nlohmann_json
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND AND L5_USE_FETCHCONTENT)
  FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
  )
  FetchContent_MakeAvailable(nlohmann_json)
endif()

# simdjson
find_package(simdjson QUIET)
if(NOT simdjson_FOUND AND L5_USE_FETCHCONTENT)
  FetchContent_Declare(
    simdjson
    GIT_REPOSITORY https://github.com/simdjson/simdjson.git
    GIT_TAG v3.10.1
  )
  FetchContent_MakeAvailable(simdjson)
endif()

# Threads (for std::thread)
find_package(Threads REQUIRED)

# libzip (for zip.h)
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBZIP REQUIRED libzip)

# sqlite3 (prefer system, fallback to FetchContent optional)
# On Linux usually: sudo apt-get install -y libsqlite3-dev
find_package(SQLite3 QUIET)
if(NOT SQLite3_FOUND)
  message(STATUS "SQLite3 not found via find_package. Will try linking to sqlite3 directly.")
endif()

# cpp-httplib (header-only)
# We'll create an interface target so includes are easy.
if(L5_BUILD_SERVICE)
  FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.15.3
  )
  FetchContent_MakeAvailable(httplib)

  add_library(cpp_httplib INTERFACE)
  target_include_directories(cpp_httplib INTERFACE ${httplib_SOURCE_DIR})
  # If you want HTTPS, you need OpenSSL and define CPPHTTPLIB_OPENSSL_SUPPORT.
  # For now - plain HTTP only.
endif()

# -----------------------------
# Library: l5_engine
# -----------------------------

add_library(l5_engine
  cpp/common/text_common.cpp
  cpp/src/format.cpp
  cpp/src/manifest.cpp
  cpp/src/reader.cpp
  cpp/src/validator.cpp
  cpp/src/builder.cpp
  cpp/src/query.cpp
  cpp/src/result.cpp
  cpp/src/search_segment.cpp
  cpp/src/search_multi.cpp
)

target_include_directories(l5_engine
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp/include
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp/common
)

target_link_libraries(l5_engine
  PUBLIC
    nlohmann_json::nlohmann_json
    simdjson::simdjson
    Threads::Threads
)

# -----------------------------
# Tools (CLI)
# -----------------------------

if(L5_BUILD_TOOLS)
  add_executable(l5_build cpp/tools/l5_build_main.cpp)
  target_link_libraries(l5_build PRIVATE l5_engine)

  add_executable(l5_validate cpp/tools/l5_validate_main.cpp)
  target_link_libraries(l5_validate PRIVATE l5_engine)

  add_executable(l5_search cpp/tools/l5_search_main.cpp)
  target_link_libraries(l5_search PRIVATE l5_engine)
endif()

# -----------------------------
# Service (HTTP)
# -----------------------------

if(L5_BUILD_SERVICE)
  # You will create these files:
  #   cpp/service/main.cpp
  #   cpp/service/service.cpp
  #   cpp/service/service.h
  #   cpp/service/storage.cpp
  #   cpp/service/storage.h
  #   cpp/service/tombstone.cpp
  #   cpp/service/tombstone.h
  #   cpp/service/extractor.cpp
  #   cpp/service/extractor.h
  add_executable(l5_service
    cpp/service/main.cpp
    cpp/service/service.cpp
    cpp/service/storage.cpp
    cpp/service/tombstone.cpp
    cpp/service/extractor.cpp
  )

  target_include_directories(l5_service PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp/service
  )

  target_link_libraries(l5_service PRIVATE
    l5_engine
    cpp_httplib
  )

  target_include_directories(l5_service PRIVATE ${LIBZIP_INCLUDE_DIRS})
  target_link_libraries(l5_service PRIVATE ${LIBZIP_LIBRARIES})
  target_compile_options(l5_service PRIVATE ${LIBZIP_CFLAGS_OTHER})

  # sqlite3 link
  if(SQLite3_FOUND)
    target_link_libraries(l5_service PRIVATE SQLite::SQLite3)
  else()
    # fallback: try raw -lsqlite3
    target_link_libraries(l5_service PRIVATE sqlite3)
  endif()

  # Optional: enable large file support on some systems (usually default)
  # target_compile_definitions(l5_service PRIVATE _FILE_OFFSET_BITS=64)

endif()

# -----------------------------
# Tests
# -----------------------------

if(L5_BUILD_TESTS)
  enable_testing()

  set(L5_TEST_DATA_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cpp/tests/data")

  add_executable(test_build_smoke cpp/tests/test_build_smoke.cpp)
  target_link_libraries(test_build_smoke PRIVATE l5_engine)
  target_compile_definitions(test_build_smoke PRIVATE L5_TEST_DATA_DIR="${L5_TEST_DATA_DIR}")
  add_test(NAME test_build_smoke COMMAND test_build_smoke)

  add_executable(test_validate cpp/tests/test_validate.cpp)
  target_link_libraries(test_validate PRIVATE l5_engine)
  target_compile_definitions(test_validate PRIVATE L5_TEST_DATA_DIR="${L5_TEST_DATA_DIR}")
  add_test(NAME test_validate COMMAND test_validate)

  add_executable(test_search_smoke cpp/tests/test_search_smoke.cpp)
  target_link_libraries(test_search_smoke PRIVATE l5_engine)
  target_compile_definitions(test_search_smoke PRIVATE L5_TEST_DATA_DIR="${L5_TEST_DATA_DIR}")
  add_test(NAME test_search_smoke COMMAND test_search_smoke)
endif()
