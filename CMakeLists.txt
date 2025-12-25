cmake_minimum_required(VERSION 3.16)
project(l5_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(L5_USE_FETCHCONTENT "Fetch simdjson/nlohmann_json if not found" ON)

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

# -----------------------------
# Library
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
)

# -----------------------------
# Tools
# -----------------------------

add_executable(l5_build cpp/tools/l5_build_main.cpp)
target_link_libraries(l5_build PRIVATE l5_engine)

add_executable(l5_validate cpp/tools/l5_validate_main.cpp)
target_link_libraries(l5_validate PRIVATE l5_engine)

add_executable(l5_search cpp/tools/l5_search_main.cpp)
target_link_libraries(l5_search PRIVATE l5_engine)

# -----------------------------
# Tests
# -----------------------------

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
