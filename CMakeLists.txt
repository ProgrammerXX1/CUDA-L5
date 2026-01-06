cmake_minimum_required(VERSION 3.16)
project(l5_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(L5_USE_FETCHCONTENT "Fetch dependencies if not found" ON)
option(L5_BUILD_SERVICE "Build HTTP service" ON)

include(FetchContent)

# -----------------------------
# Dependencies (core)
# -----------------------------
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND AND L5_USE_FETCHCONTENT)
  FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.3
  )
  FetchContent_MakeAvailable(nlohmann_json)
endif()

find_package(simdjson QUIET)
if(NOT simdjson_FOUND AND L5_USE_FETCHCONTENT)
  FetchContent_Declare(
    simdjson
    GIT_REPOSITORY https://github.com/simdjson/simdjson.git
    GIT_TAG v3.10.1
  )
  FetchContent_MakeAvailable(simdjson)
endif()

find_package(Threads REQUIRED)

# Resolve target names (differs by install/fetch)
set(L5_SIMDJSON_TARGET "")
if(TARGET simdjson::simdjson)
  set(L5_SIMDJSON_TARGET simdjson::simdjson)
elseif(TARGET simdjson)
  set(L5_SIMDJSON_TARGET simdjson)
else()
  message(FATAL_ERROR "simdjson target not found (expected simdjson::simdjson or simdjson)")
endif()

set(L5_NLOHMANN_TARGET "")
if(TARGET nlohmann_json::nlohmann_json)
  set(L5_NLOHMANN_TARGET nlohmann_json::nlohmann_json)
else()
  message(FATAL_ERROR "nlohmann_json target not found (expected nlohmann_json::nlohmann_json)")
endif()

# -----------------------------
# Generate build/include/l5/* headers as symlinks to your current layout
# This makes #include "l5/reader.h" etc work without editing sources.
# -----------------------------
set(L5_GEN_INCLUDE_ROOT "${CMAKE_CURRENT_BINARY_DIR}/include")
set(L5_GEN_L5_DIR "${L5_GEN_INCLUDE_ROOT}/l5")
file(MAKE_DIRECTORY "${L5_GEN_L5_DIR}")

function(l5_symlink_header dst_name src_rel)
  set(src "${CMAKE_CURRENT_SOURCE_DIR}/${src_rel}")
  set(dst "${L5_GEN_L5_DIR}/${dst_name}")

  if(NOT EXISTS "${src}")
    message(FATAL_ERROR "Missing header: ${src}")
  endif()

  if(EXISTS "${dst}" OR IS_SYMLINK "${dst}")
    file(REMOVE "${dst}")
  endif()

  execute_process(
    COMMAND ${CMAKE_COMMAND} -E create_symlink "${src}" "${dst}"
    RESULT_VARIABLE rv
  )
  if(NOT rv EQUAL 0)
    message(FATAL_ERROR "Failed to create symlink: ${dst} -> ${src}")
  endif()
endfunction()

# l5/*.h mapping (your current folders)
l5_symlink_header(builder.h        cpp/2_Builder/builder.h)

l5_symlink_header(format.h         cpp/3_Index_IO/format.h)
l5_symlink_header(manifest.h       cpp/3_Index_IO/manifest.h)
l5_symlink_header(reader.h         cpp/3_Index_IO/reader.h)
l5_symlink_header(validator.h      cpp/3_Index_IO/validator.h)
l5_symlink_header(merge.h          cpp/3_Index_IO/merge.h)
l5_symlink_header(compactor.h      cpp/3_Index_IO/compactor.h)
l5_symlink_header(errors.h         cpp/3_Index_IO/errors.h)
l5_symlink_header(docinfo.h        cpp/3_Index_IO/docinfo.h)
l5_symlink_header(tokenmap.h       cpp/3_Index_IO/tokenmap.h)

l5_symlink_header(query.h          cpp/4_Search/query.h)
l5_symlink_header(result.h         cpp/4_Search/result.h)
l5_symlink_header(search_segment.h cpp/4_Search/search_segment.h)
l5_symlink_header(search_multi.h   cpp/4_Search/search_multi.h)

# -----------------------------
# Library: l5_engine
# -----------------------------
add_library(l5_engine
  cpp/1_Text/text_common.cpp
  cpp/1_Text/extractor.cpp

  cpp/2_Builder/builder.cpp

  cpp/3_Index_IO/format.cpp
  cpp/3_Index_IO/manifest.cpp
  cpp/3_Index_IO/reader.cpp
  cpp/3_Index_IO/validator.cpp
  cpp/3_Index_IO/merge.cpp
  cpp/3_Index_IO/compactor.cpp

  cpp/4_Search/query.cpp
  cpp/4_Search/result.cpp
  cpp/4_Search/search_segment.cpp
  cpp/4_Search/search_multi.cpp
  cpp/4_Search/segobj_cache.cpp
)

target_include_directories(l5_engine
  PUBLIC
    ${L5_GEN_INCLUDE_ROOT}                 # provides l5/*.h
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp/1_Text # text_common.h, extractor.h
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp/2_Builder
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp/3_Index_IO
    ${CMAKE_CURRENT_SOURCE_DIR}/cpp/4_Search
)

target_link_libraries(l5_engine
  PUBLIC
    ${L5_NLOHMANN_TARGET}
    ${L5_SIMDJSON_TARGET}
    Threads::Threads
)

# -----------------------------
# Service (HTTP): cpp/5_Service
# NOTE: extractor.cpp is in l5_engine already
# -----------------------------
if(L5_BUILD_SERVICE)
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(LIBZIP REQUIRED libzip)

  find_package(SQLite3 QUIET)
  if(NOT SQLite3_FOUND)
    message(STATUS "SQLite3 not found via find_package. Will try linking to sqlite3 directly.")
  endif()

  FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.15.3
  )
  FetchContent_MakeAvailable(httplib)

  add_library(cpp_httplib INTERFACE)
  target_include_directories(cpp_httplib INTERFACE ${httplib_SOURCE_DIR})

  add_executable(l5_service
  cpp/5_Service/main.cpp
  
  cpp/5_Service/core/job_queue.cpp
  cpp/5_Service/core/hot_cache.cpp

  cpp/5_Service/core/service_common.cpp
  cpp/5_Service/core/service_ingest.cpp
  cpp/5_Service/core/service_search.cpp
  cpp/5_Service/core/service_compact.cpp

  cpp/5_Service/storage.cpp
  cpp/5_Service/tombstone.cpp

  cpp/5_Service/routes/register_all.cpp
  cpp/5_Service/routes/route_utils.cpp
  cpp/5_Service/routes/route_jobs.cpp
  cpp/5_Service/routes/route_ingest_zip.cpp
  cpp/5_Service/routes/route_debug_normalized_text.cpp
  cpp/5_Service/routes/route_rebuild_one.cpp
  cpp/5_Service/routes/route_rebuild_info.cpp
  cpp/5_Service/routes/route_segment_docs.cpp
  cpp/5_Service/routes/route_process.cpp
  cpp/5_Service/routes/route_admin_compact.cpp
  cpp/5_Service/routes/route_admin_wipe_all.cpp
  cpp/5_Service/routes/route_admin_hot_cache.cpp
)

  target_include_directories(l5_service PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/cpp/5_Service
  ${CMAKE_CURRENT_SOURCE_DIR}/cpp/5_Service/routes
  ${CMAKE_CURRENT_SOURCE_DIR}/cpp/5_Service/core
)

  target_link_libraries(l5_service PRIVATE
    l5_engine
    cpp_httplib
    Threads::Threads
  )

  target_include_directories(l5_service PRIVATE ${LIBZIP_INCLUDE_DIRS})
  target_link_libraries(l5_service PRIVATE ${LIBZIP_LIBRARIES})
  target_compile_options(l5_service PRIVATE ${LIBZIP_CFLAGS_OTHER})

  if(SQLite3_FOUND)
    target_link_libraries(l5_service PRIVATE SQLite::SQLite3)
  else()
    target_link_libraries(l5_service PRIVATE sqlite3)
  endif()
endif()
