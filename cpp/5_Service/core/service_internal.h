// Back_Last/cpp/5_Service/core/service_internal.h
#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>

namespace svc_detail {

// constants (были в namespace{} в service.cpp)
constexpr unsigned  BUILD_THREADS_DEFAULT    = 20u;
constexpr uint64_t  SORT_RAM_BYTES_DEFAULT  = (100ull << 30);   // 100 GiB

constexpr size_t    ZIP_MAX_FILES_DEFAULT   = 20000;
constexpr uint64_t  ZIP_MAX_TOTAL_BYTES_DEF = (10ull << 30);    // 10 GiB uncompressed safety cap

constexpr unsigned  CONVERT_PROCS_FALLBACK  = 20u;   // max concurrent soffice procs
constexpr unsigned  CONVERT_BATCH_DEFAULT   = 200u;  // files per soffice invocation
constexpr unsigned  EXTRACT_THREADS_CAP     = 20u;

// helpers (реализация в service_common.cpp)
void ensure_dirs(const std::filesystem::path& p);

unsigned  env_u32(const char* k, unsigned defv);
uint64_t  env_u64(const char* k, uint64_t defv);

std::string to_lower_copy(std::string s);
std::string lower_ext(const std::filesystem::path& p);
std::string basename_of(const std::filesystem::path& p);
std::string shard_dir_name(unsigned shard);

std::string make_unique_segment_name();

size_t utf8_safe_prefix_len(std::string_view s, size_t max_bytes);
void   clip_text_utf8_inplace(std::string& s, size_t max_bytes);

std::string read_text_utf8_best_effort(const std::filesystem::path& p, size_t cap);

} // namespace svc_detail
