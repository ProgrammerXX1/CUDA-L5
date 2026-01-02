// Back_Last/cpp/5_Service/routes/route_utils.h
#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

#include "httplib.h"
#include <nlohmann/json.hpp>

#include "l5/manifest.h"

namespace fs = std::filesystem;
using json = nlohmann::ordered_json;

// helpers
void reply_json(httplib::Response& res, int status, const json& j);
std::string to_lower_copy(std::string s);
bool parse_bool_str(const std::string& v, bool defv);
bool parse_bool_json(const json& j, const char* key, bool defv);
std::string parse_org_id_str(const json& j);

int64_t safe_stoll(const std::string& s);

bool is_safe_segment_name(const std::string& s);
bool is_safe_org_id(const std::string& org);
bool is_safe_upload_name(const std::string& name);

void ensure_dirs(const fs::path& p);

std::string shard_dir_name(unsigned shard);
unsigned env_u32(const char* k, unsigned defv);
unsigned l5_shards_count_guess();

bool write_text_file_atomic_best_effort(const fs::path& path, const std::string& content);
std::string read_text_file(const fs::path& path, std::string* err);

bool write_manifest_file(const fs::path& out_root, const l5::Manifest& m);
json manifest_to_json(const l5::Manifest& m);

// Parse docids.json and return first `limit` items
json read_docids_preview(const fs::path& docids_json, size_t limit);

std::string lower_ext(const fs::path& p);

// shell helpers (for soffice conversion in debug route)
std::string shell_quote(const std::string& s);
int run_cmd_bash(const std::string& cmd);
fs::path mk_tmp_dir_simple(const std::string& prefix);
fs::path replace_ext_txt(fs::path p);

// text extraction
std::string to_text_utf8_best_effort_from_path(const fs::path& file_path, size_t max_bytes);
