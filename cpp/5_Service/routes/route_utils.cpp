// Back_Last/cpp/5_Service/routes/route_utils.cpp
#include "route_utils.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <random>
#include <system_error>
#include <time.h>
#include <vector>

#include "l5/format.h"
#include "extractor.h"
#include "text_common.h"

void reply_json(httplib::Response& res, int status, const json& j) {
  res.status = status;
  res.set_content(j.dump(), "application/json; charset=utf-8");
}

std::string to_lower_copy(std::string s) {
  for (char& c : s) c = (char)std::tolower((unsigned char)c);
  return s;
}

bool parse_bool_str(const std::string& v, bool defv) {
  std::string s = to_lower_copy(v);
  if (s == "1" || s == "true" || s == "yes" || s == "on") return true;
  if (s == "0" || s == "false" || s == "no" || s == "off") return false;
  return defv;
}

bool parse_bool_json(const json& j, const char* key, bool defv) {
  if (!j.contains(key)) return defv;
  const auto& v = j[key];
  if (v.is_boolean()) return v.get<bool>();
  if (v.is_number_integer()) return v.get<int>() != 0;
  if (v.is_string()) return parse_bool_str(v.get<std::string>(), defv);
  return defv;
}

std::string parse_org_id_str(const json& j) {
  if (!j.contains("organization_id")) return "";
  const auto& v = j["organization_id"];
  if (v.is_number_integer()) return std::to_string(v.get<int64_t>());
  if (v.is_string()) return v.get<std::string>();
  return "";
}

int64_t safe_stoll(const std::string& s) {
  try {
    size_t pos = 0;
    long long x = std::stoll(s, &pos);
    if (pos != s.size()) return 0;
    return (int64_t)x;
  } catch (...) {
    return 0;
  }
}

bool is_safe_segment_name(const std::string& s) {
  if (s.empty()) return true;            // пусто => автоимя
  if (s.size() > 128) return false;
  if (s.find("..") != std::string::npos) return false;
  if (s.find('/') != std::string::npos) return false;
  if (s.find('\\') != std::string::npos) return false;
  for (char c : s) {
    const bool ok = (c >= '0' && c <= '9') ||
                    (c >= 'a' && c <= 'z') ||
                    (c >= 'A' && c <= 'Z') ||
                    (c == '_' || c == '-');
    if (!ok) return false;
  }
  return true;
}

bool is_safe_org_id(const std::string& org) {
  if (org.empty()) return false;
  if (org.find("..") != std::string::npos) return false;
  for (char c : org) {
    const bool ok = (c >= '0' && c <= '9') ||
                    (c >= 'a' && c <= 'z') ||
                    (c >= 'A' && c <= 'Z') ||
                    (c == '_' || c == '-');
    if (!ok) return false;
  }
  return true;
}

bool is_safe_upload_name(const std::string& name) {
  if (name.empty()) return false;
  if (name.size() > 512) return false;
  if (name.find('\0') != std::string::npos) return false;
  if (name.find("..") != std::string::npos) return false;
  if (name.find('/') != std::string::npos) return false;
  if (name.find('\\') != std::string::npos) return false;
  return true;
}

void ensure_dirs(const fs::path& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  if (ec) throw std::runtime_error("mkdir failed: " + p.string() + " err=" + ec.message());
}

std::string shard_dir_name(unsigned shard) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "s%02u", shard);
  return std::string(buf);
}

unsigned env_u32(const char* k, unsigned defv) {
  const char* s = std::getenv(k);
  if (!s || !*s) return defv;
  char* end = nullptr;
  unsigned long v = std::strtoul(s, &end, 10);
  if (!end || *end != '\0') return defv;
  return (unsigned)v;
}

unsigned l5_shards_count_guess() {
  unsigned n = env_u32("PLAGIO_L5_SHARDS", 32u);
  if (n < 1) n = 1;
  if (n > 256) n = 256;
  return n;
}

bool write_text_file_atomic_best_effort(const fs::path& path, const std::string& content) {
  try {
    ensure_dirs(path.parent_path());
    fs::path tmp = path;
    tmp += ".tmp";

    std::ofstream out(tmp, std::ios::binary);
    if (!out) return false;
    out.write(content.data(), (std::streamsize)content.size());
    out.flush();
    if (!out) return false;

    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (!ec) return true;

    fs::remove(path, ec);
    ec.clear();
    fs::rename(tmp, path, ec);
    return !ec;
  } catch (...) {
    return false;
  }
}

std::string read_text_file(const fs::path& path, std::string* err) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    if (err) *err = "cannot open " + path.string();
    return {};
  }
  std::string s((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  return s;
}

// Write manifest JSON compatible with l5::load_manifest
bool write_manifest_file(const fs::path& out_root, const l5::Manifest& m) {
  json j;
  j["segments"] = json::array();
  for (const auto& se : m.segments) {
    json e;
    e["segment_name"] = se.segment_name;
    e["path"] = se.path;
    e["built_at_utc"] = se.built_at_utc;
    e["stats"] = {{"docs", se.stats.docs}, {"k9", se.stats.k9}, {"k13", se.stats.k13}};
    j["segments"].push_back(std::move(e));
  }

  const fs::path fin = out_root / "level5_manifest.json";
  const fs::path tmp = out_root / "level5_manifest.json.tmp";

  ensure_dirs(out_root);

  std::ofstream out(tmp, std::ios::binary);
  if (!out) return false;
  const std::string content = j.dump();
  out.write(content.data(), (std::streamsize)content.size());
  out.flush();
  if (!out) return false;

  return l5::atomic_replace_file_best_effort(tmp, fin);
}

json manifest_to_json(const l5::Manifest& m) {
  json out = json::array();
  for (const auto& se : m.segments) {
    out.push_back({
      {"segment_name", se.segment_name},
      {"path", se.path},
      {"built_at_utc", se.built_at_utc},
      {"stats", {{"docs", se.stats.docs}, {"k9", se.stats.k9}, {"k13", se.stats.k13}}}
    });
  }
  return out;
}

json read_docids_preview(const fs::path& docids_json, size_t limit) {
  std::ifstream in(docids_json);
  if (!in) return json::array();

  json j;
  try { in >> j; } catch (...) { return json::array(); }
  if (!j.is_array()) return json::array();

  json out = json::array();
  const size_t n = std::min(limit, j.size());

  for (size_t i = 0; i < n; ++i) {
    const auto& v = j[i];
    if (v.is_object()) {
      out.push_back({
        {"doc_id", v.value("doc_id", "")},
        {"external_id", v.value("external_id", "")},
        {"source_name", v.value("source_name", "")},
        {"source_path", v.value("source_path", "")},
        {"meta_path", v.value("meta_path", "")}
      });
    } else if (v.is_string()) {
      out.push_back({{"doc_id", v.get<std::string>()}});
    }
  }
  return out;
}

std::string lower_ext(const fs::path& p) {
  std::string ext = p.extension().string();
  for (auto& c : ext) c = (char)std::tolower((unsigned char)c);
  return ext;
}

std::string shell_quote(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 8);
  out.push_back('\'');
  for (char c : s) {
    if (c == '\'') out += "'\\''";
    else out.push_back(c);
  }
  out.push_back('\'');
  return out;
}

int run_cmd_bash(const std::string& cmd) {
  const std::string full = "bash -lc " + shell_quote(cmd);
  return std::system(full.c_str());
}

fs::path mk_tmp_dir_simple(const std::string& prefix) {
  fs::path base = fs::temp_directory_path();

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis;

  for (int i = 0; i < 200; ++i) {
    const uint32_t r = dis(gen);
    fs::path p = base / (prefix + "_" + std::to_string((uint64_t)std::time(nullptr)) + "_" + std::to_string(r));
    std::error_code ec;
    if (fs::create_directories(p, ec) && !ec) return p;
  }
  throw std::runtime_error("cannot create temp dir");
}

fs::path replace_ext_txt(fs::path p) {
  p.replace_extension(".txt");
  return p;
}

std::string to_text_utf8_best_effort_from_path(const fs::path& file_path, size_t max_bytes) {
  // extractor.h поддерживает .txt (и CP1251 fallback)
  ExtractedText ex = extract_text_from_file(file_path, /*assume_normalized=*/false, max_bytes);
  return ex.text;
}
