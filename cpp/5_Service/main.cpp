// cpp/service/main.cpp
#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "httplib.h"
#include <nlohmann/json.hpp>

#include "service.h"
#include "l5/result.h"

#include "l5/manifest.h"
#include "l5/format.h"
#include "l5/compactor.h"
#include "extractor.h"
#include "text_common.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

static std::mutex g_admin_mu;

// -------------------- helpers --------------------

static void reply_json(httplib::Response& res, int status, const json& j) {
  res.status = status;
  res.set_content(j.dump(), "application/json; charset=utf-8");
}

static std::string to_lower_copy(std::string s) {
  for (char& c : s) c = (char)std::tolower((unsigned char)c);
  return s;
}

static bool parse_bool_str(const std::string& v, bool defv) {
  std::string s = to_lower_copy(v);
  if (s == "1" || s == "true" || s == "yes" || s == "on") return true;
  if (s == "0" || s == "false" || s == "no" || s == "off") return false;
  return defv;
}

static bool parse_bool_json(const json& j, const char* key, bool defv) {
  if (!j.contains(key)) return defv;
  const auto& v = j[key];
  if (v.is_boolean()) return v.get<bool>();
  if (v.is_number_integer()) return v.get<int>() != 0;
  if (v.is_string()) return parse_bool_str(v.get<std::string>(), defv);
  return defv;
}

static std::string parse_org_id_str(const json& j) {
  if (!j.contains("organization_id")) return "";
  const auto& v = j["organization_id"];
  if (v.is_number_integer()) return std::to_string(v.get<int64_t>());
  if (v.is_string()) return v.get<std::string>();
  return "";
}

static int64_t safe_stoll(const std::string& s) {
  try {
    size_t pos = 0;
    long long x = std::stoll(s, &pos);
    if (pos != s.size()) return 0;
    return (int64_t)x;
  } catch (...) {
    return 0;
  }
}

static bool is_safe_segment_name(const std::string& s) {
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

static bool is_safe_org_id(const std::string& org) {
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

static void ensure_dirs(const fs::path& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  if (ec) throw std::runtime_error("mkdir failed: " + p.string() + " err=" + ec.message());
}

static std::string shard_dir_name(unsigned shard) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "s%02u", shard);
  return std::string(buf);
}

static unsigned env_u32(const char* k, unsigned defv) {
  const char* s = std::getenv(k);
  if (!s || !*s) return defv;
  char* end = nullptr;
  unsigned long v = std::strtoul(s, &end, 10);
  if (!end || *end != '\0') return defv;
  return (unsigned)v;
}

static unsigned l5_shards_count_guess() {
  unsigned n = env_u32("PLAGIO_L5_SHARDS", 32u);
  if (n < 1) n = 1;
  if (n > 256) n = 256;
  return n;
}

static bool write_text_file_atomic_best_effort(const fs::path& path, const std::string& content) {
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

static std::string read_text_file(const fs::path& path, std::string* err) {
  std::ifstream in(path, std::ios::binary);
  if (!in) {
    if (err) *err = "cannot open " + path.string();
    return {};
  }
  std::string s((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  return s;
}

// Write manifest JSON compatible with l5::load_manifest
static bool write_manifest_file(const fs::path& out_root, const l5::Manifest& m) {
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

static json manifest_to_json(const l5::Manifest& m) {
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

// Parse docids.json (new format objects or old strings) and return first `limit` items
static json read_docids_preview(const fs::path& docids_json, size_t limit) {
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

static bool is_safe_upload_name(const std::string& name) {
  if (name.empty()) return false;
  if (name.size() > 512) return false;
  if (name.find('\0') != std::string::npos) return false;
  if (name.find("..") != std::string::npos) return false;
  if (name.find('/') != std::string::npos) return false;
  if (name.find('\\') != std::string::npos) return false;
  return true;
}

static std::string lower_ext(const fs::path& p) {
  std::string ext = p.extension().string();
  for (auto& c : ext) c = (char)std::tolower((unsigned char)c);
  return ext;
}

static std::string shell_quote(const std::string& s) {
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

static int run_cmd_bash(const std::string& cmd) {
  const std::string full = "bash -lc " + shell_quote(cmd);
  return std::system(full.c_str());
}

static fs::path mk_tmp_dir_simple(const std::string& prefix) {
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

static fs::path replace_ext_txt(fs::path p) {
  p.replace_extension(".txt");
  return p;
}

static std::string to_text_utf8_best_effort_from_path(const fs::path& file_path,
                                                      size_t max_bytes) {
  // extractor.h поддерживает .txt (и CP1251 fallback)
  ExtractedText ex = extract_text_from_file(file_path, /*assume_normalized=*/false, max_bytes);
  return ex.text;
}


// -------------------- main --------------------

int main(int argc, char** argv) {
  std::string data_root = (argc >= 2) ? argv[1] : "./DATA_ROOT";
  L5Service svc{fs::path(data_root)};

  httplib::Server app;

  constexpr size_t MAX_JSON_BODY_BYTES   = 4ull * 1024 * 1024;
  constexpr size_t MAX_TEXT_BYTES        = 2ull * 1024 * 1024;
  constexpr size_t MAX_QUERY_BYTES       = 2ull * 1024 * 1024;
  constexpr size_t MAX_ZIP_UPLOAD_BYTES  = 512ull * 1024 * 1024; // httplib keeps multipart in RAM

  // --------------------
  // form-data: file=@base.zip
  // --------------------
  app.Post(R"(/v1/orgs/([^/]+)/l5/ingest_zip)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      if (!req.is_multipart_form_data()) {
        reply_json(res, 400, {{"error","expected multipart/form-data"}});
        return;
      }

      auto it = req.files.find("file");
      if (it == req.files.end()) {
        reply_json(res, 400, {{"error","missing file field (file=@zip)"}});
        return;
      }

      const auto& f = it->second;
      if (f.content.size() > MAX_ZIP_UPLOAD_BYTES) {
        reply_json(res, 413, {{"error","zip too large"}, {"max_bytes",(uint64_t)MAX_ZIP_UPLOAD_BYTES}});
        return;
      }

      std::optional<unsigned> shard;
      if (req.has_param("l5_shard")) shard = (unsigned)std::stoul(req.get_param_value("l5_shard"));

      std::string segment_name;
      if (req.has_param("segment_name")) segment_name = req.get_param_value("segment_name");
      
      if (!is_safe_segment_name(segment_name)) {
          reply_json(res, 400, {{"error","bad segment_name"}});
        return;
      }
      
      bool normalize = true; // default: normalize in core
      if (req.has_param("normalize")) {
        normalize = parse_bool_str(req.get_param_value("normalize"), true);
      }

      auto r = svc.ingest_l5_zip_build_segment(org_id, f.filename, f.content, shard, segment_name, normalize);
 
      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"zip_name", f.filename},
        {"shard", r.shard},
        {"normalize", normalize ? 1 : 0},
        {"files_seen", r.files_seen},
        {"files_skipped", r.files_skipped},
        {"docs_indexed", r.docs_indexed},
        {"segment_name", r.build.segment_name},
        {"seg_dir", r.build.seg_dir.string()},
        {"docs", r.build.docs},
        {"post9", r.build.post9},
        {"built_at_utc", r.build.built_at_utc}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

    // --------------------
  // DEBUG: raw / normalized text by uploaded file name
  // GET /v1/orgs/{org}/debug/normalized_text?name=...&normalize=1&max_bytes=1000
  //
  // Reads from: DATA_ROOT/orgs/{org}/uploads/{name}
  // Supports: .txt, .doc, .docx (doc/docx via soffice->txt temp)
  // --------------------
  app.Get(R"(/v1/orgs/([^/]+)/debug/normalized_text)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      if (!req.has_param("name")) {
        reply_json(res, 400, {{"error","missing name param"}});
        return;
      }
      const std::string name = req.get_param_value("name");
      if (!is_safe_upload_name(name)) {
        reply_json(res, 400, {{"error","bad name"}});
        return;
      }

      bool normalize = true;
      if (req.has_param("normalize")) {
        const std::string v = to_lower_copy(req.get_param_value("normalize"));
        if (v == "1" || v == "true" || v == "yes" || v == "on") normalize = true;
        else if (v == "0" || v == "false" || v == "no" || v == "off") normalize = false;
        else { reply_json(res, 400, {{"error","bad normalize param"}}); return; }
      }

      size_t max_bytes = 1000;
      if (req.has_param("max_bytes")) {
        try { max_bytes = (size_t)std::stoull(req.get_param_value("max_bytes")); }
        catch (...) { reply_json(res, 400, {{"error","bad max_bytes"}}); return; }
      }
      // hard cap for debug
      const size_t HARD_CAP = 8ull * 1024 * 1024; // 8 MiB
      if (max_bytes == 0 || max_bytes > HARD_CAP) max_bytes = HARD_CAP;

      const fs::path uploads_dir = fs::path(data_root) / "orgs" / org_id / "uploads";
      const fs::path src_path = uploads_dir / name;

      std::error_code ec;
      if (!fs::exists(src_path, ec) || ec) {
        reply_json(res, 404, {{"error","file not found"}, {"path", src_path.string()}});
        return;
      }

      const std::string ext = lower_ext(src_path);

      std::string raw_text;
      fs::path used_path = src_path;
      bool converted = false;

      if (ext == ".txt") {
        raw_text = to_text_utf8_best_effort_from_path(src_path, max_bytes);
      } else if (ext == ".doc" || ext == ".docx") {
        // convert via soffice -> tmp txt
        const fs::path tmp = mk_tmp_dir_simple("l5_debug_norm");
        const fs::path out_dir = tmp / "out";
        const fs::path profile_dir = tmp / "lo_profile";
        ensure_dirs(out_dir);
        ensure_dirs(profile_dir);

        const fs::path abs_profile = fs::absolute(profile_dir);
        const std::string profile_uri = "file://" + abs_profile.string();

        const std::string cmd =
          "soffice --headless --nologo --nolockcheck --nodefault --norestore"
          " -env:UserInstallation=" + shell_quote(profile_uri) +
          " --convert-to " + shell_quote("txt:Text (encoded):UTF8") +
          " --outdir " + shell_quote(out_dir.string()) + " " +
          shell_quote(src_path.string());

        const int rc = run_cmd_bash(cmd);
        if (rc != 0) {
          std::error_code ec2;
          fs::remove_all(tmp, ec2);
          reply_json(res, 500, {{"error","soffice convert failed"}, {"rc", rc}});
          return;
        }

        const fs::path out_txt = out_dir / replace_ext_txt(src_path.filename());
        if (!fs::exists(out_txt, ec)) {
          std::error_code ec2;
          fs::remove_all(tmp, ec2);
          reply_json(res, 500, {{"error","converted txt not found"}, {"path", out_txt.string()}});
          return;
        }

        used_path = out_txt;
        converted = true;

        raw_text = to_text_utf8_best_effort_from_path(out_txt, max_bytes);

        std::error_code ec2;
        fs::remove_all(tmp, ec2);
      } else {
        reply_json(res, 400, {{"error","unsupported file type"}, {"ext", ext}});
        return;
      }

      std::string out_text;
      if (normalize) normalize_for_shingles_simple_to(raw_text, out_text);
      else out_text = std::move(raw_text);

      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"name", name},
        {"path", src_path.string()},
        {"used_path", used_path.string()},
        {"converted", converted ? 1 : 0},
        {"normalize", normalize ? 1 : 0},
        {"max_bytes", (uint64_t)max_bytes},
        {"text", out_text}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });


  // --------------------
  // L5 rebuild to one shard + compact to one index (manual; future worker)
  //
  // POST /v1/orgs/{org}/l5/rebuild_one?out_shard=0&fanout=20&compact=1
  //
  // Behavior:
  // 1) move all segments from all shards into out_shard
  // 2) (optional) compact inside out_shard until stable
  // 3) write rebuild report to index/l5/rebuild_last.json
  // --------------------
  app.Post(R"(/v1/orgs/([^/]+)/l5/rebuild_one)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      unsigned out_shard = 0;
      if (req.has_param("out_shard")) out_shard = (unsigned)std::stoul(req.get_param_value("out_shard"));

      unsigned fanout = 20;
      if (req.has_param("fanout")) fanout = (unsigned)std::stoul(req.get_param_value("fanout"));
      if (fanout < 2) fanout = 2;
      if (fanout > 200) fanout = 200;

      bool do_compact = true;
      if (req.has_param("compact")) do_compact = parse_bool_str(req.get_param_value("compact"), true);

      std::lock_guard<std::mutex> lk(g_admin_mu); // serialize with wipe/rebuild

      const fs::path index_root = fs::path(data_root) / "orgs" / org_id / "index";
      const fs::path l5_root = index_root / "l5";
      ensure_dirs(l5_root);

      const unsigned nshards = l5_shards_count_guess();
      if (out_shard >= nshards) out_shard = 0;

      const fs::path out_root = l5_root / shard_dir_name(out_shard);
      ensure_dirs(out_root);

      // load out manifest
      l5::Manifest outm = l5::load_manifest(out_root);
      std::unordered_set<std::string> out_names;
      out_names.reserve(outm.segments.size() * 2 + 16);
      for (const auto& s : outm.segments) out_names.insert(s.segment_name);

      json before = json::object();
      json moved = json::array();

      for (unsigned sh = 0; sh < nshards; ++sh) {
        const fs::path shard_root = l5_root / shard_dir_name(sh);
        std::error_code ec;
        if (!fs::exists(shard_root, ec) || ec) { ec.clear(); continue; }

        l5::Manifest m = l5::load_manifest(shard_root);
        before[shard_dir_name(sh)] = (uint64_t)m.segments.size();

        if (sh == out_shard) continue;

        // move all segments from this shard into out_shard
        for (const auto& se : m.segments) {
          const fs::path src_dir = shard_root / se.segment_name;
          const fs::path dst_dir = out_root / se.segment_name;

          if (!fs::exists(src_dir, ec) || ec) { ec.clear(); continue; }
          if (fs::exists(dst_dir, ec) && !ec) {
            throw std::runtime_error("segment name collision in out_shard: " + se.segment_name);
          }
          ec.clear();

          fs::rename(src_dir, dst_dir, ec);
          if (ec) {
            throw std::runtime_error("rename failed: " + src_dir.string() + " -> " + dst_dir.string() + " err=" + ec.message());
          }

          moved.push_back({
            {"from_shard", shard_dir_name(sh)},
            {"segment_name", se.segment_name},
            {"docs", se.stats.docs},
            {"k9", se.stats.k9}
          });

          if (out_names.insert(se.segment_name).second) {
            l5::SegmentEntry add = se;
            add.path = se.segment_name + "/";
            outm.segments.push_back(std::move(add));
          }
        }

        // clear moved shard manifest
        l5::Manifest empty;
        (void)write_manifest_file(shard_root, empty);
      }

      // write out_shard manifest (with moved segments)
      if (!write_manifest_file(out_root, outm)) {
        throw std::runtime_error("failed writing out_shard manifest");
      }

      uint32_t merges = 0;
      json compacted = json::array();

      if (do_compact) {
        l5::CompactOptions cop;
        cop.fanout = fanout;

        for (int guard = 0; guard < 100000; ++guard) {
          auto rr = l5::compact_once(out_root, out_root, cop);
          if (!rr.did_compact) break;
          merges += 1;
          compacted.push_back(rr.new_segment_name);
        }
      }

      const l5::Manifest fin = l5::load_manifest(out_root);

      json report = {
        {"org_id", org_id},
        {"out_shard", out_shard},
        {"out_root", out_root.string()},
        {"before_segments", before},
        {"moved_segments", moved},
        {"compaction_fanout", fanout},
        {"compaction_enabled", do_compact ? 1 : 0},
        {"compaction_merges", merges},
        {"new_segments", compacted},
        {"final_segments", manifest_to_json(fin)},
        {"built_at_utc", l5::utc_now_compact()},
        {"processed_at", L5Service::utc_now_iso_utc()}
      };

      (void)write_text_file_atomic_best_effort(l5_root / "rebuild_last.json", report.dump(2));

      reply_json(res, 200, report);
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // --------------------
  // L5 rebuild info
  // GET /v1/orgs/{org}/l5/rebuild_info
  // --------------------
  app.Get(R"(/v1/orgs/([^/]+)/l5/rebuild_info)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      const fs::path l5_root = fs::path(data_root) / "orgs" / org_id / "index" / "l5";
      const fs::path p = l5_root / "rebuild_last.json";

      std::string err;
      std::string txt = read_text_file(p, &err);
      if (txt.empty()) { reply_json(res, 404, {{"error", err}, {"path", p.string()}}); return; }

      json j;
      try { j = json::parse(txt); }
      catch (...) { reply_json(res, 500, {{"error","failed parsing rebuild_last.json"}}); return; }

      // also attach current shard segment counts
      const unsigned nshards = l5_shards_count_guess();
      json counts = json::object();
      for (unsigned sh = 0; sh < nshards; ++sh) {
        const fs::path shard_root = l5_root / shard_dir_name(sh);
        std::error_code ec;
        if (!fs::exists(shard_root, ec) || ec) { ec.clear(); continue; }
        l5::Manifest m = l5::load_manifest(shard_root);
        counts[shard_dir_name(sh)] = (uint64_t)m.segments.size();
      }
      j["current_shard_segments"] = counts;

      reply_json(res, 200, j);
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // --------------------
  // L5 segment docids preview (show files)
  // GET /v1/orgs/{org}/l5/segment_docs?shard=0&segment=seg_xxx&limit=100
  // --------------------
  app.Get(R"(/v1/orgs/([^/]+)/l5/segment_docs)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      if (!req.has_param("segment")) {
        reply_json(res, 400, {{"error","missing segment param"}});
        return;
      }

      unsigned shard = 0;
      if (req.has_param("shard")) shard = (unsigned)std::stoul(req.get_param_value("shard"));

      size_t limit = 100;
      if (req.has_param("limit")) {
        try { limit = (size_t)std::stoull(req.get_param_value("limit")); } catch (...) {}
      }
      if (limit < 1) limit = 1;
      if (limit > 5000) limit = 5000;

      const std::string segment = req.get_param_value("segment");

      const fs::path seg_dir = fs::path(data_root) / "orgs" / org_id / "index" / "l5" / shard_dir_name(shard) / segment;
      const fs::path docids = seg_dir / "index_native_docids.json";
      const fs::path meta   = seg_dir / "index_native_meta.json";

      std::error_code ec;
      if (!fs::exists(docids, ec) || ec) {
        reply_json(res, 404, {{"error","docids not found"}, {"path", docids.string()}});
        return;
      }

      json docs = read_docids_preview(docids, limit);

      json meta_j = json::object();
      if (fs::exists(meta, ec) && !ec) {
        std::ifstream in(meta);
        if (in) {
          try { in >> meta_j; } catch (...) { meta_j = json::object(); }
        }
      }

      reply_json(res, 200, {
        {"org_id", org_id},
        {"shard", shard},
        {"segment", segment},
        {"seg_dir", seg_dir.string()},
        {"limit", (uint64_t)limit},
        {"meta", meta_j},
        {"docids_preview", docs}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // --------------------
  // MAIN: index/search (L1-L4 + L5)
  // POST /v1/process  JSON
  // --------------------
  app.Post(R"(/v1/process)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      constexpr unsigned SMALL_FANOUT = 20;

      if (req.body.size() > MAX_JSON_BODY_BYTES) {
        reply_json(res, 413, {{"error","json body too large"}, {"max_bytes", (uint64_t)MAX_JSON_BODY_BYTES}});
        return;
      }

      json j;
      try { j = json::parse(req.body); }
      catch (...) { reply_json(res, 400, {{"error","invalid json"}}); return; }

      const std::string org_id = parse_org_id_str(j);
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad organization_id"}}); return; }

      const std::string document_id = j.value("document_id", "");
      if (document_id.empty()) { reply_json(res, 400, {{"error","document_id is required"}}); return; }

      const std::string file_name = j.value("file_name", "");
      if (file_name.empty()) { reply_json(res, 400, {{"error","file_name is required"}}); return; }

      const std::string title = j.value("title", "");
      const std::string author = j.value("author", "");
      const std::string created_at = j.value("created_at", "");

      const std::string text = j.value("text", "");
      if (text.empty()) { reply_json(res, 400, {{"error","text is required"}}); return; }
      if (text.size() > MAX_TEXT_BYTES) {
        reply_json(res, 413, {{"error","text too large"}, {"max_bytes", (uint64_t)MAX_TEXT_BYTES}});
        return;
      }

      const bool do_index  = parse_bool_json(j, "do_index", false);
      const bool do_search = parse_bool_json(j, "do_search", true);

      if (!do_index && !do_search) {
        reply_json(res, 400, {{"error","nothing to do: both do_index and do_search are false"}});
        return;
      }

      std::optional<IndexTextResult> indexed;
      if (do_index) {
        indexed = svc.index_text_document(org_id, document_id, file_name, title, author, created_at, text);
        (void)svc.compact_small_levels(org_id, SMALL_FANOUT);
      }

      if (!do_search) {
        json out = {
          {"document_id", document_id},
          {"status", "indexed"},
          {"processed_at", L5Service::utc_now_iso_utc()},
          {"indexed", do_index ? 1 : 0},
          {"indexed_internal_id", indexed.has_value() ? json(std::to_string(indexed->doc.id)) : json(nullptr)},
          {"last_segment", indexed.has_value() ? json(indexed->doc.last_segment) : json(nullptr)}
        };
        reply_json(res, 200, out);
        return;
      }

      if (text.size() > MAX_QUERY_BYTES) {
        reply_json(res, 413, {{"error","query too large"}, {"max_bytes",(uint64_t)MAX_QUERY_BYTES}});
        return;
      }

      l5::SearchOptions opt;
      opt.topk = 20;
      opt.candidates_topn = 2000;
      opt.min_hits = 1;
      opt.span_min_len = 1;
      opt.span_gap = 5;
      opt.max_postings_per_hash = 2000000;
      opt.alpha = 0.60;

      // L1-L4: core normalizes query (same as L5)
      auto r_small = svc.search_levels(org_id, text, /*query_is_normalized=*/false,
                                        std::vector<int>{1,2,3,4}, std::vector<unsigned>{}, opt);
      // L5: core normalizes query
      auto r_l5 = svc.search_levels(org_id, text, /*query_is_normalized=*/false,
                                    std::vector<int>{5}, std::vector<unsigned>{}, opt);

      // merge best by doc_id
      l5::SearchResult r;
      r.query = text;
      r.segments_scanned = r_small.segments_scanned + r_l5.segments_scanned;

      std::unordered_map<std::string, l5::Hit> best;
      best.reserve(r_small.hits.size() + r_l5.hits.size());

      auto push_hits = [&](std::vector<l5::Hit>&& hits){
        for (auto& h : hits) {
          auto it = best.find(h.doc_id);
          if (it == best.end() || h.C > it->second.C) best[h.doc_id] = std::move(h);
        }
      };
      push_hits(std::move(r_small.hits));
      push_hits(std::move(r_l5.hits));

      r.hits.reserve(best.size());
      for (auto& kv : best) r.hits.push_back(std::move(kv.second));

      std::sort(r.hits.begin(), r.hits.end(), [](const l5::Hit& a, const l5::Hit& b){
        return a.C > b.C;
      });
      if (r.hits.size() > opt.topk) r.hits.resize(opt.topk);

      // remove self-match only when this request is "processing a doc" (do_index)
      std::vector<l5::Hit> hits;
      hits.reserve(r.hits.size());
      for (auto& h : r.hits) {
        if (do_index && !h.external_id.empty() && h.external_id == document_id) continue;
        hits.push_back(std::move(h));
      }

      int plagiarism = 0;
      for (const auto& h : hits) {
        int c = (int)(h.C + 0.5);
        if (c > plagiarism) plagiarism = c;
      }

      std::vector<int64_t> ids;
      ids.reserve(hits.size());
      for (const auto& h : hits) {
        int64_t id = safe_stoll(h.doc_id);
        if (id > 0) ids.push_back(id);
      }

      auto docs = svc.get_docs_by_internal_ids(org_id, ids);
      std::unordered_map<int64_t, DocRow> by_id;
      by_id.reserve(docs.size() * 2);
      for (auto& d : docs) by_id[d.id] = std::move(d);

      std::unordered_map<std::string, std::string> module_ids;
      std::vector<json> modules;
      modules.reserve(64);
      int mod_seq = 1;

      auto get_module_id = [&](const std::string& name) -> std::string {
        auto it = module_ids.find(name);
        if (it != module_ids.end()) return it->second;
        std::string id = std::to_string(mod_seq++);
        module_ids[name] = id;
        modules.push_back({{"id", id}, {"module_name", name}});
        return id;
      };

      std::vector<json> sources;
      std::vector<json> matchsources;
      sources.reserve(hits.size());
      matchsources.reserve(hits.size());

      int ms_seq = 1;
      const int q_limit = (int)text.size();

      for (const auto& h : hits) {
        int64_t iid = safe_stoll(h.doc_id);
        const DocRow* meta = nullptr;
        auto it = by_id.find(iid);
        if (it != by_id.end()) meta = &it->second;

        const std::string name = (meta && !meta->title.empty()) ? meta->title : h.source_name;
        const std::string auth = (meta && !meta->author.empty()) ? meta->author : std::string("Нет автора");
        const std::string idx_date = (meta && !meta->stored_at_utc.empty()) ? meta->stored_at_utc : std::string("");

        const std::string mod_id =
            h.meta_path.empty() ? get_module_id("unknown") : get_module_id(h.meta_path);

        sources.push_back({
          {"id", h.doc_id},
          {"source_id", h.external_id},
          {"module_id", mod_id},
          {"name", name},
          {"url", nullptr},
          {"author", auth},
          {"index_date", idx_date}
        });

        matchsources.push_back({
          {"id", std::to_string(ms_seq++)},
          {"source_id", h.doc_id},
          {"q_offset", 0},
          {"q_limit", q_limit},
          {"s_offset", 0},
          {"s_limit", q_limit},
          {"type", "1"}
        });
      }

      json out = {
        {"document_id", document_id},
        {"status", "completed"},
        {"processed_at", L5Service::utc_now_iso_utc()},
        {"plagiarism_percentage", plagiarism},
        {"selfcite_percentage", 0},
        {"legal_percentage", 0},
        {"unknown_percentage", 0},
        {"sources", sources},
        {"matchsources", matchsources},
        {"modules", modules}
      };

      reply_json(res, 200, out);
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // manual compaction
  app.Post(R"(/v1/orgs/([^/]+)/admin/compact_small)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      unsigned fanout = 20;
      if (req.has_param("fanout")) fanout = (unsigned)std::stoul(req.get_param_value("fanout"));
      if (fanout < 2) fanout = 2;
      auto rep = svc.compact_small_levels(org_id, fanout);
      reply_json(res, 200, {{"ok", true}, {"org_id", org_id}, {"fanout", fanout}, {"rounds", rep.rounds}, {"merges", rep.merges}, {"new_segments", rep.new_segments}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  app.Post(R"(/v1/orgs/([^/]+)/admin/compact_l5)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      unsigned fanout = 2;
      if (req.has_param("fanout")) fanout = (unsigned)std::stoul(req.get_param_value("fanout"));
      if (fanout < 2) fanout = 2;
      auto rep = svc.compact_l5_shards(org_id, fanout);
      reply_json(res, 200, {{"ok", true}, {"org_id", org_id}, {"fanout", fanout}, {"rounds", rep.rounds}, {"merges", rep.merges}, {"new_segments", rep.new_segments}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // wipe all
  app.Post(R"(/v1/admin/wipe_all)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string confirm;
      if (req.has_param("confirm")) confirm = req.get_param_value("confirm");
      if (confirm != "WIPE_ALL") { reply_json(res, 400, {{"error","confirm required"},{"expected","WIPE_ALL"}}); return; }

      std::lock_guard<std::mutex> lk(g_admin_mu);

      const fs::path orgs_dir = svc.data_root() / "orgs";
      std::error_code ec;
      uintmax_t removed = 0;

      if (fs::exists(orgs_dir, ec)) {
        ec.clear();
        removed = fs::remove_all(orgs_dir, ec);
        if (ec) { reply_json(res, 500, {{"error","remove_all failed"},{"detail",ec.message()}}); return; }
      }

      ec.clear();
      fs::create_directories(orgs_dir, ec);
      if (ec) { reply_json(res, 500, {{"error","create_directories failed"},{"detail",ec.message()}}); return; }

      reply_json(res, 200, {{"ok", true}, {"removed_entries", (uint64_t)removed}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  const char* host = "0.0.0.0";
  int port = 8088;
  std::cout << "L5 service data_root=" << data_root << " listen " << host << ":" << port << "\n";
  app.listen(host, port);
  return 0;
}
