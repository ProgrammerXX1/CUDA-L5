// src/main.cpp
#include <iostream>
#include <filesystem>
#include <stdexcept>
#include <fstream>
#include <optional>
#include <string>
#include <cstdlib>
#include <cctype>
#include <ctime>
#include <mutex>
#include <algorithm>
#include <cstdint>

#include "httplib.h"
#include <nlohmann/json.hpp>

#include "service.h"
#include "l5/result.h"
#include "l5/format.h"

#include "storage.h"
#include "extractor.h"
#include "text_common.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

static json doc_to_json(const DocRow& d) {
  return {
    {"org_id", d.org_id},
    {"doc_id", d.doc_id},
    {"external_id", d.external_id},
    {"source_path", d.source_path},
    {"source_name", d.source_name},
    {"stored_path", d.stored_path},
    {"preview", d.preview},
    {"created_at_utc", d.created_at_utc},
    {"deleted", d.deleted},
    {"deleted_at_utc", d.deleted_at_utc},
    {"last_segment", d.last_segment},
  };
}

static json upload_to_json(const UploadResult& r) {
  return {
    {"org_id", r.org_id},
    {"doc_id", r.doc_id},
    {"external_id", r.external_id},
    {"source_name", r.source_name},
    {"stored_path", r.stored_path},
    {"bytes", r.bytes}
  };
}

static void reply_json(httplib::Response& res, int status, const json& j) {
  res.status = status;
  res.set_content(j.dump(), "application/json; charset=utf-8");
}

// admin/wipe mutex
static std::mutex g_admin_mu;

// ---- helpers for debug index view ----
static bool read_docmeta_by_did(const fs::path& bin_path,
                                uint32_t did,
                                l5::HeaderV2& hdr_out,
                                l5::DocMeta& dm_out,
                                std::string& err) {
  std::ifstream in(bin_path, std::ios::binary);
  if (!in) { err = "cannot open " + bin_path.string(); return false; }

  l5::HeaderV2 h{};
  if (!l5::read_header_v2(in, h)) { err = "invalid header in " + bin_path.string(); return false; }
  if (did >= h.n_docs) { err = "did out of range"; return false; }

  // header bytes: 4+4+4+8+8 = 28
  constexpr std::streamoff HDR_BYTES = 4 + 4 + 4 + 8 + 8;
  // docmeta bytes: tok_len(4) + hi(8) + lo(8) = 20
  constexpr std::streamoff DOCMETA_BYTES = 4 + 8 + 8;

  const std::streamoff off = HDR_BYTES + (std::streamoff)did * DOCMETA_BYTES;
  in.seekg(off, std::ios::beg);
  if (!in) { err = "seek failed"; return false; }

  l5::DocMeta dm{};
  in.read(reinterpret_cast<char*>(&dm.tok_len), sizeof(dm.tok_len));
  in.read(reinterpret_cast<char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
  in.read(reinterpret_cast<char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
  if (!in) { err = "read docmeta failed"; return false; }

  hdr_out = h;
  dm_out = dm;
  return true;
}

static std::optional<json> find_docinfo_entry_with_did(const fs::path& docids_json,
                                                       const std::string& doc_id,
                                                       uint32_t& did_out,
                                                       std::string& err) {
  std::ifstream in(docids_json);
  if (!in) { err = "cannot open " + docids_json.string(); return std::nullopt; }

  json j;
  try { in >> j; } catch (...) { err = "failed parsing " + docids_json.string(); return std::nullopt; }
  if (!j.is_array()) { err = "docids json is not array"; return std::nullopt; }

  for (size_t i = 0; i < j.size(); ++i) {
    if (!j[i].is_object()) continue;
    const std::string id = j[i].value("doc_id", "");
    if (id == doc_id) {
      did_out = (uint32_t)i;
      return j[i];
    }
  }

  err = "doc_id not found in docids.json";
  return std::nullopt;
}

// ---- helpers for text extraction endpoint ----
static std::string to_lower_copy(std::string s) {
  for (char& c : s) c = (char)std::tolower((unsigned char)c);
  return s;
}

static std::string lower_ext(const fs::path& p) {
  return to_lower_copy(p.extension().string());
}

static void ensure_dirs(const fs::path& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  if (ec) throw std::runtime_error("mkdir failed: " + p.string() + " err=" + ec.message());
}

static std::string shell_quote(const std::string& s) {
  // single-quote safe for bash: ' -> '\''
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
  std::string full = "bash -lc " + shell_quote(cmd);
  return std::system(full.c_str());
}

static fs::path mk_tmp_dir() {
  const auto base = fs::temp_directory_path();
  const uint64_t t = (uint64_t)std::time(nullptr);
  for (int i = 0; i < 200; ++i) {
    fs::path p = base / ("l5_dbg_" + std::to_string(t) + "_" + std::to_string(i));
    std::error_code ec;
    if (fs::create_directories(p, ec) && !ec) return p;
  }
  throw std::runtime_error("cannot create temp dir");
}

struct CleanupDir {
  fs::path p;
  ~CleanupDir() {
    if (p.empty()) return;
    std::error_code ec;
    fs::remove_all(p, ec);
  }
};

// UTF-8 safe prefix boundary
static size_t utf8_safe_prefix_len(std::string_view s, size_t max_bytes) {
  const size_t n = std::min(max_bytes, s.size());
  size_t i = 0;
  size_t last_good = 0;

  while (i < n) {
    const unsigned char c = (unsigned char)s[i];

    size_t len = 1;
    if (c < 0x80) len = 1;
    else if (c >= 0xC2 && c <= 0xDF) len = 2;
    else if (c >= 0xE0 && c <= 0xEF) len = 3;
    else if (c >= 0xF0 && c <= 0xF4) len = 4;
    else break;

    if (i + len > n) break;

    bool ok = true;
    for (size_t j = 1; j < len; ++j) {
      const unsigned char cc = (unsigned char)s[i + j];
      if ((cc & 0xC0) != 0x80) { ok = false; break; }
    }
    if (!ok) break;

    i += len;
    last_good = i;
  }
  return last_good;
}

static fs::path convert_doc_to_txt_utf8(const fs::path& in_file, const fs::path& tmp_dir) {
  // Convert doc/docx to txt via soffice using isolated profile
  const fs::path conv_src = tmp_dir / "conv_src";
  const fs::path conv_out = tmp_dir / "conv_out";
  const fs::path lo_profile = tmp_dir / "lo_profile";
  ensure_dirs(conv_src);
  ensure_dirs(conv_out);
  ensure_dirs(lo_profile);

  const fs::path unique_in = conv_src / in_file.filename();
  {
    std::ifstream in(in_file, std::ios::binary);
    if (!in) throw std::runtime_error("cannot read: " + in_file.string());
    std::ofstream out(unique_in, std::ios::binary);
    if (!out) throw std::runtime_error("cannot write: " + unique_in.string());
    out << in.rdbuf();
    out.flush();
    if (!out) throw std::runtime_error("copy failed: " + unique_in.string());
  }

  const fs::path abs_profile = fs::absolute(lo_profile);
  std::string profile_uri = "file://" + abs_profile.string();

  std::string cmd =
    "soffice --headless --nologo --nolockcheck --nodefault --norestore"
    " -env:UserInstallation=" + shell_quote(profile_uri) +
    " --convert-to " + shell_quote("txt:Text (encoded):UTF8") +
    " --outdir " + shell_quote(conv_out.string()) + " " +
    shell_quote(unique_in.string());

  const int rc = run_cmd_bash(cmd);
  if (rc != 0) throw std::runtime_error("soffice convert failed rc=" + std::to_string(rc));

  const fs::path out_txt = conv_out / (unique_in.stem().string() + ".txt");
  if (!fs::exists(out_txt)) {
    // sometimes soffice changes name; try find any .txt in outdir
    for (auto it = fs::directory_iterator(conv_out); it != fs::directory_iterator(); ++it) {
      if (it->is_regular_file() && lower_ext(it->path()) == ".txt") return it->path();
    }
    throw std::runtime_error("soffice produced no .txt in " + conv_out.string());
  }
  return out_txt;
}

static std::string trim_copy(std::string s) {
  auto is_ws = [](unsigned char c) { return std::isspace(c) != 0; };
  size_t a = 0;
  while (a < s.size() && is_ws((unsigned char)s[a])) ++a;
  size_t b = s.size();
  while (b > a && is_ws((unsigned char)s[b - 1])) --b;
  return s.substr(a, b - a);
}

// parse bool from query/form value
static bool parse_bool_str(const std::string& v, bool defv) {
  std::string s = trim_copy(v);
  if (s.empty()) return defv;
  s = to_lower_copy(s);
  if (s == "1" || s == "true" || s == "yes" || s == "on") return true;
  if (s == "0" || s == "false" || s == "no" || s == "off") return false;
  return defv;
}

// try read param from multipart form or query params
static std::optional<std::string> get_param_any(const httplib::Request& req, const char* key) {
  if (req.has_param(key)) return req.get_param_value(key);
  auto it = req.files.find(key);
  if (it != req.files.end()) {
    // for text fields in multipart, filename is usually empty
    if (it->second.filename.empty()) return it->second.content;
  }
  return std::nullopt;
}

int main(int argc, char** argv) {
  std::string data_root = (argc >= 2) ? argv[1] : "./DATA_ROOT";
  L5Service svc{fs::path(data_root)};

  httplib::Server app;

  // Simple safety limits (всё равно в памяти, но хотя бы режем DoS)
  constexpr size_t MAX_ZIP_UPLOAD_BYTES = 512ull * 1024 * 1024; // 512MB
  constexpr size_t MAX_JSON_BODY_BYTES  = 1ull * 1024 * 1024;   // 1MB
  constexpr size_t MAX_QUERY_BYTES      = 256ull * 1024;        // 256KB

  // Debug endpoint hard cap
  constexpr size_t MAX_DEBUG_TEXT_BYTES_DEFAULT = 8ull * 1024 * 1024;   // 8 MiB
  constexpr size_t MAX_DEBUG_TEXT_BYTES_HARD    = 64ull * 1024 * 1024;  // 64 MiB

  // Batch ZIP: one upload => one segment
  // POST /v1/orgs/{org}/ingest_zip  multipart: file=@batch.zip,
  //   normalize=1|0 (default 1): делать нормализацию В ЯДРЕ при индексации
  //   (legacy) text_is_normalized=1|0: если 1 -> normalize=0
  app.Post(R"(/v1/orgs/([^/]+)/ingest_zip)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];

      if (!req.is_multipart_form_data()) {
        reply_json(res, 400, {{"error","expected multipart/form-data"}});
        return;
      }
      auto file_it = req.files.find("file");
      if (file_it == req.files.end()) {
        reply_json(res, 400, {{"error","missing file field"}});
        return;
      }

      const auto& f = file_it->second;

      if (f.content.size() > MAX_ZIP_UPLOAD_BYTES) {
        reply_json(res, 413, {{"error","zip too large"}, {"max_bytes", (uint64_t)MAX_ZIP_UPLOAD_BYTES}});
        return;
      }

      // --- choose normalization at indexing stage ---
      bool do_normalize = true;        // default = normalize
      bool have_norm = false;

      if (auto v = get_param_any(req, "normalize")) {
        do_normalize = parse_bool_str(*v, true);
        have_norm = true;
      }

      // legacy fallback ONLY if normalize param not provided
      if (!have_norm) {
        if (auto v = get_param_any(req, "text_is_normalized")) {
          const bool text_is_norm = parse_bool_str(*v, false);
          do_normalize = !text_is_norm;
        }
      }

      // Builder expects: text_is_normalized flag in corpus:
      //   true  => skip normalization
      //   false => do normalization
      const bool text_is_normalized_flag = !do_normalize;

      std::string segment_name;
      if (req.has_param("segment_name")) segment_name = req.get_param_value("segment_name");

      auto r = svc.ingest_zip_build_segment(org_id, f.filename, f.content,
                                           /*text_is_normalized=*/text_is_normalized_flag,
                                           segment_name);

      json docs = json::array();
      for (const auto& d : r.docs) docs.push_back(upload_to_json(d));

      json j = {
        {"segment_name", r.build.segment_name},
        {"seg_dir", r.build.seg_dir.string()},
        {"docs", r.build.docs},
        {"post9", r.build.post9},
        {"threads", r.build.threads},
        {"strict_text_is_normalized", r.build.strict_text_is_normalized},
        {"built_at_utc", r.build.built_at_utc},
        {"ingested_docs", docs},
        {"skipped", json::array()},

        // echo selected mode
        {"normalize", do_normalize ? 1 : 0},
        {"text_is_normalized", text_is_normalized_flag ? 1 : 0}
      };

      for (const auto& s : r.skipped) {
        j["skipped"].push_back({
          {"external_id", s.external_id},
          {"source_name", s.source_name},
          {"reason", s.reason},
        });
      }

      reply_json(res, 200, j);
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // Search: ядро НЕ нормализует запрос. Ищем "как ввели".
  // Чтобы был матч, query должен быть в ТОЙ ЖЕ ФОРМЕ, что и индекс:
  //   - индексировали normalize=1 => query должен быть нормализован тем же алгоритмом
  //   - индексировали normalize=0 => query должен быть raw
  app.Post(R"(/v1/orgs/([^/]+)/search)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];

      if (req.body.size() > MAX_JSON_BODY_BYTES) {
        reply_json(res, 413, {{"error","json body too large"}, {"max_bytes", (uint64_t)MAX_JSON_BODY_BYTES}});
        return;
      }

      json j;
      try {
        j = json::parse(req.body);
      } catch (...) {
        reply_json(res, 400, {{"error","invalid json"}}); return;
      }

      std::string query = j.value("query", "");
      if (query.empty()) { reply_json(res, 400, {{"error","query is empty"}}); return; }
      if (query.size() > MAX_QUERY_BYTES) {
        reply_json(res, 413, {{"error","query too large"}, {"max_bytes", (uint64_t)MAX_QUERY_BYTES}});
        return;
      }

      // IMPORTANT: do NOT normalize query inside core
      const bool query_is_normalized = true;

      l5::SearchOptions opt;
      opt.topk = j.value("topk", opt.topk);
      opt.candidates_topn = j.value("candidates_topn", opt.candidates_topn);
      opt.min_hits = j.value("min_hits", opt.min_hits);
      opt.max_postings_per_hash = j.value("max_postings_per_hash", opt.max_postings_per_hash);
      opt.span_min_len = j.value("span_min_len", opt.span_min_len);
      opt.span_gap = j.value("span_gap", opt.span_gap);
      opt.max_spans_per_doc = j.value("max_spans_per_doc", opt.max_spans_per_doc);
      opt.alpha = j.value("alpha", opt.alpha);

      auto r = svc.search(org_id, query, query_is_normalized, opt);
      reply_json(res, 200, l5::to_json(r));
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // List documents
  app.Get(R"(/v1/orgs/([^/]+)/documents)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];
      int limit = 50, offset = 0;

      auto parse_i = [](const std::string& s) -> int {
        size_t pos = 0;
        int v = std::stoi(s, &pos);
        if (pos != s.size()) throw std::invalid_argument("bad int");
        return v;
      };

      if (req.has_param("limit")) limit = parse_i(req.get_param_value("limit"));
      if (req.has_param("offset")) offset = parse_i(req.get_param_value("offset"));

      if (limit < 1) limit = 1;
      if (limit > 1000) limit = 1000;
      if (offset < 0) offset = 0;

      auto rows = svc.list_docs(org_id, limit, offset);
      json arr = json::array();
      for (auto& r : rows) arr.push_back(doc_to_json(r));
      reply_json(res, 200, {{"items", arr}, {"limit", limit}, {"offset", offset}});
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // Delete document (by doc_id or external_id)
  app.Delete(R"(/v1/orgs/([^/]+)/documents/([^/]+))", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];
      std::string key = req.matches[2];
      svc.delete_doc(org_id, key);
      reply_json(res, 200, {{"ok", true}});
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // Debug: показать как документ лежит в сегменте (DocInfo + did + docmeta)
  // GET /v1/orgs/{org}/debug/index_view?key=<doc_id|external_id>&max_preview=4000
  app.Get(R"(/v1/orgs/([^/]+)/debug/index_view)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];

      if (!req.has_param("key")) {
        reply_json(res, 400, {{"error","missing key param (doc_id or external_id)"}});
        return;
      }
      const std::string key = req.get_param_value("key");

      int max_preview = 2000;
      if (req.has_param("max_preview")) {
        max_preview = std::stoi(req.get_param_value("max_preview"));
        if (max_preview < 200) max_preview = 200;
        if (max_preview > 200000) max_preview = 200000;
      }

      const fs::path org_dir = fs::path(data_root) / "orgs" / org_id;
      const fs::path sqlite_path = org_dir / "meta.sqlite";
      const fs::path index_root  = org_dir / "index";

      Storage st(sqlite_path.string());
      st.init();

      auto row_opt = st.get_by_doc_or_external(org_id, key);
      if (!row_opt) {
        reply_json(res, 404, {{"error","document not found"}, {"key", key}});
        return;
      }
      const DocRow& row = *row_opt;

      if (row.last_segment.empty()) {
        reply_json(res, 400, {{"error","document has no last_segment (not indexed yet)"}, {"doc_id", row.doc_id}});
        return;
      }

      const fs::path seg_dir = index_root / row.last_segment;
      const auto docids_path = seg_dir / "index_native_docids.json";
      const auto bin_path    = seg_dir / "index_native.bin";

      uint32_t did = 0;
      std::string err;
      auto docinfo_opt = find_docinfo_entry_with_did(docids_path, row.doc_id, did, err);
      if (!docinfo_opt) {
        reply_json(res, 500, {{"error","failed reading docids"}, {"detail", err}, {"seg_dir", seg_dir.string()}});
        return;
      }
      json docinfo = *docinfo_opt;

      if (docinfo.contains("preview_text") && docinfo["preview_text"].is_string()) {
        std::string pv = docinfo["preview_text"].get<std::string>();
        if ((int)pv.size() > max_preview) pv.resize((size_t)max_preview);
        docinfo["preview_text"] = pv;
      }

      l5::HeaderV2 h{};
      l5::DocMeta dm{};
      if (!read_docmeta_by_did(bin_path, did, h, dm, err)) {
        reply_json(res, 500, {{"error","failed reading docmeta"}, {"detail", err}, {"bin", bin_path.string()}});
        return;
      }

      json out = {
        {"org_id", org_id},
        {"key", key},
        {"doc", doc_to_json(row)},
        {"segment_name", row.last_segment},
        {"seg_dir", seg_dir.string()},
        {"did", did},
        {"docinfo", docinfo},
        {"docmeta", {
          {"tok_len", dm.tok_len},
          {"simhash_hi", dm.simhash_hi},
          {"simhash_lo", dm.simhash_lo}
        }},
        {"header", {
          {"version", h.version},
          {"n_docs", h.n_docs},
          {"n_post9", h.n_post9},
          {"n_post13", h.n_post13}
        }},
        {"note", "Full text is not stored in segment; only preview_text + postings/docmeta. Use /debug/normalized_text to re-extract full text from file."}
      };

      reply_json(res, 200, out);
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // DEBUG: открыть выбранный файл и вернуть его текст.
  // normalize=0 -> вернуть RAW (как в файле/после конвертации)
  // normalize=1 -> вернуть NORMALIZED (как индексировали при normalize=1)
  //
  // GET /v1/orgs/{org}/debug/normalized_text?name=<external_id_or_doc_id>&normalize=0|1&max_bytes=...
  app.Get(R"(/v1/orgs/([^/]+)/debug/normalized_text)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];

      if (!req.has_param("name")) {
        reply_json(res, 400, {{"error","missing name param (external_id or doc_id)"}});
        return;
      }
      const std::string name = req.get_param_value("name");

      bool do_normalize = false; // default RAW
      if (req.has_param("normalize")) {
        do_normalize = parse_bool_str(req.get_param_value("normalize"), false);
      }

      size_t max_bytes = MAX_DEBUG_TEXT_BYTES_DEFAULT;
      if (req.has_param("max_bytes")) {
        long long v = std::stoll(req.get_param_value("max_bytes"));
        if (v < 0) v = 0;
        max_bytes = (size_t)v;
        if (max_bytes == 0) max_bytes = MAX_DEBUG_TEXT_BYTES_DEFAULT;
        if (max_bytes > MAX_DEBUG_TEXT_BYTES_HARD) max_bytes = MAX_DEBUG_TEXT_BYTES_HARD;
      }

      const fs::path org_dir = fs::path(data_root) / "orgs" / org_id;
      const fs::path sqlite_path = org_dir / "meta.sqlite";

      Storage st(sqlite_path.string());
      st.init();

      auto row_opt = st.get_by_doc_or_external(org_id, name);
      if (!row_opt) {
        reply_json(res, 404, {{"error","document not found"}, {"name", name}});
        return;
      }
      const DocRow& row = *row_opt;

      fs::path src = row.stored_path.empty() ? row.source_path : row.stored_path;
      if (src.empty()) {
        reply_json(res, 500, {{"error","stored_path/source_path empty"}, {"doc_id", row.doc_id}});
        return;
      }

      const std::string ext = lower_ext(src);

      ExtractedText ex;
      if (ext == ".txt") {
        ex = extract_text_from_file(src, /*assume_normalized=*/false);
      } else if (ext == ".doc" || ext == ".docx") {
        fs::path tmp = mk_tmp_dir();
        CleanupDir cleanup{tmp};

        fs::path txt_path = convert_doc_to_txt_utf8(src, tmp);
        ex = extract_text_from_file(txt_path, /*assume_normalized=*/false);
      } else {
        reply_json(res, 400, {{"error","unsupported file type"}, {"ext", ext}, {"path", src.string()}});
        return;
      }

      const uint64_t raw_bytes = (uint64_t)ex.text.size();

      std::string out_text;
      if (do_normalize) {
        normalize_for_shingles_simple_to(ex.text, out_text);
      } else {
        out_text = std::move(ex.text);
      }

      bool truncated = false;
      if (out_text.size() > max_bytes) {
        const size_t cut = utf8_safe_prefix_len(out_text, max_bytes);
        out_text.resize(cut);
        truncated = true;
      }

      json out = {
        {"org_id", org_id},
        {"name", name},
        {"doc_id", row.doc_id},
        {"external_id", row.external_id},
        {"source_name", row.source_name},
        {"source_path", row.source_path},
        {"stored_path", row.stored_path},
        {"last_segment", row.last_segment},

        {"normalize", do_normalize ? 1 : 0},
        {"raw_bytes", raw_bytes},
        {"returned_bytes", (uint64_t)out_text.size()},
        {"max_bytes", (uint64_t)max_bytes},
        {"truncated", truncated},

        {"text", out_text}
      };

      reply_json(res, 200, out);
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // ADMIN: wipe ALL базы загруженных файлов (orgs/*)
  // POST /v1/admin/wipe_all  body: {"confirm":"WIPE_ALL"}  (или query ?confirm=WIPE_ALL)
  app.Post(R"(/v1/admin/wipe_all)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string confirm;

      if (!req.body.empty()) {
        if (req.body.size() > MAX_JSON_BODY_BYTES) {
          reply_json(res, 413, {{"error","json body too large"}, {"max_bytes",(uint64_t)MAX_JSON_BODY_BYTES}});
          return;
        }
        json j;
        try { j = json::parse(req.body); }
        catch (...) { reply_json(res, 400, {{"error","invalid json"}}); return; }
        confirm = j.value("confirm", "");
      } else if (req.has_param("confirm")) {
        confirm = req.get_param_value("confirm");
      }

      if (confirm != "WIPE_ALL") {
        reply_json(res, 400, {{"error","confirm required"}, {"expected","WIPE_ALL"}});
        return;
      }

      std::lock_guard<std::mutex> lk(g_admin_mu);

      const fs::path orgs_dir = fs::path(data_root) / "orgs";
      std::error_code ec;

      uintmax_t removed = 0;
      if (fs::exists(orgs_dir, ec)) {
        ec.clear();
        removed = fs::remove_all(orgs_dir, ec);
        if (ec) {
          reply_json(res, 500, {{"error","remove_all failed"}, {"path", orgs_dir.string()}, {"detail", ec.message()}});
          return;
        }
      }

      ec.clear();
      fs::create_directories(orgs_dir, ec);
      if (ec) {
        reply_json(res, 500, {{"error","create_directories failed"}, {"path", orgs_dir.string()}, {"detail", ec.message()}});
        return;
      }

      reply_json(res, 200, {
        {"ok", true},
        {"wiped", "ALL"},
        {"orgs_dir", orgs_dir.string()},
        {"removed_entries", (uint64_t)removed}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // ADMIN: wipe ONE org полностью
  // POST /v1/orgs/{org}/admin/wipe  body: {"confirm":"WIPE_ORG"}  (или query ?confirm=WIPE_ORG)
  app.Post(R"(/v1/orgs/([^/]+)/admin/wipe)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];
      std::string confirm;

      if (!req.body.empty()) {
        if (req.body.size() > MAX_JSON_BODY_BYTES) {
          reply_json(res, 413, {{"error","json body too large"}, {"max_bytes",(uint64_t)MAX_JSON_BODY_BYTES}});
          return;
        }
        json j;
        try { j = json::parse(req.body); }
        catch (...) { reply_json(res, 400, {{"error","invalid json"}}); return; }
        confirm = j.value("confirm", "");
      } else if (req.has_param("confirm")) {
        confirm = req.get_param_value("confirm");
      }

      if (confirm != "WIPE_ORG") {
        reply_json(res, 400, {{"error","confirm required"}, {"expected","WIPE_ORG"}});
        return;
      }

      if (org_id.find("..") != std::string::npos) {
        reply_json(res, 400, {{"error","bad org_id"}});
        return;
      }

      std::lock_guard<std::mutex> lk(g_admin_mu);

      const fs::path org_dir  = fs::path(data_root) / "orgs" / org_id;
      const fs::path orgs_dir = fs::path(data_root) / "orgs";
      std::error_code ec;

      uintmax_t removed = 0;
      if (fs::exists(org_dir, ec)) {
        ec.clear();
        removed = fs::remove_all(org_dir, ec);
        if (ec) {
          reply_json(res, 500, {{"error","remove_all failed"}, {"path", org_dir.string()}, {"detail", ec.message()}});
          return;
        }
      }

      ec.clear();
      fs::create_directories(orgs_dir, ec);
      if (ec) {
        reply_json(res, 500, {{"error","create_directories failed"}, {"path", orgs_dir.string()}, {"detail", ec.message()}});
        return;
      }

      reply_json(res, 200, {
        {"ok", true},
        {"wiped", "ORG"},
        {"org_id", org_id},
        {"org_dir", org_dir.string()},
        {"removed_entries", (uint64_t)removed}
      });
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
