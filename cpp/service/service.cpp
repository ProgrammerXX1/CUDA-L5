#include "service.h"
#include "extractor.h"

#include <zip.h>

#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <fstream>
#include <iomanip>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <nlohmann/json.hpp>

#include "l5/format.h"  // utc_now_compact()

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

constexpr size_t   ZIP_MAX_FILES = 20000;
constexpr uint64_t ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES = 10ull * 1024 * 1024 * 1024; // 10 GiB safety cap

static void ensure_dirs(const fs::path& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  if (ec) throw std::runtime_error("mkdir failed: " + p.string() + " err=" + ec.message());
}

static std::string to_lower_copy(std::string s) {
  for (auto& c : s) c = (char)std::tolower((unsigned char)c);
  return s;
}

static std::string lower_ext(const fs::path& p) {
  return to_lower_copy(p.extension().string());
}

static std::string basename_of(const fs::path& p) {
  auto fn = p.filename().string();
  return fn.empty() ? std::string("file") : fn;
}

static fs::path replace_ext_txt(fs::path p) {
  p.replace_extension(".txt");
  return p;
}

static void write_bytes(const fs::path& out_path, const std::string& bytes) {
  std::ofstream out(out_path, std::ios::binary);
  if (!out) throw std::runtime_error("cannot write file: " + out_path.string());
  out.write(bytes.data(), (std::streamsize)bytes.size());
  out.flush();
  if (!out) throw std::runtime_error("write failed: " + out_path.string());
}

static void copy_file_binary(const fs::path& src, const fs::path& dst) {
  std::ifstream in(src, std::ios::binary);
  if (!in) throw std::runtime_error("cannot read file: " + src.string());
  std::ofstream out(dst, std::ios::binary);
  if (!out) throw std::runtime_error("cannot write file: " + dst.string());
  out << in.rdbuf();
  out.flush();
  if (!out) throw std::runtime_error("copy failed: " + src.string() + " -> " + dst.string());
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
    fs::path p = base / ("l5_zip_" + std::to_string(t) + "_" + std::to_string(i));
    std::error_code ec;
    if (fs::create_directories(p, ec) && !ec) return p;
  }
  throw std::runtime_error("cannot create temp dir");
}

// ---- zip slip protection ----
static bool zip_entry_name_is_safe(const std::string& name) {
  if (name.empty()) return false;
  if (name.find('\0') != std::string::npos) return false;
  if (name[0] == '/') return false;
  if (name.find('\\') != std::string::npos) return false;

  fs::path rel = fs::path(name).lexically_normal();
  if (rel.empty()) return false;
  if (rel.is_absolute()) return false;

  for (const auto& part : rel) {
    const std::string s = part.string();
    if (s == "..") return false;
  }
  return true;
}

static void unzip_libzip_safe(const fs::path& zip_path,
                              const fs::path& dst_dir,
                              size_t max_files = ZIP_MAX_FILES,
                              uint64_t max_total_bytes = ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES) {
  int err = 0;
  zip_t* za = zip_open(zip_path.string().c_str(), ZIP_RDONLY, &err);
  if (!za) throw std::runtime_error("zip_open failed err=" + std::to_string(err));
  auto za_guard = std::unique_ptr<zip_t, decltype(&zip_close)>(za, &zip_close);

  zip_int64_t n = zip_get_num_entries(za, 0);
  if (n < 0) throw std::runtime_error("zip_get_num_entries failed");
  if ((size_t)n > max_files) throw std::runtime_error("zip too many entries: " + std::to_string((size_t)n));

  ensure_dirs(dst_dir);

  uint64_t total = 0;

  for (zip_uint64_t i = 0; i < (zip_uint64_t)n; ++i) {
    zip_stat_t st;
    zip_stat_init(&st);
    if (zip_stat_index(za, i, 0, &st) != 0) continue;

    std::string name = st.name ? st.name : "";
    if (name.empty()) continue;

    // directory entry
    if (!name.empty() && name.back() == '/') {
      if (!zip_entry_name_is_safe(name)) throw std::runtime_error("unsafe zip dir entry: " + name);
      fs::path rel = fs::path(name).lexically_normal();
      ensure_dirs(dst_dir / rel);
      continue;
    }

    if (!zip_entry_name_is_safe(name)) throw std::runtime_error("unsafe zip entry: " + name);

    total += (uint64_t)st.size;
    if (total > max_total_bytes) throw std::runtime_error("zip exceeds max_total_bytes");

    fs::path rel = fs::path(name).lexically_normal();
    fs::path out = dst_dir / rel;
    ensure_dirs(out.parent_path());

    zip_file_t* zf = zip_fopen_index(za, i, 0);
    if (!zf) throw std::runtime_error("zip_fopen_index failed for entry: " + name);
    auto zf_guard = std::unique_ptr<zip_file_t, decltype(&zip_fclose)>(zf, &zip_fclose);

    std::ofstream fout(out, std::ios::binary);
    if (!fout) throw std::runtime_error("cannot write: " + out.string());

    char buf[1 << 16];
    while (true) {
      zip_int64_t rd = zip_fread(zf, buf, sizeof(buf));
      if (rd < 0) throw std::runtime_error("zip_fread failed for entry: " + name);
      if (rd == 0) break;
      fout.write(buf, (std::streamsize)rd);
      if (!fout) throw std::runtime_error("write failed: " + out.string());
    }
    fout.flush();
  }
}

struct CleanupDir {
  fs::path p;
  ~CleanupDir() {
    if (p.empty()) return;
    std::error_code ec;
    fs::remove_all(p, ec);
  }
};

} // namespace

// -------------------- L5Service --------------------

L5Service::L5Service(fs::path data_root) : data_root_(std::move(data_root)) {
  ensure_dirs(data_root_);
  ensure_dirs(data_root_ / "orgs");
}

fs::path L5Service::org_root(const std::string& org) const { return data_root_ / "orgs" / org; }
fs::path L5Service::org_index_root(const std::string& org) const { return org_root(org) / "index"; }
fs::path L5Service::org_sqlite(const std::string& org) const { return org_root(org) / "meta.sqlite"; }
fs::path L5Service::org_tombstones(const std::string& org) const { return org_root(org) / "tombstones.jsonl"; }
fs::path L5Service::org_uploads_dir(const std::string& org) const { return org_root(org) / "uploads"; }

std::mutex& L5Service::org_mutex_(const std::string& org_id) {
  const size_t h = std::hash<std::string>{}(org_id);
  return org_mu_[h % ORG_LOCKS];
}

std::string L5Service::utc_now_iso() {
  using namespace std::chrono;
  auto now = system_clock::now();
  auto t = system_clock::to_time_t(now);
  std::tm tm{};
#if defined(_WIN32)
  gmtime_s(&tm, &t);
#else
  gmtime_r(&t, &tm);
#endif
  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
  return oss.str();
}

std::string L5Service::gen_uuid_v4() {
  // UUID-like string, good enough for IDs (not strict RFC4122)
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis;

  uint64_t a = dis(gen);
  uint64_t b = dis(gen);

  std::ostringstream oss;
  oss << std::hex << std::nouppercase;
  oss << ((a >> 32) & 0xFFFFFFFFULL);
  oss << "-";
  oss << ((a >> 16) & 0xFFFFULL);
  oss << "-";
  oss << ((a >> 0) & 0xFFFFULL);
  oss << "-";
  oss << ((b >> 48) & 0xFFFFULL);
  oss << "-";
  oss << (b & 0xFFFFFFFFFFFFULL);
  return oss.str();
}

UploadResult L5Service::ingest_file(const std::string& org_id,
                                    const std::string& filename,
                                    const std::string& bytes,
                                    const std::string& external_id_opt,
                                    bool text_is_normalized) {
  // protect SQLite/file ops per org
  std::lock_guard<std::mutex> lk(org_mutex_(org_id));

  ensure_dirs(org_root(org_id));
  ensure_dirs(org_uploads_dir(org_id));
  ensure_dirs(org_index_root(org_id));

  Storage st(org_sqlite(org_id).string());
  st.init();

  const std::string doc_id = gen_uuid_v4();
  const std::string external_id = external_id_opt.empty() ? doc_id : external_id_opt;

  const fs::path stored = org_uploads_dir(org_id) / (doc_id + "_" + filename);
  write_bytes(stored, bytes);

  // NOTE: this endpoint does NOT build a segment.
  ExtractedText ex = extract_text_from_file(stored, text_is_normalized);

  DocRow row;
  row.org_id = org_id;
  row.doc_id = doc_id;
  row.external_id = external_id;
  row.source_path = stored.string();
  row.source_name = filename;
  row.stored_path = stored.string();
  row.preview = ex.preview;
  row.created_at_utc = utc_now_iso();
  row.deleted = 0;
  row.last_segment = "";
  st.upsert_doc(row);

  UploadResult r;
  r.doc_id = doc_id;
  r.external_id = external_id;
  r.source_name = filename;
  r.stored_path = stored.string();
  r.bytes = (uint64_t)bytes.size();
  return r;
}

IngestZipResult L5Service::ingest_zip_build_segment(const std::string& org_id,
                                                    const std::string& zip_name,
                                                    const std::string& zip_bytes,
                                                    bool text_is_normalized,
                                                    const std::string& segment_name_opt) {
  // Phase 0: make sure org dirs & sqlite exist
  {
    std::lock_guard<std::mutex> lk(org_mutex_(org_id));
    ensure_dirs(org_root(org_id));
    ensure_dirs(org_uploads_dir(org_id));
    ensure_dirs(org_index_root(org_id));

    Storage st(org_sqlite(org_id).string());
    st.init();
  }

  Storage st(org_sqlite(org_id).string());
  st.init();

  // temp workspace (auto cleanup)
  fs::path tmp = mk_tmp_dir();
  CleanupDir cleanup{tmp};

  fs::path zip_path = tmp / ("upload_" + gen_uuid_v4() + "_" + zip_name);
  fs::path unpack_dir = tmp / "unpacked";
  fs::path conv_src = tmp / "conv_src";
  fs::path conv_out = tmp / "conv_out";
  fs::path lo_profile = tmp / "lo_profile";

  ensure_dirs(unpack_dir);
  ensure_dirs(conv_src);
  ensure_dirs(conv_out);
  ensure_dirs(lo_profile);

  write_bytes(zip_path, zip_bytes);
  unzip_libzip_safe(zip_path, unpack_dir);

  struct PendingDoc {
    std::string doc_id;
    std::string external_id;   // rel path inside zip
    std::string source_name;   // base filename
    fs::path stored_path;      // original stored
    fs::path text_path;        // txt for extraction/indexing
    bool needs_convert{false};
  };

  std::vector<PendingDoc> pending;
  pending.reserve(4096);

  // 1) Collect supported files from unpacked
  {
    std::error_code ec;
    fs::recursive_directory_iterator it(unpack_dir, fs::directory_options::skip_permission_denied, ec);
    fs::recursive_directory_iterator end;
    for (; it != end; it.increment(ec)) {
      if (ec) { ec.clear(); continue; }
      if (!it->is_regular_file(ec) || ec) { ec.clear(); continue; }

      fs::path p = it->path();
      const std::string ext = lower_ext(p);

      const bool is_txt  = (ext == ".txt");
      const bool is_doc  = (ext == ".doc");
      const bool is_docx = (ext == ".docx");
      if (!is_txt && !is_doc && !is_docx) continue;

      PendingDoc d;
      d.doc_id = gen_uuid_v4();
      d.source_name = basename_of(p);

      std::string rel = fs::relative(p, unpack_dir, ec).generic_string();
      if (ec || rel.empty()) { ec.clear(); rel = d.source_name; }
      d.external_id = rel;

      // store original into uploads (unique doc_id prefix => no collisions)
      d.stored_path = org_uploads_dir(org_id) / (d.doc_id + "_" + d.source_name);
      copy_file_binary(p, d.stored_path);

      if (is_txt) {
        d.text_path = d.stored_path;
        d.needs_convert = false;
      } else {
        // copy to conv_src with unique name to avoid collisions
        const fs::path unique_in = conv_src / (d.doc_id + "_" + d.source_name);
        copy_file_binary(d.stored_path, unique_in);
        d.text_path = conv_out / replace_ext_txt(unique_in.filename());
        d.needs_convert = true;
      }

      pending.push_back(std::move(d));
    }
  }

  if (pending.empty()) {
    throw std::runtime_error("zip has no supported files (.txt/.doc/.docx)");
  }

  // 2) Convert doc/docx (batch) via soffice (isolated profile + UTF-8)
  {
    bool has_any = false;
    for (auto it = fs::directory_iterator(conv_src); it != fs::directory_iterator(); ++it) {
      if (it->is_regular_file()) { has_any = true; break; }
    }

    if (has_any) {
      const fs::path abs_profile = fs::absolute(lo_profile);
      std::string profile_uri = "file://" + abs_profile.string();

      std::string cmd =
        "find " + shell_quote(conv_src.string()) + " -type f -print0"
        " | xargs -0 -n 50 soffice --headless --nologo --nolockcheck --nodefault --norestore"
        " -env:UserInstallation=" + shell_quote(profile_uri) +
        " --convert-to " + shell_quote("txt:Text (encoded):UTF8") +
        " --outdir " + shell_quote(conv_out.string());

      const int rc = run_cmd_bash(cmd);
      if (rc != 0) {
        throw std::runtime_error("soffice convert failed rc=" + std::to_string(rc));
      }
    }
  }

  // 3) Build corpus.jsonl (parallel extract + jsonl parts) + bulk sqlite upsert
  fs::path corpus = tmp / "corpus.jsonl";

  struct ThreadAccum {
    std::vector<UploadResult> docs;
    std::vector<SkippedDoc> skipped;
    std::vector<DocRow> rows;
    std::vector<std::string> doc_ids_for_segment;
  };

  unsigned hw = std::thread::hardware_concurrency();
  if (hw == 0) hw = 4;

  unsigned n_threads = std::min<unsigned>(hw, 16u);
  if (n_threads > pending.size()) n_threads = (unsigned)pending.size();
  if (n_threads == 0) n_threads = 1;

  std::vector<fs::path> part_paths;
  part_paths.reserve(n_threads);
  for (unsigned t = 0; t < n_threads; ++t) {
    part_paths.push_back(tmp / ("corpus_part_" + std::to_string(t) + ".jsonl"));
  }

  std::vector<ThreadAccum> acc(n_threads);
  std::vector<std::thread> workers;
  workers.reserve(n_threads);

  std::vector<std::exception_ptr> errs(n_threads);
  std::atomic<size_t> next{0};

  const std::string created_at = utc_now_iso();
  const std::string org_j = json(org_id).dump();
  const char* norm_flag = text_is_normalized ? "true" : "false";

  for (unsigned t = 0; t < n_threads; ++t) {
    workers.emplace_back([&, t]() {
      try {
        std::ofstream outp(part_paths[t], std::ios::binary);
        if (!outp) throw std::runtime_error("cannot open corpus part: " + part_paths[t].string());

        auto& A = acc[t];
        A.docs.reserve(1024);
        A.rows.reserve(1024);
        A.doc_ids_for_segment.reserve(1024);

        while (true) {
          size_t i = next.fetch_add(1, std::memory_order_relaxed);
          if (i >= pending.size()) break;

          auto& d = pending[i];

          if (d.needs_convert && !fs::exists(d.text_path)) {
            A.skipped.push_back(SkippedDoc{d.external_id, d.source_name, "convert_failed_no_txt"});
            continue;
          }

          ExtractedText ex = extract_text_from_file(d.text_path, text_is_normalized);

          outp
            << "{\"doc_id\":" << json(d.doc_id).dump()
            << ",\"organization_id\":" << org_j
            << ",\"external_id\":" << json(d.external_id).dump()
            << ",\"source_path\":" << json(d.stored_path.string()).dump()
            << ",\"source_name\":" << json(d.source_name).dump()
            << ",\"text\":" << json(ex.text).dump()
            << ",\"text_is_normalized\":" << norm_flag
            << "}\n";

          DocRow row;
          row.org_id = org_id;
          row.doc_id = d.doc_id;
          row.external_id = d.external_id;
          row.source_path = d.stored_path.string();
          row.source_name = d.source_name;
          row.stored_path = d.stored_path.string();
          row.preview = ex.preview;
          row.created_at_utc = created_at;
          row.deleted = 0;
          row.deleted_at_utc = "";
          row.last_segment = "";
          A.rows.push_back(std::move(row));

          UploadResult ur;
          ur.doc_id = d.doc_id;
          ur.external_id = d.external_id;
          ur.source_name = d.source_name;
          ur.stored_path = d.stored_path.string();
          {
            std::error_code ec;
            ur.bytes = (uint64_t)fs::file_size(d.stored_path, ec);
            if (ec) ur.bytes = 0;
          }
          A.docs.push_back(std::move(ur));
          A.doc_ids_for_segment.push_back(d.doc_id);
        }

        outp.flush();
        if (!outp) throw std::runtime_error("write failed corpus part: " + part_paths[t].string());
      } catch (...) {
        errs[t] = std::current_exception();
      }
    });
  }

  for (auto& th : workers) th.join();
  for (auto& e : errs) {
    if (e) std::rethrow_exception(e);
  }

  IngestZipResult out;
  out.docs.reserve(pending.size());
  out.skipped.reserve(64);

  std::vector<std::string> doc_ids_for_segment;
  doc_ids_for_segment.reserve(pending.size());

  std::vector<DocRow> rows_all;
  rows_all.reserve(pending.size());

  for (unsigned t = 0; t < n_threads; ++t) {
    for (auto& s : acc[t].skipped) out.skipped.push_back(std::move(s));
    for (auto& r : acc[t].rows) rows_all.push_back(std::move(r));
    for (auto& d : acc[t].docs) out.docs.push_back(std::move(d));
    for (auto& id : acc[t].doc_ids_for_segment) doc_ids_for_segment.push_back(std::move(id));
  }

  if (out.docs.empty()) {
    throw std::runtime_error("no documents converted/extracted for indexing");
  }

  // merge parts -> corpus.jsonl
  {
    std::ofstream corpus_out(corpus, std::ios::binary);
    if (!corpus_out) throw std::runtime_error("cannot open temp corpus.jsonl: " + corpus.string());

    for (auto& pp : part_paths) {
      std::ifstream in(pp, std::ios::binary);
      if (!in) continue;
      corpus_out << in.rdbuf();
    }

    corpus_out.flush();
    if (!corpus_out) throw std::runtime_error("failed writing corpus.jsonl");
  }

  // bulk sqlite write (protected)
  {
    std::lock_guard<std::mutex> lk(org_mutex_(org_id));
    st.upsert_docs_bulk(rows_all);
  }

  // build index segment (serialize builds per process)
  l5::BuildOptions opt;
  opt.segment_name = segment_name_opt.empty()
      ? (std::string("seg_") + l5::utc_now_compact() + "_" + gen_uuid_v4().substr(0, 8))
      : segment_name_opt;

  opt.max_threads = hw;

  const fs::path out_root = org_index_root(org_id);
  {
    std::lock_guard<std::mutex> lk(build_mu_);
    out.build = l5::build_segment_jsonl(corpus, out_root, opt);
  }

  // update last_segment (protected)
  {
    std::lock_guard<std::mutex> lk(org_mutex_(org_id));
    st.update_last_segment(org_id, doc_ids_for_segment, out.build.segment_name);
  }

  return out;
}

l5::SearchResult L5Service::search(const std::string& org_id,
                                   const std::string& query,
                                   bool query_is_normalized,
                                   const l5::SearchOptions& opt) {
  const fs::path out_root = org_index_root(org_id);

  Tombstones ts(org_tombstones(org_id));
  {
    std::lock_guard<std::mutex> lk(tomb_mu_);
    ts.load();
  }

  auto res = l5::search_out_root(out_root, query, query_is_normalized, opt);

  std::vector<l5::Hit> filtered;
  filtered.reserve(res.hits.size());
  for (auto& h : res.hits) {
    if (ts.contains(h.doc_id)) continue;
    filtered.push_back(std::move(h));
  }
  res.hits = std::move(filtered);
  return res;
}

void L5Service::delete_doc(const std::string& org_id, const std::string& key) {
  // SQLite read/update protected per org
  std::lock_guard<std::mutex> lk(org_mutex_(org_id));

  Storage st(org_sqlite(org_id).string());
  st.init();

  auto row = st.get_by_doc_or_external(org_id, key);
  if (!row) return;

  // tombstones protected
  {
    std::lock_guard<std::mutex> lk2(tomb_mu_);
    Tombstones ts(org_tombstones(org_id));
    ts.load();
    ts.append(row->doc_id);
  }

  st.mark_deleted(org_id, key, utc_now_iso());
}

std::vector<DocRow> L5Service::list_docs(const std::string& org_id, int limit, int offset) {
  std::lock_guard<std::mutex> lk(org_mutex_(org_id));
  Storage st(org_sqlite(org_id).string());
  st.init();
  return st.list_docs(org_id, limit, offset);
}
