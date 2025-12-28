#include "service.h"
#include "extractor.h"

#include <zip.h>

#include <atomic>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

#include "l5/format.h"
#include "l5/compactor.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

// -------------------- limits --------------------
constexpr size_t   ZIP_MAX_FILES = 20000;
constexpr uint64_t ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES = 10ull * 1024 * 1024 * 1024; // 10 GiB safety cap

// perf defaults
constexpr unsigned  PLAGIO_BUILD_THREADS_DEFAULT   = 20u;
constexpr uint64_t  PLAGIO_SORT_RAM_BYTES_DEFAULT = (100ull << 30); // 100 GiB

// soffice conversion defaults
constexpr unsigned  PLAGIO_CONVERT_PROCS_FALLBACK  = 20u;
constexpr unsigned  PLAGIO_CONVERT_BATCH_DEFAULT   = 200u;

// default L5 shards
constexpr unsigned  PLAGIO_L5_SHARDS_DEFAULT = 32u;

// -------------------- env helpers --------------------
static unsigned env_u32(const char* k, unsigned defv) {
  const char* s = std::getenv(k);
  if (!s || !*s) return defv;
  char* end = nullptr;
  unsigned long v = std::strtoul(s, &end, 10);
  if (!end || *end != '\0') return defv;
  return (unsigned)v;
}

static uint64_t env_u64(const char* k, uint64_t defv) {
  const char* s = std::getenv(k);
  if (!s || !*s) return defv;
  char* end = nullptr;
  unsigned long long v = std::strtoull(s, &end, 10);
  if (!end || *end != '\0') return defv;
  return (uint64_t)v;
}

// -------------------- fs helpers --------------------
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

// -------------------- shell helpers --------------------
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
  std::string full = "bash -lc " + shell_quote(cmd);
  return std::system(full.c_str());
}

static fs::path mk_tmp_dir(const std::string& prefix) {
  const auto base = fs::temp_directory_path();
  const uint64_t t = (uint64_t)std::time(nullptr);
  for (int i = 0; i < 200; ++i) {
    fs::path p = base / (prefix + "_" + std::to_string(t) + "_" + std::to_string(i));
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

static void clip_text_inplace(std::string& s, size_t max_bytes) {
  if (max_bytes == 0) return;
  if (s.size() <= max_bytes) return;
  const size_t cut = utf8_safe_prefix_len(s, max_bytes);
  s.resize(cut);
}

// -------------------- zip slip protection --------------------
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
  }
}

// -------------------- soffice parallel conversion --------------------
static void soffice_convert_parallel(const fs::path& conv_src,
                                    const fs::path& conv_out,
                                    const fs::path& profiles_base,
                                    unsigned procs,
                                    unsigned batch_n) {
  std::vector<fs::path> inputs;
  inputs.reserve(4096);

  for (auto it = fs::directory_iterator(conv_src); it != fs::directory_iterator(); ++it) {
    std::error_code ec;
    if (!it->is_regular_file(ec) || ec) continue;
    inputs.push_back(it->path());
  }
  if (inputs.empty()) return;

  unsigned hw = std::thread::hardware_concurrency();
  if (hw == 0) hw = 4;

  if (procs == 0) procs = 1;
  if (procs > hw) procs = hw;
  if (procs > (unsigned)inputs.size()) procs = (unsigned)inputs.size();
  if (batch_n == 0) batch_n = (unsigned)inputs.size();

  const size_t n = inputs.size();
  const size_t per = (n + procs - 1) / procs;

  std::vector<std::thread> th;
  th.reserve(procs);

  std::vector<std::exception_ptr> errs(procs);

  ensure_dirs(conv_out);
  ensure_dirs(profiles_base);

  for (unsigned i = 0; i < procs; ++i) {
    const size_t start = (size_t)i * per;
    const size_t end = std::min(n, start + per);
    if (start >= end) break;

    th.emplace_back([&, i, start, end]() {
      try {
        const fs::path profile_dir = profiles_base / ("lo_profile_" + std::to_string(i));
        ensure_dirs(profile_dir);

        const fs::path abs_profile = fs::absolute(profile_dir);
        const std::string profile_uri = "file://" + abs_profile.string();

        const fs::path list_path = profiles_base / ("files_" + std::to_string(i) + ".lst");
        {
          std::ofstream lst(list_path, std::ios::binary);
          if (!lst) throw std::runtime_error("cannot open list file: " + list_path.string());
          for (size_t k = start; k < end; ++k) {
            const std::string s = inputs[k].string();
            lst.write(s.data(), (std::streamsize)s.size());
            lst.put('\0');
          }
          lst.flush();
          if (!lst) throw std::runtime_error("cannot write list file: " + list_path.string());
        }

        std::string cmd =
          "cat " + shell_quote(list_path.string()) +
          " | xargs -0 -n " + std::to_string(batch_n) +
          " soffice --headless --nologo --nolockcheck --nodefault --norestore"
          " -env:UserInstallation=" + shell_quote(profile_uri) +
          " --convert-to " + shell_quote("txt:Text (encoded):UTF8") +
          " --outdir " + shell_quote(conv_out.string());

        const int rc = run_cmd_bash(cmd);
        if (rc != 0) throw std::runtime_error("soffice convert failed rc=" + std::to_string(rc));
      } catch (...) {
        errs[i] = std::current_exception();
      }
    });
  }

  for (auto& t : th) t.join();
  for (auto& e : errs) if (e) std::rethrow_exception(e);
}

static fs::path find_any_txt(const fs::path& dir) {
  for (auto it = fs::directory_iterator(dir); it != fs::directory_iterator(); ++it) {
    std::error_code ec;
    if (!it->is_regular_file(ec) || ec) continue;
    if (lower_ext(it->path()) == ".txt") return it->path();
  }
  return {};
}

} // namespace

// -------------------- L5Service --------------------

L5Service::L5Service(fs::path data_root) : data_root_(std::move(data_root)) {
  ensure_dirs(data_root_);
  ensure_dirs(data_root_ / "orgs");
}

fs::path L5Service::org_root(const std::string& org) const { return data_root_ / "orgs" / org; }
fs::path L5Service::org_sqlite(const std::string& org) const { return org_root(org) / "meta.sqlite"; }
fs::path L5Service::org_tombstones(const std::string& org) const { return org_root(org) / "tombstones.jsonl"; }
fs::path L5Service::org_uploads_dir(const std::string& org) const { return org_root(org) / "uploads"; }

fs::path L5Service::org_index_base(const std::string& org) const { return org_root(org) / "index"; }

fs::path L5Service::org_level_root(const std::string& org, int level) const {
  if (level < 1) level = 1;
  if (level > 4) level = 4;
  return org_index_base(org) / ("l" + std::to_string(level));
}

static std::string shard_dir_name(unsigned shard) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "s%02u", shard);
  return std::string(buf);
}

fs::path L5Service::org_l5_shard_root(const std::string& org, unsigned shard) const {
  return org_index_base(org) / "l5" / shard_dir_name(shard);
}

unsigned L5Service::l5_shards_count() const {
  unsigned n = env_u32("PLAGIO_L5_SHARDS", PLAGIO_L5_SHARDS_DEFAULT);
  if (n < 1) n = 1;
  if (n > 256) n = 256;
  return n;
}

unsigned L5Service::pick_l5_shard(const std::string& seed) const {
  const unsigned n = l5_shards_count();
  const size_t h = std::hash<std::string>{}(seed);
  return (unsigned)(h % n);
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

IngestOneResult L5Service::ingest_file_build_segment(const std::string& org_id,
                                                     const std::string& filename,
                                                     const std::string& bytes,
                                                     const std::string& external_id_opt,
                                                     bool text_is_normalized,
                                                     int target_level,
                                                     std::optional<unsigned> l5_shard_opt,
                                                     const std::string& segment_name_opt) {
  ensure_dirs(org_root(org_id));
  ensure_dirs(org_uploads_dir(org_id));
  ensure_dirs(org_index_base(org_id));

  Storage st(org_sqlite(org_id).string());
  st.init();

  const std::string doc_id = gen_uuid_v4();
  const std::string external_id = external_id_opt.empty() ? doc_id : external_id_opt;

  const fs::path stored = org_uploads_dir(org_id) / (doc_id + "_" + filename);
  write_bytes(stored, bytes);

  // extract text (txt/doc/docx)
  const std::string ext = lower_ext(stored);
  ExtractedText ex;

  if (ext == ".txt") {
    ex = extract_text_from_file(stored, /*assume_normalized=*/false, (size_t)opt.max_text_bytes_per_do);
  } else if (ext == ".doc" || ext == ".docx") {
    fs::path tmp = mk_tmp_dir("l5_one");
    CleanupDir cleanup{tmp};

    fs::path conv_src = tmp / "conv_src";
    fs::path conv_out = tmp / "conv_out";
    fs::path profiles = tmp / "profiles";
    ensure_dirs(conv_src);
    ensure_dirs(conv_out);
    ensure_dirs(profiles);

    const fs::path unique_in = conv_src / (doc_id + "_" + filename);
    copy_file_binary(stored, unique_in);

    soffice_convert_parallel(conv_src, conv_out, profiles, /*procs=*/1, /*batch=*/1);

    fs::path out_txt = conv_out / replace_ext_txt(unique_in.filename());
    if (!fs::exists(out_txt)) {
      out_txt = find_any_txt(conv_out);
    }
    if (out_txt.empty() || !fs::exists(out_txt)) {
      throw std::runtime_error("soffice produced no .txt for single file");
    }
    ex = extract_text_from_file(out_txt, /*assume_normalized=*/false, (size_t)opt.max_text_bytes_per_do);
  } else {
    throw std::invalid_argument("unsupported file type for ingest_file (only .txt/.doc/.docx)");
  }

  // builder options (important: clip here to avoid huge JSONL line drop)
  l5::BuildOptions opt;
  opt.segment_name = segment_name_opt;
  opt.max_threads = std::min<unsigned>(std::max(1u, std::thread::hardware_concurrency()),
                                      env_u32("PLAGIO_BUILD_THREADS", PLAGIO_BUILD_THREADS_DEFAULT));
  opt.ram_limit_bytes = env_u64("PLAGIO_SORT_RAM_BYTES", PLAGIO_SORT_RAM_BYTES_DEFAULT);

  // clip input text to builder cap (8MiB default)
  clip_text_inplace(ex.text, (size_t)opt.max_text_bytes_per_doc);

  // choose out_root by level
  fs::path out_root;
  if (target_level == 5) {
    const unsigned shard = l5_shard_opt.has_value() ? *l5_shard_opt : pick_l5_shard(filename + ":" + external_id);
    out_root = org_l5_shard_root(org_id, shard);
  } else {
    out_root = org_level_root(org_id, target_level <= 0 ? 1 : target_level);
  }
  ensure_dirs(out_root);

  // build corpus.jsonl in temp
  fs::path tmp = mk_tmp_dir("l5_corpus_one");
  CleanupDir cleanup{tmp};

  fs::path corpus = tmp / "corpus.jsonl";
  {
    std::ofstream out(corpus, std::ios::binary);
    if (!out) throw std::runtime_error("cannot open corpus.jsonl for single file");

    const std::string org_j = json(org_id).dump();
    const char* norm_flag = text_is_normalized ? "true" : "false";

    out
      << "{\"doc_id\":" << json(doc_id).dump()
      << ",\"organization_id\":" << org_j
      << ",\"external_id\":" << json(external_id).dump()
      << ",\"source_path\":" << json(stored.string()).dump()
      << ",\"source_name\":" << json(filename).dump()
      << ",\"text\":" << json(ex.text).dump()
      << ",\"text_is_normalized\":" << norm_flag
      << "}\n";

    out.flush();
    if (!out) throw std::runtime_error("write failed corpus.jsonl");
  }

  // sqlite upsert
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
  row.deleted_at_utc = "";
  row.last_segment = "";
  st.upsert_doc(row);

  IngestOneResult res;

  // build segment (serialize per-org)
  {
    std::lock_guard<std::mutex> lk(build_mu_for(org_id));
    res.build = l5::build_segment_jsonl(corpus, out_root, opt);
  }

  // update last_segment
  st.update_last_segment(org_id, std::vector<std::string>{doc_id}, res.build.segment_name);

  // upload result
  UploadResult ur;
  ur.org_id = org_id;
  ur.doc_id = doc_id;
  ur.external_id = external_id;
  ur.source_name = filename;
  ur.stored_path = stored.string();
  ur.bytes = (uint64_t)bytes.size();
  res.doc = std::move(ur);

  return res;
}

IngestZipResult L5Service::ingest_zip_build_segment(const std::string& org_id,
                                                    const std::string& zip_name,
                                                    const std::string& zip_bytes,
                                                    bool text_is_normalized,
                                                    int target_level,
                                                    std::optional<unsigned> l5_shard_opt,
                                                    const std::string& segment_name_opt) {
  ensure_dirs(org_root(org_id));
  ensure_dirs(org_uploads_dir(org_id));
  ensure_dirs(org_index_base(org_id));

  Storage st(org_sqlite(org_id).string());
  st.init();

  fs::path tmp = mk_tmp_dir("l5_zip");
  CleanupDir cleanup{tmp};

  fs::path zip_path     = tmp / ("upload_" + gen_uuid_v4() + "_" + zip_name);
  fs::path unpack_dir   = tmp / "unpacked";
  fs::path conv_src     = tmp / "conv_src";
  fs::path conv_out     = tmp / "conv_out";
  fs::path lo_profiles  = tmp / "lo_profiles";

  ensure_dirs(unpack_dir);
  ensure_dirs(conv_src);
  ensure_dirs(conv_out);
  ensure_dirs(lo_profiles);

  write_bytes(zip_path, zip_bytes);
  unzip_libzip_safe(zip_path, unpack_dir);

  struct PendingDoc {
    std::string doc_id;
    std::string external_id;
    std::string source_name;
    fs::path stored_path;
    fs::path text_path;
    bool needs_convert{false};
  };

  std::vector<PendingDoc> pending;
  pending.reserve(4096);

  // collect supported files
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

      d.stored_path = org_uploads_dir(org_id) / (d.doc_id + "_" + d.source_name);
      copy_file_binary(p, d.stored_path);

      if (is_txt) {
        d.text_path = d.stored_path;
        d.needs_convert = false;
      } else {
        const fs::path unique_in = conv_src / (d.doc_id + "_" + d.source_name);
        copy_file_binary(d.stored_path, unique_in);
        d.text_path = conv_out / replace_ext_txt(unique_in.filename());
        d.needs_convert = true;
      }

      pending.push_back(std::move(d));
    }
  }

  if (pending.empty()) throw std::runtime_error("zip has no supported files (.txt/.doc/.docx)");

  unsigned hw = std::thread::hardware_concurrency();
  if (hw == 0) hw = 4;

  const unsigned want_threads  = env_u32("PLAGIO_BUILD_THREADS", PLAGIO_BUILD_THREADS_DEFAULT);
  const unsigned build_threads = std::min<unsigned>(hw, want_threads);

  // convert doc/docx
  {
    bool has_any = false;
    for (auto it = fs::directory_iterator(conv_src); it != fs::directory_iterator(); ++it) {
      std::error_code ec;
      if (it->is_regular_file(ec) && !ec) { has_any = true; break; }
    }
    if (has_any) {
      const unsigned def_procs = std::min<unsigned>(PLAGIO_CONVERT_PROCS_FALLBACK, build_threads);
      unsigned procs = env_u32("PLAGIO_CONVERT_PROCS", def_procs);
      unsigned batch = env_u32("PLAGIO_CONVERT_BATCH", PLAGIO_CONVERT_BATCH_DEFAULT);
      soffice_convert_parallel(conv_src, conv_out, lo_profiles, procs, batch);
    }
  }

  // choose out_root by level
  fs::path out_root;
  if (target_level == 5) {
    const unsigned shard = l5_shard_opt.has_value()
      ? *l5_shard_opt
      : pick_l5_shard(zip_name);
    out_root = org_l5_shard_root(org_id, shard);
  } else {
    out_root = org_level_root(org_id, target_level <= 0 ? 1 : target_level);
  }
  ensure_dirs(out_root);

  // build corpus parts (parallel)
  fs::path corpus = tmp / "corpus.jsonl";

  struct ThreadAccum {
    std::vector<UploadResult> docs;
    std::vector<SkippedDoc> skipped;
    std::vector<DocRow> rows;
    std::vector<std::string> doc_ids_for_segment;
  };

  unsigned n_threads = std::min<unsigned>(build_threads, 20u);
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

  // builder options (clip text here to avoid huge JSONL line drop)
  l5::BuildOptions opt;
  opt.segment_name = segment_name_opt;
  opt.max_threads = build_threads;
  opt.ram_limit_bytes = env_u64("PLAGIO_SORT_RAM_BYTES", PLAGIO_SORT_RAM_BYTES_DEFAULT);

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

          ExtractedText ex = extract_text_from_file(
    d.text_path,
    /*assume_normalized=*/text_is_normalized,
    (size_t)opt.max_text_bytes_per_doc
);
          // clip to builder cap (8MiB default) to avoid JSONL line cap drop in builder reader
          clip_text_inplace(ex.text, (size_t)opt.max_text_bytes_per_doc);

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
          ur.org_id = org_id;
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
  for (auto& e : errs) if (e) std::rethrow_exception(e);

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

  if (out.docs.empty()) throw std::runtime_error("no documents converted/extracted for indexing");

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

  // sqlite bulk write
  st.upsert_docs_bulk(rows_all);

  // segment name
  opt.segment_name = segment_name_opt.empty()
      ? (std::string("seg_") + l5::utc_now_compact() + "_" + gen_uuid_v4().substr(0, 8))
      : segment_name_opt;

  // build segment (serialize per-org)
  {
    std::lock_guard<std::mutex> lk(build_mu_for(org_id));
    out.build = l5::build_segment_jsonl(corpus, out_root, opt);
  }

  st.update_last_segment(org_id, doc_ids_for_segment, out.build.segment_name);
  return out;
}

l5::SearchResult L5Service::search_levels(const std::string& org_id,
                                          const std::string& query,
                                          bool query_is_normalized,
                                          const std::vector<int>& levels,
                                          const std::vector<unsigned>& l5_shards,
                                          const l5::SearchOptions& opt) {
  Tombstones ts(org_tombstones(org_id));
  {
    std::lock_guard<std::mutex> lk(tomb_mu_for(org_id));
    ts.load();
  }

  l5::SearchResult out;
  out.query = query;

  std::unordered_map<std::string, l5::Hit> best;
  best.reserve(1024);

  auto merge_res = [&](const l5::SearchResult& r) {
    out.segments_scanned += r.segments_scanned;
    for (auto h : r.hits) {
      if (ts.contains(h.doc_id)) continue;
      if (h.organization_id.empty()) h.organization_id = org_id;
      auto it = best.find(h.doc_id);
      if (it == best.end() || h.C > it->second.C) best[h.doc_id] = std::move(h);
    }
  };

  const unsigned nshards = l5_shards_count();

  for (int lvl : levels) {
    if (lvl >= 1 && lvl <= 4) {
      const fs::path root = org_level_root(org_id, lvl);
      std::error_code ec;
      if (!fs::exists(root, ec)) continue;
      merge_res(l5::search_out_root(root, query, query_is_normalized, opt));
    } else if (lvl == 5) {
      if (!l5_shards.empty()) {
        for (unsigned sh : l5_shards) {
          if (sh >= nshards) continue;
          const fs::path root = org_l5_shard_root(org_id, sh);
          std::error_code ec;
          if (!fs::exists(root, ec)) continue;
          merge_res(l5::search_out_root(root, query, query_is_normalized, opt));
        }
      } else {
        for (unsigned sh = 0; sh < nshards; ++sh) {
          const fs::path root = org_l5_shard_root(org_id, sh);
          std::error_code ec;
          if (!fs::exists(root, ec)) continue;
          merge_res(l5::search_out_root(root, query, query_is_normalized, opt));
        }
      }
    }
  }

  out.hits.reserve(best.size());
  for (auto& kv : best) out.hits.push_back(std::move(kv.second));

  std::sort(out.hits.begin(), out.hits.end(), [](const l5::Hit& a, const l5::Hit& b) {
    return a.C > b.C;
  });
  if (out.hits.size() > opt.topk) out.hits.resize(opt.topk);

  return out;
}

void L5Service::delete_doc(const std::string& org_id, const std::string& key) {
  Storage st(org_sqlite(org_id).string());
  st.init();

  auto row = st.get_by_doc_or_external(org_id, key);
  if (!row) return;
  if (row->deleted) return;

  {
    std::lock_guard<std::mutex> lk(tomb_mu_for(org_id));
    Tombstones ts(org_tombstones(org_id));
    ts.append(row->doc_id);
  }

  st.mark_deleted(org_id, key, utc_now_iso());
}

std::vector<DocRow> L5Service::list_docs(const std::string& org_id, int limit, int offset) {
  Storage st(org_sqlite(org_id).string());
  st.init();
  return st.list_docs(org_id, limit, offset);
}

CompactReport L5Service::compact_small_levels(const std::string& org_id, unsigned fanout) {
  CompactReport rep;
  l5::CompactOptions opt;
  opt.fanout = fanout;

  std::lock_guard<std::mutex> lk(build_mu_for(org_id)); // serialize with build

  // chain compaction until stable
  for (int guard = 0; guard < 10000; ++guard) {
    bool did_any = false;

    for (int lvl = 1; lvl <= 3; ++lvl) {
      const fs::path src = org_level_root(org_id, lvl);
      const fs::path dst = org_level_root(org_id, lvl + 1);

      std::error_code ec;
      if (!fs::exists(src, ec)) continue;

      auto r = l5::compact_once(src, dst, opt);
      if (r.did_compact) {
        did_any = true;
        rep.merges += 1;
        rep.new_segments.push_back("l" + std::to_string(lvl+1) + "/" + r.new_segment_name);
      }
    }

    rep.rounds += 1;
    if (!did_any) break;
  }

  return rep;
}

CompactReport L5Service::compact_l5_shards(const std::string& org_id, unsigned fanout) {
  CompactReport rep;
  l5::CompactOptions opt;
  opt.fanout = fanout;

  std::lock_guard<std::mutex> lk(build_mu_for(org_id)); // serialize with build

  const unsigned nshards = l5_shards_count();

  for (unsigned sh = 0; sh < nshards; ++sh) {
    const fs::path root = org_l5_shard_root(org_id, sh);
    std::error_code ec;
    if (!fs::exists(root, ec)) continue;

    for (int guard = 0; guard < 10000; ++guard) {
      auto r = l5::compact_once(root, root, opt);
      if (!r.did_compact) break;
      rep.merges += 1;
      rep.new_segments.push_back("l5/" + shard_dir_name(sh) + "/" + r.new_segment_name);
    }
  }

  rep.rounds = 1;
  return rep;
}
