#include "service.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <zip.h>
#include <nlohmann/json.hpp>

#include "l5/format.h"
#include "l5/compactor.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

// -------------------- constants --------------------
constexpr unsigned  BUILD_THREADS_DEFAULT      = 20u;
constexpr uint64_t  SORT_RAM_BYTES_DEFAULT    = (100ull << 30);   // 100 GiB

constexpr size_t    ZIP_MAX_FILES_DEFAULT     = 20000;
constexpr uint64_t  ZIP_MAX_TOTAL_BYTES_DEF   = (10ull << 30);    // 10 GiB uncompressed safety cap

constexpr unsigned  CONVERT_PROCS_FALLBACK    = 20u;  // max concurrent soffice procs
constexpr unsigned  CONVERT_BATCH_DEFAULT     = 200u; // files per soffice invocation
constexpr unsigned  EXTRACT_THREADS_CAP       = 20u;

// -------------------- utils --------------------
static void ensure_dirs(const fs::path& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  if (ec) throw std::runtime_error("mkdir failed: " + p.string() + " err=" + ec.message());
}

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

static std::string shard_dir_name(unsigned shard) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "s%02u", shard);
  return std::string(buf);
}

static std::string rand_hex8() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint32_t> dis;
  uint32_t v = dis(gen);
  static const char* hex = "0123456789abcdef";
  std::string s(8, '0');
  for (int i = 7; i >= 0; --i) { s[i] = hex[v & 0xF]; v >>= 4; }
  return s;
}

static std::string make_unique_segment_name() {
  return std::string("seg_") + l5::utc_now_compact() + "_" + rand_hex8();
}

// -------------------- UTF-8 safe clip --------------------
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

static void clip_text_utf8_inplace(std::string& s, size_t max_bytes) {
  if (max_bytes == 0) return;
  if (s.size() <= max_bytes) return;
  const size_t cut = utf8_safe_prefix_len(s, max_bytes);
  s.resize(cut);
}

// -------------------- CP1251 decode (best effort) --------------------
static bool utf8_is_valid(std::string_view s) {
  size_t i = 0;
  while (i < s.size()) {
    const unsigned char c = (unsigned char)s[i];

    size_t len = 1;
    if (c < 0x80) len = 1;
    else if (c >= 0xC2 && c <= 0xDF) len = 2;
    else if (c >= 0xE0 && c <= 0xEF) len = 3;
    else if (c >= 0xF0 && c <= 0xF4) len = 4;
    else return false;

    if (i + len > s.size()) return false;

    for (size_t j = 1; j < len; ++j) {
      const unsigned char cc = (unsigned char)s[i + j];
      if ((cc & 0xC0) != 0x80) return false;
    }

    if (len == 3) {
      const unsigned char c1 = (unsigned char)s[i + 1];
      if (c == 0xE0 && c1 < 0xA0) return false;
      if (c == 0xED && c1 >= 0xA0) return false;
    } else if (len == 4) {
      const unsigned char c1 = (unsigned char)s[i + 1];
      if (c == 0xF0 && c1 < 0x90) return false;
      if (c == 0xF4 && c1 > 0x8F) return false;
    }

    i += len;
  }
  return true;
}

static inline void append_utf8(uint32_t cp, std::string& out) {
  if (cp <= 0x7F) out.push_back((char)cp);
  else if (cp <= 0x7FF) {
    out.push_back((char)(0xC0 | ((cp >> 6) & 0x1F)));
    out.push_back((char)(0x80 | (cp & 0x3F)));
  } else if (cp <= 0xFFFF) {
    out.push_back((char)(0xE0 | ((cp >> 12) & 0x0F)));
    out.push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
    out.push_back((char)(0x80 | (cp & 0x3F)));
  } else {
    out.push_back((char)(0xF0 | ((cp >> 18) & 0x07)));
    out.push_back((char)(0x80 | ((cp >> 12) & 0x3F)));
    out.push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
    out.push_back((char)(0x80 | (cp & 0x3F)));
  }
}

static uint16_t cp1251_to_unicode(unsigned char c) {
  if (c < 0x80) return (uint16_t)c;
  static const uint16_t tbl[128] = {
    0x0402,0x0403,0x201A,0x0453,0x201E,0x2026,0x2020,0x2021,
    0x20AC,0x2030,0x0409,0x2039,0x040A,0x040C,0x040B,0x040F,
    0x0452,0x2018,0x2019,0x201C,0x201D,0x2022,0x2013,0x2014,
    0x0000,0x2122,0x0459,0x203A,0x045A,0x045C,0x045B,0x045F,
    0x00A0,0x040E,0x045E,0x0408,0x00A4,0x0490,0x00A6,0x00A7,
    0x0401,0x00A9,0x0404,0x00AB,0x00AC,0x00AD,0x00AE,0x0407,
    0x00B0,0x00B1,0x0406,0x0456,0x0491,0x00B5,0x00B6,0x00B7,
    0x0451,0x2116,0x0454,0x00BB,0x0458,0x0405,0x0455,0x0457,
    0x0410,0x0411,0x0412,0x0413,0x0414,0x0415,0x0416,0x0417,
    0x0418,0x0419,0x041A,0x041B,0x041C,0x041D,0x041E,0x041F,
    0x0420,0x0421,0x0422,0x0423,0x0424,0x0425,0x0426,0x0427,
    0x0428,0x0429,0x042A,0x042B,0x042C,0x042D,0x042E,0x042F,
    0x0430,0x0431,0x0432,0x0433,0x0434,0x0435,0x0436,0x0437,
    0x0438,0x0439,0x043A,0x043B,0x043C,0x043D,0x043E,0x043F,
    0x0440,0x0441,0x0442,0x0443,0x0444,0x0445,0x0446,0x0447,
    0x0448,0x0449,0x044A,0x044B,0x044C,0x044D,0x044E,0x044F
  };
  const uint16_t cp = tbl[c - 0x80];
  return cp ? cp : (uint16_t)'?';
}

static std::string cp1251_to_utf8(std::string_view s) {
  std::string out;
  out.reserve(s.size() * 2);
  for (unsigned char c : s) {
    append_utf8((uint32_t)cp1251_to_unicode(c), out);
  }
  return out;
}

static std::string read_file_prefix(const fs::path& p, size_t max_bytes) {
  std::ifstream in(p, std::ios::binary);
  if (!in) return {};
  const size_t extra = 64;
  const size_t want = max_bytes > 0 ? (max_bytes + extra) : (8u << 20);
  std::string buf;
  buf.resize(want);
  in.read(buf.data(), (std::streamsize)want);
  buf.resize((size_t)in.gcount());
  return buf;
}

static std::string read_text_utf8_best_effort(const fs::path& p, size_t cap) {
  std::string raw = read_file_prefix(p, cap);
  if (raw.empty()) return {};
  if (utf8_is_valid(raw)) {
    clip_text_utf8_inplace(raw, cap);
    return raw;
  }
  std::string conv = cp1251_to_utf8(std::string_view(raw.data(), std::min(raw.size(), cap)));
  clip_text_utf8_inplace(conv, cap);
  return conv;
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

// -------------------- zip slip protection --------------------
static bool zip_entry_name_is_safe(const std::string& name) {
  if (name.empty()) return false;
  if (name.find('\0') != std::string::npos) return false;
  if (!name.empty() && name[0] == '/') return false;
  if (name.find('\\') != std::string::npos) return false;

  fs::path rel = fs::path(name).lexically_normal();
  if (rel.empty()) return false;
  if (rel.is_absolute()) return false;

  for (const auto& part : rel) {
    if (part == "..") return false;
  }
  return true;
}

static void unzip_libzip_safe_buffer(const std::string& zip_bytes,
                                     const fs::path& dst_dir,
                                     size_t max_files,
                                     uint64_t max_total_bytes) {
  ensure_dirs(dst_dir);

  zip_error_t ze;
  zip_error_init(&ze);

  zip_source_t* zs = zip_source_buffer_create(zip_bytes.data(), zip_bytes.size(), 0, &ze);
  if (!zs) { zip_error_fini(&ze); throw std::runtime_error("zip_source_buffer_create failed"); }

  zip_t* za = zip_open_from_source(zs, ZIP_RDONLY, &ze);
  if (!za) { zip_source_free(zs); zip_error_fini(&ze); throw std::runtime_error("zip_open_from_source failed"); }

  auto za_guard = std::unique_ptr<zip_t, decltype(&zip_close)>(za, &zip_close);
  zip_error_fini(&ze);

  const zip_int64_t n = zip_get_num_entries(za, 0);
  if (n < 0) throw std::runtime_error("zip_get_num_entries failed");
  if ((size_t)n > max_files) throw std::runtime_error("zip too many entries");

  uint64_t total = 0;

  for (zip_uint64_t i = 0; i < (zip_uint64_t)n; ++i) {
    zip_stat_t st;
    zip_stat_init(&st);
    if (zip_stat_index(za, i, 0, &st) != 0) continue;

    std::string name = st.name ? st.name : "";
    if (name.empty()) continue;

    // dir
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

    std::ofstream fout(out, std::ios::binary);
    if (!fout) { zip_fclose(zf); throw std::runtime_error("cannot write: " + out.string()); }

    char buf[1 << 16];
    while (true) {
      zip_int64_t rd = zip_fread(zf, buf, sizeof(buf));
      if (rd < 0) { zip_fclose(zf); throw std::runtime_error("zip_fread failed for entry: " + name); }
      if (rd == 0) break;
      fout.write(buf, (std::streamsize)rd);
      if (!fout) { zip_fclose(zf); throw std::runtime_error("write failed: " + out.string()); }
    }
    zip_fclose(zf);
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

  ensure_dirs(conv_out);
  ensure_dirs(profiles_base);

  const size_t n = inputs.size();
  const size_t per = (n + procs - 1) / procs;

  std::vector<std::thread> th;
  th.reserve(procs);
  std::vector<std::exception_ptr> errs(procs);

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

static fs::path replace_ext_txt(fs::path p) {
  p.replace_extension(".txt");
  return p;
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

static bool copy_file_best_effort(const fs::path& src, const fs::path& dst) {
  std::error_code ec;
  fs::create_directories(dst.parent_path(), ec);
  ec.clear();
  fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
  return !ec;
}

} // namespace

// -------------------- L5Service --------------------

L5Service::L5Service(fs::path data_root) : data_root_(std::move(data_root)) {
  ensure_dirs(data_root_);
  ensure_dirs(data_root_ / "orgs");
}

fs::path L5Service::org_root(const std::string& org) const { return data_root_ / "orgs" / org; }
fs::path L5Service::org_index_base(const std::string& org) const { return org_root(org) / "index"; }

fs::path L5Service::org_level_root(const std::string& org, int level) const {
  if (level < 1) level = 1;
  if (level > 4) level = 4;
  return org_index_base(org) / ("l" + std::to_string(level));
}

fs::path L5Service::org_l5_shard_root(const std::string& org, unsigned shard) const {
  return org_index_base(org) / "l5" / shard_dir_name(shard);
}

fs::path L5Service::org_sqlite(const std::string& org) const { return org_root(org) / "meta.sqlite"; }
fs::path L5Service::org_tombstones(const std::string& org) const { return org_root(org) / "tombstones.jsonl"; }

unsigned L5Service::l5_shards_count() const {
  unsigned n = env_u32("PLAGIO_L5_SHARDS", 32u);
  if (n < 1) n = 1;
  if (n > 256) n = 256;
  return n;
}

unsigned L5Service::pick_l5_shard(const std::string& seed) const {
  const unsigned n = l5_shards_count();
  const size_t h = std::hash<std::string>{}(seed);
  return (unsigned)(h % n);
}

std::string L5Service::utc_now_iso_utc() {
  using namespace std::chrono;

  const auto now = system_clock::now();
  const auto t = system_clock::to_time_t(now);
  std::tm tm{};
#if defined(_WIN32)
  gmtime_s(&tm, &t);
#else
  gmtime_r(&t, &tm);
#endif

  const auto us = duration_cast<microseconds>(now.time_since_epoch()).count() % 1000000;

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
  oss << "." << std::setw(6) << std::setfill('0') << us;
  oss << "+00:00";
  return oss.str();
}

std::string L5Service::mk_tmp_dir(const std::string& prefix) {
  const auto base = fs::temp_directory_path();
  const uint64_t t = (uint64_t)std::time(nullptr);

  for (int i = 0; i < 200; ++i) {
    fs::path p = base / (prefix + "_" + std::to_string(t) + "_" + std::to_string(i));
    std::error_code ec;
    if (fs::create_directories(p, ec) && !ec) return p.string();
  }
  throw std::runtime_error("cannot create temp dir");
}

// -------------------- index_text_document --------------------
IndexTextResult L5Service::index_text_document(const std::string& org_id,
                                               const std::string& source_id,
                                               const std::string& file_name,
                                               const std::string& title,
                                               const std::string& author,
                                               const std::string& created_at,
                                               const std::string& text_in) {
  if (org_id.empty()) throw std::invalid_argument("org_id is empty");
  if (source_id.empty()) throw std::invalid_argument("document_id/source_id is empty");
  if (file_name.empty()) throw std::invalid_argument("file_name is empty");

  ensure_dirs(org_root(org_id));
  ensure_dirs(org_index_base(org_id));
  ensure_dirs(org_level_root(org_id, 1));
  ensure_dirs(org_level_root(org_id, 2));
  ensure_dirs(org_level_root(org_id, 3));
  ensure_dirs(org_level_root(org_id, 4));
  ensure_dirs(org_index_base(org_id) / "l5");

  Storage st(org_sqlite(org_id).string());
  st.init();

  DocRow row;
  row.org_id = org_id;
  row.source_id = source_id;
  row.file_name = file_name;
  row.title = title;
  row.author = author;
  row.created_at = created_at;
  row.stored_at_utc = utc_now_iso_utc();
  row.deleted = 0;
  row.deleted_at_utc = "";
  row.last_segment = "";

  const int64_t internal_id = st.upsert_doc_get_id(row);

  l5::BuildOptions opt;
  opt.segment_name = make_unique_segment_name();
  opt.max_threads = std::min<unsigned>(std::max(1u, std::thread::hardware_concurrency()),
                                      env_u32("PLAGIO_BUILD_THREADS", BUILD_THREADS_DEFAULT));
  opt.ram_limit_bytes = env_u64("PLAGIO_SORT_RAM_BYTES", SORT_RAM_BYTES_DEFAULT);

  const fs::path out_root = org_level_root(org_id, 1);

  const fs::path tmp = fs::path(mk_tmp_dir("l5_text"));
  const fs::path corpus = tmp / "corpus.jsonl";

  std::string text = text_in;
  clip_text_utf8_inplace(text, (size_t)opt.max_text_bytes_per_doc);

  {
    std::ofstream out(corpus, std::ios::binary);
    if (!out) throw std::runtime_error("cannot write corpus.jsonl");

    const char* text_is_normalized_flag = "false"; // core will normalize (stable contract)

    out
      << "{\"doc_id\":" << json(std::to_string(internal_id)).dump()
      << ",\"organization_id\":" << json(org_id).dump()
      << ",\"external_id\":" << json(source_id).dump()
      << ",\"source_path\":" << json("").dump()
      << ",\"source_name\":" << json(file_name).dump()
      << ",\"text\":" << json(text).dump()
      << ",\"text_is_normalized\":" << text_is_normalized_flag
      << "}\n";

    out.flush();
    if (!out) throw std::runtime_error("write failed corpus.jsonl");
  }

  IndexTextResult res;
  {
    std::lock_guard<std::mutex> lk(build_mu_for(org_id));
    res.build = l5::build_segment_jsonl(corpus, out_root, opt);
  }

  const std::string last_segment_rel = "l1/" + res.build.segment_name;
  st.update_last_segment_by_ids(org_id, std::vector<int64_t>{internal_id}, last_segment_rel);

  row.id = internal_id;
  row.last_segment = last_segment_rel;
  res.doc = row;
  res.indexed_level = 1;
  res.l5_shard = std::nullopt;

  {
    std::error_code ec;
    fs::remove_all(tmp, ec);
  }
  return res;
}

// -------------------- ingest_l5_zip_build_segment (parallel) --------------------
L5ZipIngestResult L5Service::ingest_l5_zip_build_segment(const std::string& org_id,
                                                         const std::string& zip_name,
                                                         const std::string& zip_bytes,
                                                         std::optional<unsigned> l5_shard_opt,
                                                         const std::string& segment_name_opt,bool normalize) {
  if (org_id.empty()) throw std::invalid_argument("org_id is empty");

  ensure_dirs(org_root(org_id));
  ensure_dirs(org_index_base(org_id));
  ensure_dirs(org_index_base(org_id) / "l5");

  Storage st(org_sqlite(org_id).string());
  st.init();

  const unsigned shard = l5_shard_opt.has_value() ? *l5_shard_opt : pick_l5_shard(zip_name);
  const fs::path out_root = org_l5_shard_root(org_id, shard);
  ensure_dirs(out_root);

  l5::BuildOptions opt;
  opt.segment_name = segment_name_opt.empty() ? make_unique_segment_name() : segment_name_opt;
  opt.max_threads = std::min<unsigned>(std::max(1u, std::thread::hardware_concurrency()),
                                       env_u32("PLAGIO_BUILD_THREADS", BUILD_THREADS_DEFAULT));
  opt.ram_limit_bytes = env_u64("PLAGIO_SORT_RAM_BYTES", SORT_RAM_BYTES_DEFAULT);

  const size_t   ZIP_MAX_FILES = (size_t)env_u32("PLAGIO_L5_ZIP_MAX_FILES", (unsigned)ZIP_MAX_FILES_DEFAULT);
  const uint64_t ZIP_MAX_TOTAL = env_u64("PLAGIO_L5_ZIP_MAX_TOTAL_BYTES", ZIP_MAX_TOTAL_BYTES_DEF);

  const fs::path tmp = fs::path(mk_tmp_dir("l5_zip"));
  const fs::path unpack_dir = tmp / "unpacked";
  const fs::path conv_src = tmp / "conv_src";
  const fs::path conv_out = tmp / "conv_out";
  const fs::path profiles = tmp / "lo_profiles";

  ensure_dirs(unpack_dir);
  ensure_dirs(conv_src);
  ensure_dirs(conv_out);
  ensure_dirs(profiles);

  // 1) unzip to disk (safe)
  unzip_libzip_safe_buffer(zip_bytes, unpack_dir, ZIP_MAX_FILES, ZIP_MAX_TOTAL);

  struct Pending {
    std::string rel_in_zip;     // path inside zip
    std::string source_id;      // zip_name::rel
    std::string file_name;      // basename
    fs::path src_path;          // unpacked path
    bool needs_convert{false};
    fs::path text_path;         // final txt path to read
    int64_t internal_id{0};
    std::string upload_rel;
  };

  std::vector<Pending> pending;
  pending.reserve(4096);

  // 2) scan files
  {
    std::error_code ec;
    fs::recursive_directory_iterator it(unpack_dir, fs::directory_options::skip_permission_denied, ec);
    fs::recursive_directory_iterator end;
    for (; it != end; it.increment(ec)) {
      if (ec) { ec.clear(); continue; }
      if (!it->is_regular_file(ec) || ec) { ec.clear(); continue; }

      const fs::path p = it->path();
      const std::string ext = lower_ext(p);

      const bool is_txt  = (ext == ".txt");
      const bool is_doc  = (ext == ".doc");
      const bool is_docx = (ext == ".docx");
      if (!is_txt && !is_doc && !is_docx) continue;

      Pending d;
      d.src_path = p;
      d.file_name = basename_of(p);

      std::string rel = fs::relative(p, unpack_dir, ec).generic_string();
      if (ec || rel.empty()) { ec.clear(); rel = d.file_name; }
      d.rel_in_zip = rel;
      d.source_id = zip_name + "::" + rel;
      d.needs_convert = !is_txt;

      pending.push_back(std::move(d));
    }
  }

  L5ZipIngestResult rr;
  rr.shard = shard;
  rr.files_seen = (uint64_t)pending.size();

  if (pending.empty()) throw std::runtime_error("zip has no supported files (.txt/.doc/.docx)");

  // 3) pre-assign internal ids in sqlite (single-thread)
  for (auto& d : pending) {
    DocRow row;
    row.org_id = org_id;
    row.source_id = d.source_id;
    row.file_name = d.file_name;
    row.title = "";
    row.author = "";
    row.created_at = "";
    row.stored_at_utc = utc_now_iso_utc();
    row.deleted = 0;
    row.deleted_at_utc = "";
    row.last_segment = "";
    d.internal_id = st.upsert_doc_get_id(row);
  }
  const fs::path uploads_dir = org_root(org_id) / "uploads";
  ensure_dirs(uploads_dir);

  for (auto& d : pending) {
    // сохраняем оригинальный файл (txt/doc/docx) как есть
    const fs::path dst = uploads_dir / d.file_name; // basename
    if (copy_file_best_effort(d.src_path, dst)) {
      d.upload_rel = (fs::path("uploads") / d.file_name).generic_string();
    } else {
      // если не удалось сохранить — не ломаем индексацию, просто оставляем пустым
      d.upload_rel.clear();
    }
  }
  // 4) prepare doc/docx for conversion
  for (auto& d : pending) {
    if (!d.needs_convert) {
      d.text_path = d.src_path; // txt directly
      continue;
    }
    const fs::path unique_in = conv_src / (std::to_string(d.internal_id) + "_" + d.file_name);
    copy_file_binary(d.src_path, unique_in);
    d.text_path = conv_out / replace_ext_txt(unique_in.filename());
  }

  // 5) convert in parallel (doc/docx -> txt)
  {
    bool has_any = false;
    for (auto it = fs::directory_iterator(conv_src); it != fs::directory_iterator(); ++it) {
      std::error_code ec;
      if (it->is_regular_file(ec) && !ec) { has_any = true; break; }
    }
    if (has_any) {
      unsigned hw = std::thread::hardware_concurrency();
      if (hw == 0) hw = 4;

      const unsigned build_thr = opt.max_threads;
      const unsigned def_procs = std::min<unsigned>(CONVERT_PROCS_FALLBACK, build_thr);
      unsigned procs = env_u32("PLAGIO_L5_CONVERT_PROCS", def_procs);
      unsigned batch = env_u32("PLAGIO_L5_CONVERT_BATCH", CONVERT_BATCH_DEFAULT);

      if (procs == 0) procs = 1;
      if (procs > hw) procs = hw;

      soffice_convert_parallel(conv_src, conv_out, profiles, procs, batch);
    }
  }

  // 6) build corpus parts in parallel
  const unsigned hw = std::max(1u, std::thread::hardware_concurrency());
  const unsigned build_thr = std::min<unsigned>(hw, opt.max_threads);

  unsigned n_threads = env_u32("PLAGIO_L5_EXTRACT_THREADS", std::min<unsigned>(build_thr, EXTRACT_THREADS_CAP));
  if (n_threads == 0) n_threads = 1;
  if (n_threads > (unsigned)pending.size()) n_threads = (unsigned)pending.size();
  if (n_threads == 0) n_threads = 1;

  std::vector<fs::path> part_paths;
  part_paths.reserve(n_threads);
  for (unsigned t = 0; t < n_threads; ++t) {
    part_paths.push_back(tmp / ("corpus_part_" + std::to_string(t) + ".jsonl"));
  }

  struct ThreadAccum {
    uint64_t skipped{0};
    std::vector<int64_t> indexed_ids;
  };

  std::vector<ThreadAccum> acc(n_threads);
  std::vector<std::thread> workers;
  workers.reserve(n_threads);
  std::vector<std::exception_ptr> errs(n_threads);

  std::atomic<size_t> next{0};

  const std::string org_j = json(org_id).dump();
  const char* text_is_normalized_flag = normalize ? "false" : "true";
  const size_t cap = (size_t)opt.max_text_bytes_per_doc;

  for (unsigned t = 0; t < n_threads; ++t) {
    workers.emplace_back([&, t]() {
      try {
        std::ofstream outp(part_paths[t], std::ios::binary);
        if (!outp) throw std::runtime_error("cannot open corpus part: " + part_paths[t].string());

        auto& A = acc[t];
        A.indexed_ids.reserve(1024);

        while (true) {
          size_t i = next.fetch_add(1, std::memory_order_relaxed);
          if (i >= pending.size()) break;

          auto& d = pending[i];

          if (d.needs_convert) {
            std::error_code ec;
            if (!fs::exists(d.text_path, ec) || ec) { A.skipped++; continue; }
          }

          std::string text = read_text_utf8_best_effort(d.text_path, cap);
          if (text.empty()) { A.skipped++; continue; }

          // write jsonl line
          outp
            << "{\"doc_id\":" << json(std::to_string(d.internal_id)).dump()
            << ",\"organization_id\":" << org_j
            << ",\"external_id\":" << json(d.source_id).dump()
            << ",\"source_path\":" << json(d.upload_rel).dump()
            << ",\"source_name\":" << json(d.file_name).dump()
            << ",\"text\":" << json(text).dump()
            << ",\"text_is_normalized\":" << text_is_normalized_flag
            << "}\n";

          A.indexed_ids.push_back(d.internal_id);
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

  // merge parts -> corpus.jsonl
  const fs::path corpus = tmp / "corpus.jsonl";
  {
    std::ofstream corpus_out(corpus, std::ios::binary);
    if (!corpus_out) throw std::runtime_error("cannot open corpus.jsonl");

    for (auto& pp : part_paths) {
      std::ifstream in(pp, std::ios::binary);
      if (!in) continue;
      corpus_out << in.rdbuf();
    }
    corpus_out.flush();
    if (!corpus_out) throw std::runtime_error("failed writing corpus.jsonl");
  }

  std::vector<int64_t> indexed_ids;
  uint64_t skipped = 0;
  for (auto& a : acc) {
    skipped += a.skipped;
    for (auto id : a.indexed_ids) indexed_ids.push_back(id);
  }

  rr.files_skipped = skipped;
  rr.docs_indexed = (uint64_t)indexed_ids.size();
  if (rr.docs_indexed == 0) throw std::runtime_error("no documents extracted for indexing (all skipped)");

  // 7) build segment
  {
    std::lock_guard<std::mutex> lk(build_mu_for(org_id));
    rr.build = l5::build_segment_jsonl(corpus, out_root, opt);
  }

  // 8) update last_segment for successfully indexed docs
  const std::string last_segment_rel = "l5/" + shard_dir_name(shard) + "/" + rr.build.segment_name;
  st.update_last_segment_by_ids(org_id, indexed_ids, last_segment_rel);

  // cleanup tmp
  {
    std::error_code ec;
    fs::remove_all(tmp, ec);
  }

  return rr;
}

// -------------------- get_docs_by_internal_ids --------------------
std::vector<DocRow> L5Service::get_docs_by_internal_ids(const std::string& org_id, const std::vector<int64_t>& ids) {
  Storage st(org_sqlite(org_id).string());
  st.init();
  return st.get_by_internal_ids(org_id, ids);
}

// -------------------- search_levels --------------------
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

  auto merge_res = [&](l5::SearchResult&& r, const std::string& prefix) {
    out.segments_scanned += r.segments_scanned;
    for (auto& h : r.hits) {
      if (ts.contains(h.doc_id)) continue;

      if (!prefix.empty()) {
        if (h.meta_path.rfind(prefix, 0) != 0) {
          h.meta_path = prefix + h.meta_path;
        }
      }

      if (h.organization_id.empty()) h.organization_id = org_id;

      auto it = best.find(h.doc_id);
      if (it == best.end() || h.C > it->second.C) best[h.doc_id] = std::move(h);
    }
  };

  std::error_code ec;
  const unsigned nshards = l5_shards_count();

  for (int lvl : levels) {
    if (lvl >= 1 && lvl <= 4) {
      const fs::path root = org_level_root(org_id, lvl);
      if (!fs::exists(root, ec)) { ec.clear(); continue; }
      auto r = l5::search_out_root(root, query, query_is_normalized, opt);
      merge_res(std::move(r), "l" + std::to_string(lvl) + "/");
    } else if (lvl == 5) {
      if (!l5_shards.empty()) {
        for (unsigned sh : l5_shards) {
          if (sh >= nshards) continue;
          const fs::path root = org_l5_shard_root(org_id, sh);
          if (!fs::exists(root, ec)) { ec.clear(); continue; }
          auto r = l5::search_out_root(root, query, query_is_normalized, opt);
          merge_res(std::move(r), "l5/" + shard_dir_name(sh) + "/");
        }
      } else {
        for (unsigned sh = 0; sh < nshards; ++sh) {
          const fs::path root = org_l5_shard_root(org_id, sh);
          if (!fs::exists(root, ec)) { ec.clear(); continue; }
          auto r = l5::search_out_root(root, query, query_is_normalized, opt);
          merge_res(std::move(r), "l5/" + shard_dir_name(sh) + "/");
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

// -------------------- compaction --------------------
CompactReport L5Service::compact_small_levels(const std::string& org_id, unsigned fanout) {
  CompactReport rep;
  l5::CompactOptions opt;
  opt.fanout = fanout;

  std::lock_guard<std::mutex> lk(build_mu_for(org_id));

  for (int guard = 0; guard < 10000; ++guard) {
    bool did_any = false;

    for (int lvl = 1; lvl <= 3; ++lvl) {
      const fs::path src = org_level_root(org_id, lvl);
      const fs::path dst = org_level_root(org_id, lvl + 1);

      std::error_code ec;
      if (!fs::exists(src, ec)) { ec.clear(); continue; }

      auto r = l5::compact_once(src, dst, opt);
      if (r.did_compact) {
        did_any = true;
        rep.merges += 1;
        rep.new_segments.push_back("l" + std::to_string(lvl + 1) + "/" + r.new_segment_name);
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

  std::lock_guard<std::mutex> lk(build_mu_for(org_id));

  const unsigned nshards = l5_shards_count();

  for (unsigned sh = 0; sh < nshards; ++sh) {
    const fs::path root = org_l5_shard_root(org_id, sh);
    std::error_code ec;
    if (!fs::exists(root, ec)) { ec.clear(); continue; }

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
