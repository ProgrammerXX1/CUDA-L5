// Back_Last/cpp/5_Service/core/service_common.cpp
#include "service.h"
#include "core/service_internal.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "l5/format.h"

namespace fs = std::filesystem;

namespace svc_detail {

// -------------------- utils --------------------
void ensure_dirs(const fs::path& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  if (ec) throw std::runtime_error("mkdir failed: " + p.string() + " err=" + ec.message());
}

unsigned env_u32(const char* k, unsigned defv) {
  const char* s = std::getenv(k);
  if (!s || !*s) return defv;
  char* end = nullptr;
  unsigned long v = std::strtoul(s, &end, 10);
  if (!end || *end != '\0') return defv;
  return (unsigned)v;
}

uint64_t env_u64(const char* k, uint64_t defv) {
  const char* s = std::getenv(k);
  if (!s || !*s) return defv;
  char* end = nullptr;
  unsigned long long v = std::strtoull(s, &end, 10);
  if (!end || *end != '\0') return defv;
  return (uint64_t)v;
}

std::string to_lower_copy(std::string s) {
  for (auto& c : s) c = (char)std::tolower((unsigned char)c);
  return s;
}

std::string lower_ext(const fs::path& p) {
  return to_lower_copy(p.extension().string());
}

std::string basename_of(const fs::path& p) {
  auto fn = p.filename().string();
  return fn.empty() ? std::string("file") : fn;
}

std::string shard_dir_name(unsigned shard) {
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

std::string make_unique_segment_name() {
  return std::string("seg_") + l5::utc_now_compact() + "_" + rand_hex8();
}

// -------------------- UTF-8 safe clip --------------------
size_t utf8_safe_prefix_len(std::string_view s, size_t max_bytes) {
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

void clip_text_utf8_inplace(std::string& s, size_t max_bytes) {
  if (max_bytes == 0) return;
  if (s.size() <= max_bytes) return;
  const size_t cut = utf8_safe_prefix_len(s, max_bytes);
  s.resize(cut);
}

} // namespace svc_detail

namespace {

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

} // namespace

namespace svc_detail {

std::string read_text_utf8_best_effort(const fs::path& p, size_t cap) {
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

} // namespace svc_detail

namespace { using namespace svc_detail; }

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
