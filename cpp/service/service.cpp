#include "service.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include <nlohmann/json.hpp>

#include "l5/format.h"
#include "l5/compactor.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace {

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

} // namespace

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
  opt.segment_name = make_unique_segment_name(); // IMPORTANT: avoid per-second collisions
  opt.max_threads = std::min<unsigned>(std::max(1u, std::thread::hardware_concurrency()),
                                      env_u32("PLAGIO_BUILD_THREADS", 20u));
  opt.ram_limit_bytes = env_u64("PLAGIO_SORT_RAM_BYTES", (100ull << 30));

  const uint64_t l5_min = env_u64("PLAGIO_L5_MIN_BYTES", 0); // 0 => always L1 for this contract
  const bool go_l5 = (l5_min > 0 && (uint64_t)text_in.size() >= l5_min);

  int level = 1;
  std::optional<unsigned> shard;
  fs::path out_root;

  if (go_l5) {
    level = 5;
    shard = pick_l5_shard(org_id + ":" + source_id);
    out_root = org_l5_shard_root(org_id, *shard);
    ensure_dirs(out_root);
  } else {
    level = 1;
    out_root = org_level_root(org_id, 1);
  }

  const fs::path tmp = fs::path(mk_tmp_dir("l5_text"));
  const fs::path corpus = tmp / "corpus.jsonl";

  std::string text = text_in;
  clip_text_utf8_inplace(text, (size_t)opt.max_text_bytes_per_doc);

  {
    std::ofstream out(corpus, std::ios::binary);
    if (!out) throw std::runtime_error("cannot write corpus.jsonl");

    const char* text_is_normalized_flag = "false";

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

  std::string last_segment_rel;
  if (level == 5 && shard.has_value()) {
    last_segment_rel = "l5/" + shard_dir_name(*shard) + "/" + res.build.segment_name;
  } else {
    last_segment_rel = "l1/" + res.build.segment_name;
  }

  st.update_last_segment_by_ids(org_id, std::vector<int64_t>{internal_id}, last_segment_rel);

  row.id = internal_id;
  row.last_segment = last_segment_rel;

  res.doc = row;
  res.indexed_level = level;
  res.l5_shard = shard;

  {
    std::error_code ec;
    fs::remove_all(tmp, ec);
  }

  return res;
}

std::vector<DocRow> L5Service::get_docs_by_internal_ids(const std::string& org_id, const std::vector<int64_t>& ids) {
  Storage st(org_sqlite(org_id).string());
  st.init();
  return st.get_by_internal_ids(org_id, ids);
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
