// Back_Last/cpp/5_Service/core/service_search.cpp
#include "service.h"
#include "core/service_internal.h"

#include <algorithm>
#include <filesystem>
#include <string>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;

namespace { using namespace svc_detail; }

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
