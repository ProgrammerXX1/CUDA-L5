// Back_Last/cpp/5_Service/core/service_compact.cpp
#include "service.h"
#include "core/service_internal.h"

#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace { using namespace svc_detail; }

std::vector<DocRow> L5Service::get_docs_by_internal_ids(const std::string& org_id, const std::vector<int64_t>& ids) {
  Storage st(org_sqlite(org_id).string());
  st.init();
  return st.get_by_internal_ids(org_id, ids);
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
