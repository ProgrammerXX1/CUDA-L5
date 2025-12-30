#include "routes.h"
#include "route_utils.h"

#include <unordered_set>

#include "l5/compactor.h"
#include "l5/manifest.h"

void register_route_rebuild_one(httplib::Server& app, ServiceRouteContext& ctx) {
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

      std::lock_guard<std::mutex> lk(*ctx.admin_mu); // serialize with wipe/rebuild

      const fs::path index_root = ctx.data_root / "orgs" / org_id / "index";
      const fs::path l5_root = index_root / "l5";
      ensure_dirs(l5_root);

      const unsigned nshards = l5_shards_count_guess();
      if (out_shard >= nshards) out_shard = 0;

      const fs::path out_root = l5_root / shard_dir_name(out_shard);
      ensure_dirs(out_root);

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

        l5::Manifest empty;
        (void)write_manifest_file(shard_root, empty);
      }

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
}
