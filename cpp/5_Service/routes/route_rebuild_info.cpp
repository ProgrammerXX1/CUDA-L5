// Back_Last/cpp/5_Service/routes/route_rebuild_info.cpp
#include "routes.h"
#include "route_utils.h"

#include "l5/manifest.h"

void register_route_rebuild_info(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Get(R"(/v1/orgs/([^/]+)/l5/rebuild_info)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      const fs::path l5_root = ctx.data_root / "orgs" / org_id / "index" / "l5";
      const fs::path p = l5_root / "rebuild_last.json";

      std::string err;
      std::string txt = read_text_file(p, &err);
      if (txt.empty()) { reply_json(res, 404, {{"error", err}, {"path", p.string()}}); return; }

      json j;
      try { j = json::parse(txt); }
      catch (...) { reply_json(res, 500, {{"error","failed parsing rebuild_last.json"}}); return; }

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
}
