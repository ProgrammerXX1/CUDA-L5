#include "routes.h"
#include "route_utils.h"

#include <fstream>

void register_route_segment_docs(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Get(R"(/v1/orgs/([^/]+)/l5/segment_docs)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      if (!req.has_param("segment")) {
        reply_json(res, 400, {{"error","missing segment param"}});
        return;
      }

      unsigned shard = 0;
      if (req.has_param("shard")) shard = (unsigned)std::stoul(req.get_param_value("shard"));

      size_t limit = 100;
      if (req.has_param("limit")) {
        try { limit = (size_t)std::stoull(req.get_param_value("limit")); } catch (...) {}
      }
      if (limit < 1) limit = 1;
      if (limit > 5000) limit = 5000;

      const std::string segment = req.get_param_value("segment");

      const fs::path seg_dir = ctx.data_root / "orgs" / org_id / "index" / "l5" / shard_dir_name(shard) / segment;
      const fs::path docids = seg_dir / "index_native_docids.json";
      const fs::path meta   = seg_dir / "index_native_meta.json";

      std::error_code ec;
      if (!fs::exists(docids, ec) || ec) {
        reply_json(res, 404, {{"error","docids not found"}, {"path", docids.string()}});
        return;
      }

      json docs = read_docids_preview(docids, limit);

      json meta_j = json::object();
      if (fs::exists(meta, ec) && !ec) {
        std::ifstream in(meta);
        if (in) {
          try { in >> meta_j; } catch (...) { meta_j = json::object(); }
        }
      }

      reply_json(res, 200, {
        {"org_id", org_id},
        {"shard", shard},
        {"segment", segment},
        {"seg_dir", seg_dir.string()},
        {"limit", (uint64_t)limit},
        {"meta", meta_j},
        {"docids_preview", docs}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
