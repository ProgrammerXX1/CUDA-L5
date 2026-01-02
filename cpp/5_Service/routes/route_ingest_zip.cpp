// Back_Last/cpp/5_Service/routes/route_ingest_zip.cpp
#include "routes.h"
#include "route_utils.h"

void register_route_ingest_zip(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Post(R"(/v1/orgs/([^/]+)/l5/ingest_zip)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      if (!req.is_multipart_form_data()) {
        reply_json(res, 400, {{"error","expected multipart/form-data"}});
        return;
      }

      auto it = req.files.find("file");
      if (it == req.files.end()) {
        reply_json(res, 400, {{"error","missing file field (file=@zip)"}});
        return;
      }

      const auto& f = it->second;
      if (f.content.size() > ctx.max_zip_upload_bytes) {
        reply_json(res, 413, {{"error","zip too large"}, {"max_bytes",(uint64_t)ctx.max_zip_upload_bytes}});
        return;
      }

      std::optional<unsigned> shard;
      if (req.has_param("l5_shard")) shard = (unsigned)std::stoul(req.get_param_value("l5_shard"));

      std::string segment_name;
      if (req.has_param("segment_name")) segment_name = req.get_param_value("segment_name");

      if (!is_safe_segment_name(segment_name)) {
        reply_json(res, 400, {{"error","bad segment_name"}});
        return;
      }

      bool normalize = true; // default: normalize in core
      if (req.has_param("normalize")) {
        normalize = parse_bool_str(req.get_param_value("normalize"), true);
      }

      auto r = ctx.svc->ingest_l5_zip_build_segment(org_id, f.filename, f.content, shard, segment_name, normalize);

      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"zip_name", f.filename},
        {"shard", r.shard},
        {"normalize", normalize ? 1 : 0},
        {"files_seen", r.files_seen},
        {"files_skipped", r.files_skipped},
        {"docs_indexed", r.docs_indexed},
        {"segment_name", r.build.segment_name},
        {"seg_dir", r.build.seg_dir.string()},
        {"docs", r.build.docs},
        {"post9", r.build.post9},
        {"built_at_utc", r.build.built_at_utc}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
