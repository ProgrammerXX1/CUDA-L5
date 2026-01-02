// Back_Last/cpp/5_Service/routes/route_ingest_zip.cpp
#include "routes.h"
#include "route_utils.h"

#include <fstream>
#include "core/job_queue.h"

void register_route_ingest_zip(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Post(R"(/v1/orgs/([^/]+)/l5/ingest_zip)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }
      if (!ctx.q) { reply_json(res, 500, {{"error","job queue not configured"}}); return; }

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

      // 1) enqueue job first
      json payload = {
        {"org_id", org_id},
        {"zip_name", f.filename},
        {"normalize", normalize ? 1 : 0},
        {"segment_name", segment_name},
      };
      if (shard.has_value()) payload["l5_shard"] = (uint64_t)(*shard);

      const int64_t job_id = ctx.q->enqueue("INGEST_L5_ZIP", payload.dump(), /*priority*/10);

      // 2) persist zip to disk under org/jobs/job_<id>/
      const fs::path job_dir = ctx.data_root / "orgs" / org_id / "jobs" / ("job_" + std::to_string(job_id));
      ensure_dirs(job_dir);
      const fs::path zip_path = job_dir / "input.zip";
      {
        std::ofstream out(zip_path, std::ios::binary);
        if (!out) { reply_json(res, 500, {{"error","cannot write zip"}, {"path", zip_path.string()}}); return; }
        out.write(f.content.data(), (std::streamsize)f.content.size());
        out.flush();
        if (!out) { reply_json(res, 500, {{"error","write zip failed"}, {"path", zip_path.string()}}); return; }
      }

      // 3) update payload with relative zip_path (portable)
      payload["zip_path"] = (fs::path("orgs") / org_id / "jobs" / ("job_" + std::to_string(job_id)) / "input.zip").generic_string();
      (void)ctx.q->update_payload(job_id, payload.dump());

      reply_json(res, 202, {
        {"ok", true},
        {"org_id", org_id},
        {"job_id", std::to_string(job_id)},
        {"status", "queued"},
        {"type", "INGEST_L5_ZIP"}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
