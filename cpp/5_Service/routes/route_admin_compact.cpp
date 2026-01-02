// Back_Last/cpp/5_Service/routes/route_admin_compact.cpp
#include "routes.h"
#include "route_utils.h"

#include "core/job_queue.h"

void register_route_admin_compact(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Post(R"(/v1/orgs/([^/]+)/admin/compact_small)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      unsigned fanout = 20;
      if (req.has_param("fanout")) fanout = (unsigned)std::stoul(req.get_param_value("fanout"));
      if (fanout < 2) fanout = 2;

      if (!ctx.q) { reply_json(res, 500, {{"error","job queue not configured"}}); return; }
      json payload = {{"org_id", org_id}, {"fanout", (uint64_t)fanout}};
      const int64_t job_id = ctx.q->enqueue("COMPACT_SMALL", payload.dump(), /*priority*/1);
      reply_json(res, 202, {{"ok", true}, {"org_id", org_id}, {"job_id", std::to_string(job_id)}, {"status","queued"}, {"type","COMPACT_SMALL"}});

    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  app.Post(R"(/v1/orgs/([^/]+)/admin/compact_l5)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      unsigned fanout = 2;
      if (req.has_param("fanout")) fanout = (unsigned)std::stoul(req.get_param_value("fanout"));
      if (fanout < 2) fanout = 2;

      if (!ctx.q) { reply_json(res, 500, {{"error","job queue not configured"}}); return; }
      json payload = {{"org_id", org_id}, {"fanout", (uint64_t)fanout}};
      const int64_t job_id = ctx.q->enqueue("COMPACT_L5", payload.dump(), /*priority*/1);
      reply_json(res, 202, {{"ok", true}, {"org_id", org_id}, {"job_id", std::to_string(job_id)}, {"status","queued"}, {"type","COMPACT_L5"}});

      
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
