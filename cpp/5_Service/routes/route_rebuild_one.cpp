// Back_Last/cpp/5_Service/routes/route_rebuild_one.cpp
#include "routes.h"
#include "route_utils.h"

#include <unordered_set>

#include "l5/compactor.h"
#include "l5/manifest.h"
#include "core/job_queue.h"

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

      if (!ctx.q) { reply_json(res, 500, {{"error","job queue not configured"}}); return; }
      json payload = {
        {"org_id", org_id},
        {"out_shard", (uint64_t)out_shard},
        {"fanout", (uint64_t)fanout},
        {"compact", do_compact ? 1 : 0}
      };
      const int64_t job_id = ctx.q->enqueue("REBUILD_ONE", payload.dump(), /*priority*/5);
      reply_json(res, 202, {{"ok", true}, {"org_id", org_id}, {"job_id", std::to_string(job_id)}, {"status","queued"}, {"type","REBUILD_ONE"}});
 
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
