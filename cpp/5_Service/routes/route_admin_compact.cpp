#include "routes.h"
#include "route_utils.h"

void register_route_admin_compact(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Post(R"(/v1/orgs/([^/]+)/admin/compact_small)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      unsigned fanout = 20;
      if (req.has_param("fanout")) fanout = (unsigned)std::stoul(req.get_param_value("fanout"));
      if (fanout < 2) fanout = 2;

      auto rep = ctx.svc->compact_small_levels(org_id, fanout);
      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"fanout", fanout},
        {"rounds", rep.rounds},
        {"merges", rep.merges},
        {"new_segments", rep.new_segments}
      });
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

      auto rep = ctx.svc->compact_l5_shards(org_id, fanout);
      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"fanout", fanout},
        {"rounds", rep.rounds},
        {"merges", rep.merges},
        {"new_segments", rep.new_segments}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
