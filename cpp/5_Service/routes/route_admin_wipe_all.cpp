//  Back_Last/cpp/5_Service/routes/route_admin_wipe_all.cpp
#include "routes.h"
#include "route_utils.h"

#include "core/job_queue.h"

void register_route_admin_wipe_all(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Post(R"(/v1/admin/wipe_all)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string confirm;
      if (req.has_param("confirm")) confirm = req.get_param_value("confirm");
      if (confirm != "WIPE_ALL") { reply_json(res, 400, {{"error","confirm required"},{"expected","WIPE_ALL"}}); return; }

      if (!ctx.q) { reply_json(res, 500, {{"error","job queue not configured"}}); return; }
      json payload = {{"confirm","WIPE_ALL"}};
      const int64_t job_id = ctx.q->enqueue("WIPE_ALL", payload.dump(), /*priority*/100);
      reply_json(res, 202, {{"ok", true}, {"job_id", std::to_string(job_id)}, {"status","queued"}, {"type","WIPE_ALL"}});

    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
