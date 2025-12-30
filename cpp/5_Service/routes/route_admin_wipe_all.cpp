#include "routes.h"
#include "route_utils.h"

void register_route_admin_wipe_all(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Post(R"(/v1/admin/wipe_all)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string confirm;
      if (req.has_param("confirm")) confirm = req.get_param_value("confirm");
      if (confirm != "WIPE_ALL") { reply_json(res, 400, {{"error","confirm required"},{"expected","WIPE_ALL"}}); return; }

      std::lock_guard<std::mutex> lk(*ctx.admin_mu);

      const fs::path orgs_dir = ctx.svc->data_root() / "orgs";
      std::error_code ec;
      uintmax_t removed = 0;

      if (fs::exists(orgs_dir, ec)) {
        ec.clear();
        removed = fs::remove_all(orgs_dir, ec);
        if (ec) { reply_json(res, 500, {{"error","remove_all failed"},{"detail",ec.message()}}); return; }
      }

      ec.clear();
      fs::create_directories(orgs_dir, ec);
      if (ec) { reply_json(res, 500, {{"error","create_directories failed"},{"detail",ec.message()}}); return; }

      reply_json(res, 200, {{"ok", true}, {"removed_entries", (uint64_t)removed}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
