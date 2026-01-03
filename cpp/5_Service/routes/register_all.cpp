// Back_Last/cpp/5_Service/routes/register_all.cpp
#include "routes.h"

void register_route_ingest_zip(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_debug_normalized_text(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_rebuild_one(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_rebuild_info(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_segment_docs(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_process(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_admin_compact(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_admin_wipe_all(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_jobs(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_admin_hot_cache(httplib::Server& app, ServiceRouteContext& ctx);
void register_route_admin_ingest_fs_batch(httplib::Server& app, ServiceRouteContext& ctx);

void register_routes(httplib::Server& app, ServiceRouteContext& ctx) {
  register_route_ingest_zip(app, ctx);
  register_route_debug_normalized_text(app, ctx);
  register_route_rebuild_one(app, ctx);
  register_route_rebuild_info(app, ctx);
  register_route_segment_docs(app, ctx);
  register_route_process(app, ctx);
  register_route_admin_compact(app, ctx);
  register_route_admin_wipe_all(app, ctx);
  register_route_jobs(app, ctx);
  register_route_admin_hot_cache(app, ctx);
  register_route_admin_ingest_fs_batch(app, ctx);
}
