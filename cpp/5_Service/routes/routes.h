// Back_Last/cpp/5_Service/routes/routes.h
#pragma once

#include <filesystem>
#include <mutex>
#include <string>

#include "httplib.h"
#include "service.h"

class JobQueue;

struct ServiceRouteContext {
  std::filesystem::path data_root;
  L5Service* svc{nullptr};
  std::mutex* admin_mu{nullptr};

  JobQueue* q{nullptr};          // async jobs (SQLite queue)
  std::string node_id;           // for locked_by

  size_t max_json_body_bytes{0};
  size_t max_text_bytes{0};
  size_t max_query_bytes{0};
  size_t max_zip_upload_bytes{0};
};

void register_routes(httplib::Server& app, ServiceRouteContext& ctx);
