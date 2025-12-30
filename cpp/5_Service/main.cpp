// cpp/5_Service/main.cpp
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>

#include "httplib.h"

#include "service.h"
#include "routes/routes.h"

namespace fs = std::filesystem;

static std::mutex g_admin_mu;

int main(int argc, char** argv) {
  std::string data_root = (argc >= 2) ? argv[1] : "./DATA_ROOT";
  L5Service svc{fs::path(data_root)};

  httplib::Server app;

  constexpr size_t MAX_JSON_BODY_BYTES   = 4ull * 1024 * 1024;
  constexpr size_t MAX_TEXT_BYTES        = 2ull * 1024 * 1024;
  constexpr size_t MAX_QUERY_BYTES       = 2ull * 1024 * 1024;
  constexpr size_t MAX_ZIP_UPLOAD_BYTES  = 512ull * 1024 * 1024; // httplib keeps multipart in RAM

  ServiceRouteContext ctx;
  ctx.data_root = fs::path(data_root);
  ctx.svc = &svc;
  ctx.admin_mu = &g_admin_mu;
  ctx.max_json_body_bytes  = MAX_JSON_BODY_BYTES;
  ctx.max_text_bytes       = MAX_TEXT_BYTES;
  ctx.max_query_bytes      = MAX_QUERY_BYTES;
  ctx.max_zip_upload_bytes = MAX_ZIP_UPLOAD_BYTES;

  register_routes(app, ctx);

  const char* host = "0.0.0.0";
  int port = 8088;
  std::cout << "L5 service data_root=" << data_root << " listen " << host << ":" << port << "\n";
  app.listen(host, port);
  return 0;
}
