// NEW FILE: Back_Last/cpp/5_Service/routes/route_admin_ingest_fs_batch.cpp
#include "routes.h"
#include "route_utils.h"

#include "core/job_queue.h"

#include <cstdio>
#include <string>
#include <vector>

namespace {
static std::string vol_name(unsigned v) {
  char buf[32];
  std::snprintf(buf, sizeof(buf), "vol_%04u", v);
  return std::string(buf);
}
static std::string dir3(unsigned x) {
  char buf[8];
  std::snprintf(buf, sizeof(buf), "%03u", x);
  return std::string(buf);
}
static std::string segname_safe(const std::string& pref, unsigned vol, unsigned g) {
  // only [A-Za-z0-9_-]
  std::string p;
  p.reserve(pref.size());
  for (char c : pref) {
    const bool ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c=='_' || c=='-';
    p.push_back(ok ? c : '_');
  }
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%s_v%04u_g%02u", p.c_str(), vol, g);
  return std::string(buf);
}
} // namespace

void register_route_admin_ingest_fs_batch(httplib::Server& app, ServiceRouteContext& ctx) {
  // POST /v1/orgs/{org}/admin/l5/ingest_fs_batch
  app.Post(R"(/v1/orgs/([^/]+)/admin/l5/ingest_fs_batch)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }
      if (!ctx.q) { reply_json(res, 500, {{"error","job queue not configured"}}); return; }
      if (req.body.empty()) { reply_json(res, 400, {{"error","json body required"}}); return; }

      json j;
      try { j = json::parse(req.body); }
      catch (...) { reply_json(res, 400, {{"error","invalid json"}}); return; }

      const std::string dataset_root = j.value("dataset_root", "");
      if (dataset_root.empty()) { reply_json(res, 400, {{"error","dataset_root required"}}); return; }

      const std::string dataset_prefix = j.value("dataset_prefix", std::string("cc100_ru"));
      const unsigned vol_start = (unsigned)j.value("vol_start", 1);
      const unsigned vol_end   = (unsigned)j.value("vol_end", 15);
      unsigned group_size = (unsigned)j.value("group_size", 10);
      if (group_size < 1) group_size = 1;
      if (group_size > 50) group_size = 50;
      if (100 % group_size != 0) { reply_json(res, 400, {{"error","group_size must divide 100"}}); return; }

      const unsigned shard = (unsigned)j.value("l5_shard", 11);
      const bool normalize = j.value("normalize", 1) != 0;
      const bool recursive = j.value("recursive", 0) != 0;

      const unsigned groups_per_vol = 100 / group_size; // for 10 => 10 groups

      json jobs = json::array();
      uint64_t total_jobs = 0;

      for (unsigned v = vol_start; v <= vol_end; ++v) {
        const std::string vol = vol_name(v);
        for (unsigned g = 0; g < groups_per_vol; ++g) {
          std::vector<std::string> src_dirs;
          src_dirs.reserve(group_size);
          for (unsigned k = 0; k < group_size; ++k) {
            const unsigned d = g * group_size + k; // 0..99
            src_dirs.push_back((fs::path(dataset_root) / vol / dir3(d)).string());
          }

          json payload = {
            {"org_id", org_id},
            {"dataset_root", dataset_root},
            {"dataset_prefix", dataset_prefix},
            {"l5_shard", (uint64_t)shard},
            {"normalize", normalize ? 1 : 0},
            {"recursive", recursive ? 1 : 0},
            {"src_dirs", src_dirs},
            {"segment_name", segname_safe(dataset_prefix, v, g)}
          };

          const int64_t job_id = ctx.q->enqueue("INGEST_L5_FS", payload.dump(), /*priority*/10);
          jobs.push_back({{"job_id", std::to_string(job_id)}, {"vol", vol}, {"group", (uint64_t)g}});
          total_jobs++;
        }
      }

      reply_json(res, 202, {
        {"ok", true},
        {"org_id", org_id},
        {"type", "INGEST_L5_FS"},
        {"dataset_root", dataset_root},
        {"dataset_prefix", dataset_prefix},
        {"l5_shard", (uint64_t)shard},
        {"normalize", normalize ? 1 : 0},
        {"group_size_dirs", (uint64_t)group_size},
        {"jobs_count", total_jobs},
        {"jobs", jobs}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
