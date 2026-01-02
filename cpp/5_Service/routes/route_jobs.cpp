// NEW FILE: Back_Last/cpp/5_Service/routes/route_jobs.cpp
#include "routes.h"
#include "route_utils.h"

#include "core/job_queue.h"

void register_route_jobs(httplib::Server& app, ServiceRouteContext& ctx) {
  // GET /v1/jobs/{id}
  app.Get(R"(/v1/jobs/(\d+))", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      if (!ctx.q) { reply_json(res, 500, {{"error","job queue not configured"}}); return; }
      const int64_t id = safe_stoll(req.matches[1]);
      if (id <= 0) { reply_json(res, 400, {{"error","bad job id"}}); return; }

      auto j = ctx.q->get(id);
      if (!j) { reply_json(res, 404, {{"error","job not found"}}); return; }

      json out;
      out["id"] = std::to_string(j->id);
      out["status"] = j->status;
      out["type"] = j->type;
      out["priority"] = j->priority;
      out["created_at_unix"] = (uint64_t)j->created_at;
      out["run_after_unix"] = (uint64_t)j->run_after;
      out["attempts"] = j->attempts;
      out["max_attempts"] = j->max_attempts;
      out["locked_by"] = j->locked_by.empty() ? json(nullptr) : json(j->locked_by);
      out["locked_at_unix"] = (uint64_t)j->locked_at;
      out["started_at_unix"] = (uint64_t)j->started_at;
      out["finished_at_unix"] = (uint64_t)j->finished_at;

      // payload/result are stored as JSON strings; parse best-effort
      try { out["payload"] = j->payload.empty() ? json::object() : json::parse(j->payload); }
      catch (...) { out["payload"] = j->payload; }

      if (!j->result.empty()) {
        try { out["result"] = json::parse(j->result); }
        catch (...) { out["result"] = j->result; }
      } else {
        out["result"] = nullptr;
      }

      out["error"] = j->error.empty() ? json(nullptr) : json(j->error);
      reply_json(res, 200, out);
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
