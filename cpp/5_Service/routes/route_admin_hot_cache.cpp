// NEW FILE: cpp/5_Service/routes/route_admin_hot_cache.cpp
#include "routes.h"
#include "route_utils.h"

#include "core/hot_cache.h"

namespace {
static bool is_safe_rel_path(const std::string& s) {
  if (s.empty()) return false;
  fs::path p(s);
  if (p.is_absolute()) return false;
  for (const auto& part : p) {
    const std::string ss = part.generic_string();
    if (ss == "..") return false;
    if (ss.find('\\') != std::string::npos) return false;
  }
  return true;
}
} // namespace

void register_route_admin_hot_cache(httplib::Server& app, ServiceRouteContext& ctx) {
  // GET status
  app.Get(R"(/v1/orgs/([^/]+)/admin/hot_cache/status)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }
      if (!ctx.hot) { reply_json(res, 500, {{"error","hot cache not configured"}}); return; }

      ctx.hot->refresh_best_effort();
      auto items = ctx.hot->list();

      json files = json::array();
      for (const auto& it : items) {
        files.push_back({
          {"path", it.path},
          {"size_bytes", (uint64_t)it.size},
          {"mode", HotCache::mode_name(it.mode)},
          {"locked", it.locked ? 1 : 0},
          {"mtime_ns", (uint64_t)it.mtime_ns}
        });
      }

      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"pinned_files", (uint64_t)items.size()},
        {"pinned_bytes", (uint64_t)ctx.hot->total_bytes()},
        {"files", files}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // POST clear
  app.Post(R"(/v1/orgs/([^/]+)/admin/hot_cache/clear)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }
      if (!ctx.hot) { reply_json(res, 500, {{"error","hot cache not configured"}}); return; }
      ctx.hot->clear();
      reply_json(res, 200, {{"ok", true}, {"org_id", org_id}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // POST pin segments
  app.Post(R"(/v1/orgs/([^/]+)/admin/hot_cache/pin)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }
      if (!ctx.hot) { reply_json(res, 500, {{"error","hot cache not configured"}}); return; }

      if (req.body.empty()) { reply_json(res, 400, {{"error","json body required"}}); return; }
      json j;
      try { j = json::parse(req.body); }
      catch (...) { reply_json(res, 400, {{"error","invalid json"}}); return; }

      if (!j.contains("segments") || !j["segments"].is_array()) {
        reply_json(res, 400, {{"error","segments[] required"}}); return;
      }

      const bool pin_bin = j.value("pin_bin", 1) != 0;
      const bool pin_doc = j.value("pin_docids", 1) != 0;
      const bool pin_meta= j.value("pin_meta", 0) != 0;

      const std::string mode_s = j.value("mode", std::string("mmap"));
      const HotCache::Mode mode = HotCache::parse_mode(mode_s, HotCache::Mode::Mmap);

      const fs::path index_root = ctx.data_root / "orgs" / org_id / "index";

      json results = json::array();
      uint64_t ok_cnt = 0;

      for (const auto& v : j["segments"]) {
        const std::string seg = v.is_string() ? v.get<std::string>() : "";
        if (!is_safe_rel_path(seg)) {
          results.push_back({{"segment", seg}, {"ok", 0}, {"error", "bad segment rel path"}});
          continue;
        }

        const fs::path seg_dir = index_root / fs::path(seg);
        std::string err;
        const bool ok = ctx.hot->pin_segment_dir(seg_dir, pin_bin, pin_doc, pin_meta, mode, &err);
        results.push_back({{"segment", seg}, {"ok", ok ? 1 : 0}, {"error", err.empty() ? json(nullptr) : json(err)}});
        if (ok) ok_cnt++;
      }

      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"mode", HotCache::mode_name(mode)},
        {"pin_bin", pin_bin ? 1 : 0},
        {"pin_docids", pin_doc ? 1 : 0},
        {"pin_meta", pin_meta ? 1 : 0},
        {"segments_ok", ok_cnt},
        {"pinned_bytes", (uint64_t)ctx.hot->total_bytes()},
        {"results", results}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
