// NEW FILE: cpp/5_Service/routes/route_admin_hot_cache.cpp
#include "routes.h"
#include "route_utils.h"

#include "core/hot_cache.h"
#include "segobj_cache.h"
#include "l5/reader.h"

namespace {
    static inline bool starts_with(const std::string& s, const std::string& pref) {
  return s.size() >= pref.size() && std::memcmp(s.data(), pref.data(), pref.size()) == 0;
}
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
      auto items_all = ctx.hot->list();

      const std::string org_index_pref =
        fs::absolute(ctx.data_root / "orgs" / org_id / "index").lexically_normal().generic_string();
 

      json files = json::array();
      uint64_t pinned_bytes = 0;
      uint64_t pinned_files = 0;
      for (const auto& it : items_all) {
        if (!starts_with(it.path, org_index_pref)) continue; // show only this org
        files.push_back({
          {"path", it.path},
          {"size_bytes", (uint64_t)it.size},
          {"mode", HotCache::mode_name(it.mode)},
          {"locked", it.locked ? 1 : 0},
          {"mtime_ns", (uint64_t)it.mtime_ns}
        });
        pinned_bytes += it.size;
        pinned_files += 1;
      }

      auto sst = l5::segobj_cache().stats();
      const uint64_t budget = l5::segobj_cache().budget_bytes();

      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"file_cache", {
          {"pinned_files", pinned_files},
          {"pinned_bytes", pinned_bytes},
          {"files", files}
        }},
        {"segobj_cache", {
          {"enabled", budget > 0 ? 1 : 0},
          {"budget_bytes", budget},
          {"entries", (uint64_t)sst.entries},
          {"bytes", (uint64_t)sst.bytes},
          {"pinned_entries", (uint64_t)sst.pinned_entries},
          {"pinned_bytes", (uint64_t)sst.pinned_bytes}
        }}
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
           std::string which = "file";
      if (req.has_param("which")) which = to_lower_copy(req.get_param_value("which"));
      if (which != "file" && which != "segobj" && which != "both") which = "file";

      const std::string org_index_pref =
        fs::absolute(ctx.data_root / "orgs" / org_id / "index").lexically_normal().generic_string();

      uint64_t removed_files = 0;
      if (which == "file" || which == "both") {
        auto items = ctx.hot->list();
        for (const auto& it : items) {
          if (!starts_with(it.path, org_index_pref)) continue;
          if (ctx.hot->unpin_file(fs::path(it.path))) removed_files++;
        }
      }

      if (which == "segobj" || which == "both") {
        l5::segobj_cache().clear(); // global per-process cache
      }

      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"which", which},
        {"removed_file_cache_entries", removed_files}
      });
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
      std::string cache_kind = to_lower_copy(j.value("cache", std::string("file")));
      if (cache_kind != "file" && cache_kind != "segobj" && cache_kind != "both") cache_kind = "file";


      const bool pin_bin = j.value("pin_bin", 1) != 0;
      const bool pin_doc = j.value("pin_docids", 1) != 0;
      const bool pin_meta= j.value("pin_meta", 0) != 0;

      const std::string mode_s = j.value("mode", std::string("mmap"));
      const HotCache::Mode mode = HotCache::parse_mode(mode_s, HotCache::Mode::Mmap);

      const fs::path index_root = ctx.data_root / "orgs" / org_id / "index";

      json results = json::array();
      uint64_t ok_cnt = 0;
      uint64_t segobj_ok = 0;
      uint64_t segobj_fail = 0;

      const bool do_file = (cache_kind == "file" || cache_kind == "both");
      const bool do_segobj = (cache_kind == "segobj" || cache_kind == "both");
      const bool segobj_pin = j.value("segobj_pin", 1) != 0;


      for (const auto& v : j["segments"]) {
        const std::string seg = v.is_string() ? v.get<std::string>() : "";
        if (!is_safe_rel_path(seg)) {
          results.push_back({{"segment", seg}, {"ok", 0}, {"error", "bad segment rel path"}});
          continue;
        }

        const fs::path seg_dir = index_root / fs::path(seg);
        std::string err;
        bool ok = true;
        if (do_file) {
          const bool ok_file = ctx.hot->pin_segment_dir(seg_dir, pin_bin, pin_doc, pin_meta, mode, &err);
          ok = ok && ok_file;
        }

        if (do_segobj) {
          if (l5::segobj_cache().budget_bytes() == 0) {
            ok = false;
            if (!err.empty()) err += "; ";
            err += "segobj_cache disabled (PLAGIO_SEGOBJ_CACHE_BYTES=0)";
            segobj_fail++;
          } else {
            std::string ee;
            auto sp = l5::segobj_cache().get_or_load(
              seg_dir,
              [&](std::string* e2) -> std::shared_ptr<l5::SegmentData> {
                auto p = std::make_shared<l5::SegmentData>();
                std::string eee;
                if (!l5::load_segment_bin(seg_dir, *p, &eee)) {
                  if (e2) *e2 = eee;
                  return {};
                }
                return p;
              },
              &ee
            );
            if (!sp) {
              ok = false;
              if (!err.empty()) err += "; ";
              err += ("segobj_load failed: " + ee);
              segobj_fail++;
            } else {
              if (segobj_pin) (void)l5::segobj_cache().pin(seg_dir);
              segobj_ok++;
            }
          }
        }

        results.push_back({{"segment", seg}, {"ok", ok ? 1 : 0}, {"error", err.empty() ? json(nullptr) : json(err)}});
        if (ok) ok_cnt++;
      }

      reply_json(res, 200, {
        {"ok", true},
        {"org_id", org_id},
        {"cache", cache_kind},
        {"mode", HotCache::mode_name(mode)},
        {"pin_bin", pin_bin ? 1 : 0},
        {"pin_docids", pin_doc ? 1 : 0},
        {"pin_meta", pin_meta ? 1 : 0},
        {"segments_ok", ok_cnt},
        {"file_pinned_bytes_total", (uint64_t)ctx.hot->total_bytes()},
        {"segobj_ok", segobj_ok},
        {"segobj_fail", segobj_fail},
        {"results", results}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
