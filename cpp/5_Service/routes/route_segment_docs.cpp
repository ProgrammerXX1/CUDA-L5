#include "routes.h"
#include "route_utils.h"

#include <algorithm>
#include <fstream>
#include <string>
#include <system_error>
#include <vector>

namespace {

static bool is_safe_rel_path(const std::string& s) {
  if (s.empty()) return false;
  fs::path p(s);
  if (p.is_absolute()) return false;
  for (const auto& part : p) {
    const std::string ss = part.generic_string();
    if (ss == "..") return false;
    if (ss.find('\\') != std::string::npos) return false; // на всякий случай
  }
  return true;
}

static std::vector<std::string> list_segments_under(const fs::path& base_dir, size_t hard_cap) {
  std::vector<std::string> out;
  std::error_code ec;
  if (!fs::exists(base_dir, ec) || ec) return out;

  auto opts = fs::directory_options::skip_permission_denied;
  fs::recursive_directory_iterator it(base_dir, opts, ec), end;

  for (; it != end && out.size() < hard_cap; it.increment(ec)) {
    if (ec) { ec.clear(); continue; }
    // base/(sXX)/(seg_*)/(...) / index_native_docids.json
    if (it.depth() > 4) { it.disable_recursion_pending(); continue; }
    if (!it->is_regular_file(ec) || ec) { ec.clear(); continue; }
    if (it->path().filename() != "index_native_docids.json") continue;

    const fs::path seg_dir = it->path().parent_path();
    fs::path rel = fs::relative(seg_dir, base_dir, ec);
    if (ec) { ec.clear(); continue; }
    std::string seg = rel.generic_string();
    if (seg.empty() || seg == ".") continue;
    out.push_back(std::move(seg));
  }

  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

} // namespace

void register_route_segment_docs(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Get(R"(/v1/orgs/([^/]+)/l5/segment_docs)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }

      unsigned shard = 0;
      if (req.has_param("shard")) shard = (unsigned)std::stoul(req.get_param_value("shard"));

      const fs::path base_dir =
          ctx.data_root / "orgs" / org_id / "index" / "l5" / shard_dir_name(shard);

      // --- LIST MODE: no segment param => return all existing segments
      if (!req.has_param("segment")) {
        size_t seg_limit = 50000; // по умолчанию "все", но с safety cap
        if (req.has_param("limit")) {
          try { seg_limit = (size_t)std::stoull(req.get_param_value("limit")); } catch (...) {}
        }
        if (seg_limit < 1) seg_limit = 1;
        if (seg_limit > 200000) seg_limit = 200000;

        std::error_code ec;
        if (!fs::exists(base_dir, ec) || ec) {
          reply_json(res, 404, {{"error","l5 index not found"}, {"base_dir", base_dir.string()}});
          return;
        }

        auto segs = list_segments_under(base_dir, seg_limit);
        reply_json(res, 200, {
          {"org_id", org_id},
          {"shard", shard},
          {"base_dir", base_dir.string()},
          {"limit", (uint64_t)seg_limit},
          {"segments_count", (uint64_t)segs.size()},
          {"segments", segs}
        });
        return;
      }

      // --- SEGMENT MODE (old behavior)
      size_t limit = 100; // docids preview limit
      if (req.has_param("limit")) {
        try { limit = (size_t)std::stoull(req.get_param_value("limit")); } catch (...) {}
      }
      if (limit < 1) limit = 1;
      if (limit > 5000) limit = 5000;

      const std::string segment = req.get_param_value("segment");
      if (!is_safe_rel_path(segment)) {
        reply_json(res, 400, {{"error","bad segment"}, {"segment", segment}});
        return;
      }

      const fs::path seg_dir = base_dir / segment;
      const fs::path docids = seg_dir / "index_native_docids.json";
      const fs::path meta   = seg_dir / "index_native_meta.json";

      std::error_code ec;
      if (!fs::exists(docids, ec) || ec) {
        reply_json(res, 404, {{"error","docids not found"}, {"path", docids.string()}});
        return;
      }

      json docs = read_docids_preview(docids, limit);

      json meta_j = json::object();
      if (fs::exists(meta, ec) && !ec) {
        std::ifstream in(meta);
        if (in) {
          try { in >> meta_j; } catch (...) { meta_j = json::object(); }
        }
      }

      reply_json(res, 200, {
        {"org_id", org_id},
        {"shard", shard},
        {"segment", segment},
        {"seg_dir", seg_dir.string()},
        {"limit", (uint64_t)limit},
        {"meta", meta_j},
        {"docids_preview", docs}
      });
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
