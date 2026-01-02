// cpp/5_Service/routes/route_segment_docs.cpp
#include "routes.h"
#include "route_utils.h"
#include "core/file_lock.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
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
    if (ss.find('\\') != std::string::npos) return false;
  }
  return true;
}

static inline bool is_shard_dir_name_like(const std::string& s) {
  if (s.size() < 2) return false;
  if (s[0] != 's' && s[0] != 'S') return false;
  for (size_t i = 1; i < s.size(); ++i) {
    if (!std::isdigit((unsigned char)s[i])) return false;
  }
  return true;
}

static inline bool path_has_component(const fs::path& p, const std::string& comp) {
  for (const auto& part : p) {
    if (part.generic_string() == comp) return true;
  }
  return false;
}

// Ищем сегменты по маркеру index_native_docids.json.
// base_dir задаёт “корень” для относительных путей в ответе.
//
// must_have_component используется ТОЛЬКО для L5 (например "s08"):
//   - пруним другие sYY
//   - и фильтруем rel-path: должен содержать компонент s08.
static std::vector<std::string> list_segments_under(
    const fs::path& base_dir,
    size_t hard_cap,
    int max_depth,
    const std::string& must_have_component = {}) {
  std::vector<std::string> out;
  std::error_code ec;
  if (!fs::exists(base_dir, ec) || ec) return out;

  auto opts = fs::directory_options::skip_permission_denied;
  fs::recursive_directory_iterator it(base_dir, opts, ec), end;

  for (; it != end && out.size() < hard_cap; it.increment(ec)) {
    if (ec) { ec.clear(); continue; }

    if (it.depth() > max_depth) {
      it.disable_recursion_pending();
      continue;
    }

    // prune shard dirs только если нужно
    if (!must_have_component.empty()) {
      const bool is_dir = it->is_directory(ec);
      if (ec) { ec.clear(); continue; }
      if (is_dir) {
        const std::string name = it->path().filename().generic_string();
        if (is_shard_dir_name_like(name) && name != must_have_component) {
          it.disable_recursion_pending();
        }
        continue;
      }
    }

    if (!it->is_regular_file(ec) || ec) { ec.clear(); continue; }
    if (it->path().filename() != "index_native_docids.json") continue;

    const fs::path seg_dir = it->path().parent_path();

    fs::path rel = fs::relative(seg_dir, base_dir, ec);
    if (ec) { ec.clear(); continue; }

    if (!must_have_component.empty() && !path_has_component(rel, must_have_component)) continue;

    std::string seg = rel.generic_string();
    if (seg.empty() || seg == ".") continue;

    out.push_back(std::move(seg));
  }

  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

static void append_prefixed_with_cap(std::vector<std::string>& dst,
                                    const std::string& prefix,
                                    const std::vector<std::string>& src,
                                    size_t& remaining) {
  for (const auto& s : src) {
    if (remaining == 0) break;
    dst.push_back(prefix + s);
    --remaining;
  }
}

static std::vector<std::string> collect_segments_for_index_root(
    const fs::path& index_root,
    bool has_shard,
    unsigned shard,
    size_t seg_limit) {

  std::vector<std::string> segs_all;
  size_t remaining = seg_limit;

  const std::string shard_dir = shard_dir_name(shard);

  // L1-L4 не шарденные
  if (remaining) { auto v = list_segments_under(index_root / "l1", remaining, /*max_depth*/10); append_prefixed_with_cap(segs_all, "l1/", v, remaining); }
  if (remaining) { auto v = list_segments_under(index_root / "l2", remaining, /*max_depth*/10); append_prefixed_with_cap(segs_all, "l2/", v, remaining); }
  if (remaining) { auto v = list_segments_under(index_root / "l3", remaining, /*max_depth*/10); append_prefixed_with_cap(segs_all, "l3/", v, remaining); }
  if (remaining) { auto v = list_segments_under(index_root / "l4", remaining, /*max_depth*/10); append_prefixed_with_cap(segs_all, "l4/", v, remaining); }

  // L5 шарденный: если shard не задан → все sXX; если задан → только sXX
  if (remaining) {
    const fs::path l5_root = index_root / "l5";
    auto v = has_shard
      ? list_segments_under(l5_root, remaining, /*max_depth*/12, shard_dir)
      : list_segments_under(l5_root, remaining, /*max_depth*/12);
    append_prefixed_with_cap(segs_all, "l5/", v, remaining);
  }

  std::sort(segs_all.begin(), segs_all.end());
  segs_all.erase(std::unique(segs_all.begin(), segs_all.end()), segs_all.end());
  if (segs_all.size() > seg_limit) segs_all.resize(seg_limit);

  return segs_all;
}

static std::vector<std::string> list_org_ids_under(const fs::path& orgs_root, size_t hard_cap) {
  std::vector<std::string> out;
  std::error_code ec;
  if (!fs::exists(orgs_root, ec) || ec) return out;

  fs::directory_iterator it(orgs_root, ec), end;
  for (; it != end && out.size() < hard_cap; it.increment(ec)) {
    if (ec) { ec.clear(); continue; }
    if (!it->is_directory(ec) || ec) { ec.clear(); continue; }

    const std::string org_id = it->path().filename().generic_string();
    if (!is_safe_org_id(org_id)) continue;

    out.push_back(org_id);
  }

  std::sort(out.begin(), out.end());
  out.erase(std::unique(out.begin(), out.end()), out.end());
  return out;
}

static void handle_for_one_org(const std::string& org_id,
                               const httplib::Request& req,
                               httplib::Response& res,
                               ServiceRouteContext& ctx) {
  const bool has_shard = req.has_param("shard");
  unsigned shard = 0;
  if (has_shard) shard = (unsigned)std::stoul(req.get_param_value("shard"));

  const fs::path index_root = ctx.data_root / "orgs" / org_id / "index";
  const fs::path l5_base_dir = index_root / "l5" / shard_dir_name(shard);

  // LIST MODE
  if (!req.has_param("segment")) {
    size_t seg_limit = 50000;
    if (req.has_param("limit")) {
      try { seg_limit = (size_t)std::stoull(req.get_param_value("limit")); } catch (...) {}
    }
    if (seg_limit < 1) seg_limit = 1;
    if (seg_limit > 200000) seg_limit = 200000;

    std::error_code ec;
    if (!fs::exists(index_root, ec) || ec) {
      reply_json(res, 404, {{"error","index root not found"}, {"index_root", index_root.string()}});
      return;
    }

    auto segs_all = collect_segments_for_index_root(index_root, has_shard, shard, seg_limit);

    reply_json(res, 200, {
      {"org_id", org_id},
      {"shard_is_set", has_shard},
      {"shard", shard},
      {"index_root", index_root.string()},
      {"limit", (uint64_t)seg_limit},
      {"segments_count", (uint64_t)segs_all.size()},
      {"segments", segs_all}
    });
    return;
  }

  // SEGMENT MODE (для конкретного org)
  const bool has_limit = req.has_param("limit");
  size_t limit = 0; // если limit не задан -> читаем ВЕСЬ файл docids
  if (has_limit) {
    try { limit = (size_t)std::stoull(req.get_param_value("limit")); } catch (...) { limit = 100; }
    if (limit < 1) limit = 1;
    if (limit > 5000) limit = 5000;
  }

  const std::string segment = req.get_param_value("segment");
  if (!is_safe_rel_path(segment)) {
    reply_json(res, 400, {{"error","bad segment"}, {"segment", segment}});
    return;
  }

  std::error_code ec;

  // Новый формат: segment относительный к index_root (например "l3/seg.../part0", "l5/s08/seg.../part0")
  fs::path seg_dir = index_root / segment;
  if (!fs::exists(seg_dir / "index_native_docids.json", ec) || ec) {
    ec.clear();
    // Легаси для L5: segment относительный к l5_base_dir ("seg.../part0")
    seg_dir = l5_base_dir / segment;
  }
  ec.clear();

  const fs::path docids = seg_dir / "index_native_docids.json";
  const fs::path meta   = seg_dir / "index_native_meta.json";

  if (!fs::exists(docids, ec) || ec) {
    reply_json(res, 404, {{"error","docids not found"}, {"path", docids.string()}});
    return;
  }
  ec.clear();

  json docs = json::array();
  if (has_limit) {
    docs = read_docids_preview(docids, limit);
  } else {
    static constexpr uint64_t kHardMaxDocidsJsonBytes = 64ull * 1024 * 1024; // 64 MiB
    const uint64_t sz = (uint64_t)fs::file_size(docids, ec);
    if (!ec && sz > kHardMaxDocidsJsonBytes) {
      reply_json(res, 413, {
        {"error","docids file too large for full parse"},
        {"path", docids.string()},
        {"bytes", sz},
        {"hard_cap_bytes", kHardMaxDocidsJsonBytes},
        {"hint", "set ?limit=N to get preview"}
      });
      return;
    }
    ec.clear();

    std::ifstream in(docids);
    if (!in) {
      reply_json(res, 500, {{"error","failed to open docids"}, {"path", docids.string()}});
      return;
    }
    try { in >> docs; }
    catch (...) {
      reply_json(res, 500, {{"error","failed to parse docids json"}, {"path", docids.string()}});
      return;
    }
  }

  json meta_j = json::object();
  if (fs::exists(meta, ec) && !ec) {
    std::ifstream in(meta);
    if (in) {
      try { in >> meta_j; } catch (...) { meta_j = json::object(); }
    }
  }

  reply_json(res, 200, {
    {"org_id", org_id},
    {"shard_is_set", has_shard},
    {"shard", shard},
    {"segment", segment},
    {"seg_dir", seg_dir.string()},
    {"limit", has_limit ? (uint64_t)limit : 0ull},
    {"docids_is_full", !has_limit},
    {"meta", meta_j},
    {"docids_preview", docs}
  });
}

} // namespace

void register_route_segment_docs(httplib::Server& app, ServiceRouteContext& ctx) {

  // ЕДИНСТВЕННЫЙ РОУТ:
  //   GET /v1/segment_docs
  //     - если ?org_id=... -> работа внутри org (list/segment)
  //     - если org_id не задан -> list-mode по всем org (segment-mode запрещён без org_id)
  //
  // Параметры:
  //   org_id      (optional)  - если нет -> сканируем все orgs
  //   segment     (optional)  - если нет -> list mode
  //   shard       (optional)  - влияет только на L5; если нет -> все sXX
  //   limit       - в list-mode: лимит сегментов; в segment-mode: лимит docids preview
  //   org_limit   - только при org_id отсутствует: сколько org-ов сканировать
  app.Get(R"(/v1/segment_docs)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      // 1) org_id задан -> обычная логика
      FileLock global_lock(ctx.data_root / ".global.lock", FileLock::Mode::Shared);
      if (req.has_param("org_id")) {
        const std::string org_id = req.get_param_value("org_id");
        if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad org_id"}}); return; }
        FileLock org_lock(ctx.data_root / "orgs" / org_id / ".org.lock", FileLock::Mode::Shared);
        handle_for_one_org(org_id, req, res, ctx);
        return;
      }

      // 2) org_id не задан -> только list mode по всем org
      if (req.has_param("segment")) {
        reply_json(res, 400, {{"error","segment mode requires org_id"}, {"hint","use ?org_id=...&segment=..."}});
        return;
      }

      size_t seg_limit = 50000; // общий лимит сегментов на ответ
      if (req.has_param("limit")) {
        try { seg_limit = (size_t)std::stoull(req.get_param_value("limit")); } catch (...) {}
      }
      if (seg_limit < 1) seg_limit = 1;
      if (seg_limit > 200000) seg_limit = 200000;

      size_t org_limit = 2000; // сколько org-ов сканировать
      if (req.has_param("org_limit")) {
        try { org_limit = (size_t)std::stoull(req.get_param_value("org_limit")); } catch (...) {}
      }
      if (org_limit < 1) org_limit = 1;
      if (org_limit > 20000) org_limit = 20000;

      const bool has_shard = req.has_param("shard");
      unsigned shard = 0;
      if (has_shard) shard = (unsigned)std::stoul(req.get_param_value("shard"));

      const fs::path orgs_root = ctx.data_root / "orgs";
      std::error_code ec;
      if (!fs::exists(orgs_root, ec) || ec) {
        reply_json(res, 404, {{"error","orgs root not found"}, {"orgs_root", orgs_root.string()}});
        return;
      }

      auto orgs = list_org_ids_under(orgs_root, org_limit);

      json segments_by_org = json::object();
      uint64_t total_segments = 0;
      size_t remaining = seg_limit;

      for (const auto& org_id : orgs) {
        if (remaining == 0) break;

        const fs::path index_root = orgs_root / org_id / "index";
        if (!fs::exists(index_root, ec) || ec) { ec.clear(); continue; }

        auto segs = collect_segments_for_index_root(index_root, has_shard, shard, remaining);
        if (!segs.empty()) {
          segments_by_org[org_id] = segs;
          total_segments += (uint64_t)segs.size();
          remaining = (remaining > segs.size()) ? (remaining - segs.size()) : 0;
        }
      }

      reply_json(res, 200, {
        {"org_id_is_set", false},
        {"shard_is_set", has_shard},
        {"shard", shard},
        {"orgs_root", orgs_root.string()},
        {"org_limit", (uint64_t)org_limit},
        {"orgs_found", (uint64_t)orgs.size()},
        {"limit", (uint64_t)seg_limit},
        {"segments_count", total_segments},
        {"segments_by_org", segments_by_org}
      });

    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });
}
