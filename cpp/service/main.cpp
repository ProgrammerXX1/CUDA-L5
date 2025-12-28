#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "httplib.h"
#include <nlohmann/json.hpp>

#include "service.h"
#include "l5/result.h"

using json = nlohmann::json;
namespace fs = std::filesystem;

static std::mutex g_admin_mu;

static void reply_json(httplib::Response& res, int status, const json& j) {
  res.status = status;
  res.set_content(j.dump(), "application/json; charset=utf-8");
}

static std::string to_lower_copy(std::string s) {
  for (char& c : s) c = (char)std::tolower((unsigned char)c);
  return s;
}

static bool parse_bool_json(const json& j, const char* key, bool defv) {
  if (!j.contains(key)) return defv;
  const auto& v = j[key];
  if (v.is_boolean()) return v.get<bool>();
  if (v.is_number_integer()) return v.get<int>() != 0;
  if (v.is_string()) {
    std::string s = to_lower_copy(v.get<std::string>());
    if (s == "1" || s == "true" || s == "yes" || s == "on") return true;
    if (s == "0" || s == "false" || s == "no" || s == "off") return false;
  }
  return defv;
}

static std::string parse_org_id_str(const json& j) {
  if (!j.contains("organization_id")) return "";
  const auto& v = j["organization_id"];
  if (v.is_number_integer()) return std::to_string(v.get<int64_t>());
  if (v.is_string()) return v.get<std::string>();
  return "";
}

static int64_t safe_stoll(const std::string& s) {
  try {
    size_t pos = 0;
    long long x = std::stoll(s, &pos);
    if (pos != s.size()) return 0;
    return (int64_t)x;
  } catch (...) {
    return 0;
  }
}

int main(int argc, char** argv) {
  std::string data_root = (argc >= 2) ? argv[1] : "./DATA_ROOT";
  L5Service svc{fs::path(data_root)};

  httplib::Server app;

  constexpr size_t MAX_JSON_BODY_BYTES = 4ull * 1024 * 1024;
  constexpr size_t MAX_TEXT_BYTES      = 2ull * 1024 * 1024;
  constexpr size_t MAX_QUERY_BYTES     = 2ull * 1024 * 1024;

  app.Post(R"(/v1/process)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      if (req.body.size() > MAX_JSON_BODY_BYTES) {
        reply_json(res, 413, {{"error","json body too large"}, {"max_bytes", (uint64_t)MAX_JSON_BODY_BYTES}});
        return;
      }

      json j;
      try { j = json::parse(req.body); }
      catch (...) { reply_json(res, 400, {{"error","invalid json"}}); return; }

      const std::string org_id = parse_org_id_str(j);
      if (org_id.empty()) { reply_json(res, 400, {{"error","organization_id is required"}}); return; }

      const std::string document_id = j.value("document_id", "");
      if (document_id.empty()) { reply_json(res, 400, {{"error","document_id is required"}}); return; }

      const std::string file_name = j.value("file_name", "");
      if (file_name.empty()) { reply_json(res, 400, {{"error","file_name is required"}}); return; }

      const std::string title = j.value("title", "");
      const std::string author = j.value("author", "");
      const std::string created_at = j.value("created_at", "");

      const std::string text = j.value("text", "");
      if (text.empty()) { reply_json(res, 400, {{"error","text is required"}}); return; }
      if (text.size() > MAX_TEXT_BYTES) {
        reply_json(res, 413, {{"error","text too large"}, {"max_bytes", (uint64_t)MAX_TEXT_BYTES}});
        return;
      }

      const bool do_index  = parse_bool_json(j, "do_index", false);
      const bool do_search = parse_bool_json(j, "do_search", true);

      if (!do_index && !do_search) {
        reply_json(res, 400, {{"error","nothing to do: both do_index and do_search are false"}});
        return;
      }

      std::optional<IndexTextResult> indexed;
      if (do_index) {
        indexed = svc.index_text_document(org_id, document_id, file_name, title, author, created_at, text);
        (void)svc.compact_small_levels(org_id, 20);
      }

      if (!do_search) {
        json out = {
          {"document_id", document_id},
          {"status", "indexed"},
          {"processed_at", L5Service::utc_now_iso_utc()},
          {"indexed", do_index ? 1 : 0},
          {"indexed_internal_id", indexed.has_value() ? json(std::to_string(indexed->doc.id)) : json(nullptr)},
          {"last_segment", indexed.has_value() ? json(indexed->doc.last_segment) : json(nullptr)}
        };
        reply_json(res, 200, out);
        return;
      }

      if (text.size() > MAX_QUERY_BYTES) {
        reply_json(res, 413, {{"error","query too large"}, {"max_bytes",(uint64_t)MAX_QUERY_BYTES}});
        return;
      }

      std::vector<int> levels = {1,2,3,4,5};
      std::vector<unsigned> l5_shards; // empty => all

      l5::SearchOptions opt;
      opt.topk = 20;
      opt.candidates_topn = 2000;
      opt.min_hits = 1;
      opt.span_min_len = 1;
      opt.span_gap = 5;
      opt.max_postings_per_hash = 2000000;
      opt.alpha = 0.60;

      // L1-L4: query уже нормализован backend'ом
      auto r_small = svc.search_levels(org_id, text, /*query_is_normalized=*/true,
                                 std::vector<int>{1,2,3,4}, std::vector<unsigned>{}, opt);

      // L5: query нормализуем нашим алгоритмом (для L5 архива)
      auto r_l5 = svc.search_levels(org_id, text, /*query_is_normalized=*/false,
                              std::vector<int>{5}, std::vector<unsigned>{}, opt);

      // merge best by doc_id
      l5::SearchResult r;
      r.query = text;
      r.segments_scanned = r_small.segments_scanned + r_l5.segments_scanned;

      std::unordered_map<std::string, l5::Hit> best;
      best.reserve(r_small.hits.size() + r_l5.hits.size());

      auto push_hits = [&](std::vector<l5::Hit>&& hits){
        for (auto& h : hits) {
          auto it = best.find(h.doc_id);
          if (it == best.end() || h.C > it->second.C) best[h.doc_id] = std::move(h);
        }
      };

      push_hits(std::move(r_small.hits));
      push_hits(std::move(r_l5.hits));

      r.hits.reserve(best.size());
      for (auto& kv : best) r.hits.push_back(std::move(kv.second));

      std::sort(r.hits.begin(), r.hits.end(), [](const l5::Hit& a, const l5::Hit& b){
        return a.C > b.C;
      });
      if (r.hits.size() > opt.topk) r.hits.resize(opt.topk);

      // remove self-match by backend document_id
      std::vector<l5::Hit> hits;
      hits.reserve(r.hits.size());
      for (auto& h : r.hits) {
        if (!h.external_id.empty() && h.external_id == document_id) continue;
        hits.push_back(std::move(h));
      }

      int plagiarism = 0;
      for (const auto& h : hits) {
        int c = (int)(h.C + 0.5);
        if (c > plagiarism) plagiarism = c;
      }

      std::vector<int64_t> ids;
      ids.reserve(hits.size());
      for (const auto& h : hits) {
        int64_t id = safe_stoll(h.doc_id);
        if (id > 0) ids.push_back(id);
      }

      auto docs = svc.get_docs_by_internal_ids(org_id, ids);
      std::unordered_map<int64_t, DocRow> by_id;
      by_id.reserve(docs.size() * 2);
      for (auto& d : docs) by_id[d.id] = std::move(d);

      std::unordered_map<std::string, std::string> module_ids;
      std::vector<json> modules;
      modules.reserve(64);
      int mod_seq = 1;

      auto get_module_id = [&](const std::string& name) -> std::string {
        auto it = module_ids.find(name);
        if (it != module_ids.end()) return it->second;
        std::string id = std::to_string(mod_seq++);
        module_ids[name] = id;
        modules.push_back({{"id", id}, {"module_name", name}});
        return id;
      };

      std::vector<json> sources;
      std::vector<json> matchsources;
      sources.reserve(hits.size());
      matchsources.reserve(hits.size());

      int ms_seq = 1;
      const int q_limit = (int)text.size();

      for (const auto& h : hits) {
        int64_t iid = safe_stoll(h.doc_id);
        const DocRow* meta = nullptr;
        auto it = by_id.find(iid);
        if (it != by_id.end()) meta = &it->second;

        const std::string name = (meta && !meta->title.empty()) ? meta->title : h.source_name;
        const std::string auth = (meta && !meta->author.empty()) ? meta->author : std::string("Нет автора");
        const std::string idx_date = (meta && !meta->stored_at_utc.empty()) ? meta->stored_at_utc : std::string("");

        if (!h.meta_path.empty()) (void)get_module_id(h.meta_path);

        sources.push_back({
          {"id", h.doc_id},
          {"source_id", h.external_id},
          {"module_id", "plagiarism"},
          {"name", name},
          {"url", nullptr},
          {"author", auth},
          {"index_date", idx_date}
        });

        matchsources.push_back({
          {"id", std::to_string(ms_seq++)},
          {"source_id", h.doc_id},
          {"q_offset", 0},
          {"q_limit", q_limit},
          {"s_offset", 0},
          {"s_limit", q_limit},
          {"type", "1"}
        });
      }

      json out = {
        {"document_id", document_id},
        {"status", "completed"},
        {"processed_at", L5Service::utc_now_iso_utc()},
        {"plagiarism_percentage", plagiarism},
        {"selfcite_percentage", 0},
        {"legal_percentage", 0},
        {"unknown_percentage", 0},
        {"sources", sources},
        {"matchsources", matchsources},
        {"modules", modules}
      };

      reply_json(res, 200, out);
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // manual compaction
  app.Post(R"(/v1/orgs/([^/]+)/admin/compact_small)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      unsigned fanout = 20;
      if (req.has_param("fanout")) fanout = (unsigned)std::stoul(req.get_param_value("fanout"));
      if (fanout < 2) fanout = 2;
      auto rep = svc.compact_small_levels(org_id, fanout);
      reply_json(res, 200, {{"ok", true}, {"org_id", org_id}, {"fanout", fanout}, {"rounds", rep.rounds}, {"merges", rep.merges}, {"new_segments", rep.new_segments}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  app.Post(R"(/v1/orgs/([^/]+)/admin/compact_l5)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      const std::string org_id = req.matches[1];
      unsigned fanout = 2;
      if (req.has_param("fanout")) fanout = (unsigned)std::stoul(req.get_param_value("fanout"));
      if (fanout < 2) fanout = 2;
      auto rep = svc.compact_l5_shards(org_id, fanout);
      reply_json(res, 200, {{"ok", true}, {"org_id", org_id}, {"fanout", fanout}, {"rounds", rep.rounds}, {"merges", rep.merges}, {"new_segments", rep.new_segments}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // wipe all
  app.Post(R"(/v1/admin/wipe_all)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string confirm;
      if (req.has_param("confirm")) confirm = req.get_param_value("confirm");
      if (confirm != "WIPE_ALL") { reply_json(res, 400, {{"error","confirm required"},{"expected","WIPE_ALL"}}); return; }

      std::lock_guard<std::mutex> lk(g_admin_mu);

      const fs::path orgs_dir = svc.data_root() / "orgs";
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

  const char* host = "0.0.0.0";
  int port = 8088;
  std::cout << "L5 service data_root=" << data_root << " listen " << host << ":" << port << "\n";
  app.listen(host, port);
  return 0;
}
