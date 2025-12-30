// cpp/service/routes_process.cpp
#include "routes.h"
#include "route_utils.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <unordered_map>
#include <vector>

#include "l5/result.h"

namespace {

static inline int clamp_pct(int v) {
  if (v < 0) return 0;
  if (v > 100) return 100;
  return v;
}

// bbox по match_spans (в координатах шинглов: q_from/q_to и d_from/d_to включительно)
static bool bbox_from_spans(const std::vector<l5::MatchSpan>& sp,
                            uint32_t& q_from, uint32_t& q_to,
                            uint32_t& d_from, uint32_t& d_to) {
  if (sp.empty()) return false;

  q_from = std::numeric_limits<uint32_t>::max();
  d_from = std::numeric_limits<uint32_t>::max();
  q_to = 0;
  d_to = 0;

  for (const auto& s : sp) {
    if (s.q_from < q_from) q_from = s.q_from;
    if (s.q_to   > q_to)   q_to   = s.q_to;
    if (s.d_from < d_from) d_from = s.d_from;
    if (s.d_to   > d_to)   d_to   = s.d_to;
  }

  return (q_from != std::numeric_limits<uint32_t>::max()) &&
         (d_from != std::numeric_limits<uint32_t>::max()) &&
         (q_to >= q_from) && (d_to >= d_from);
}

static inline void clamp_bbox(uint32_t& from, uint32_t& to, uint32_t total) {
  if (total == 0) { from = 0; to = 0; return; }
  if (from >= total) { from = total; to = total; return; }
  if (to >= total) to = total - 1;
  if (to < from) to = from;
}

// Возвращает q/s offset+limit для matchsources.
// По умолчанию: вся длина (q_total/d_total). Если есть spans — bbox (минимальный отрезок, покрывающий матч).
static void make_match_ranges(const l5::Hit& h,
                              uint32_t fallback_q_total,
                              uint32_t& q_off, uint32_t& q_lim,
                              uint32_t& s_off, uint32_t& s_lim) {
  const uint32_t q_total = (h.q_total > 0) ? h.q_total : fallback_q_total;
  const uint32_t d_total = h.d_total;

  q_off = 0;
  q_lim = q_total;          // limit = длина отрезка
  s_off = 0;
  s_lim = d_total;          // limit = длина отрезка

  uint32_t q_from=0, q_to=0, d_from=0, d_to=0;
  if (!bbox_from_spans(h.match_spans, q_from, q_to, d_from, d_to)) {
    // нет spans — оставляем “весь документ”
    return;
  }

  if (q_total > 0) clamp_bbox(q_from, q_to, q_total);
  if (d_total > 0) clamp_bbox(d_from, d_to, d_total);

  q_off = q_from;
  q_lim = (q_to >= q_from) ? (q_to - q_from + 1) : 0;

  s_off = d_from;
  s_lim = (d_to >= d_from) ? (d_to - d_from + 1) : 0;
}

} // namespace

void register_route_process(httplib::Server& app, ServiceRouteContext& ctx) {
  app.Post(R"(/v1/process)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      constexpr unsigned SMALL_FANOUT = 20;

      if (req.body.size() > ctx.max_json_body_bytes) {
        reply_json(res, 413, {{"error","json body too large"}, {"max_bytes", (uint64_t)ctx.max_json_body_bytes}});
        return;
      }

      json j;
      try { j = json::parse(req.body); }
      catch (...) { reply_json(res, 400, {{"error","invalid json"}}); return; }

      const std::string org_id = parse_org_id_str(j);
      if (!is_safe_org_id(org_id)) { reply_json(res, 400, {{"error","bad organization_id"}}); return; }

      const std::string document_id = j.value("document_id", "");
      if (document_id.empty()) { reply_json(res, 400, {{"error","document_id is required"}}); return; }

      const std::string file_name = j.value("file_name", "");
      if (file_name.empty()) { reply_json(res, 400, {{"error","file_name is required"}}); return; }

      const std::string title = j.value("title", "");
      const std::string author = j.value("author", "");
      const std::string created_at = j.value("created_at", "");

      const std::string text = j.value("text", "");
      if (text.empty()) { reply_json(res, 400, {{"error","text is required"}}); return; }
      if (text.size() > ctx.max_text_bytes) {
        reply_json(res, 413, {{"error","text too large"}, {"max_bytes", (uint64_t)ctx.max_text_bytes}});
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
        indexed = ctx.svc->index_text_document(org_id, document_id, file_name, title, author, created_at, text);
        (void)ctx.svc->compact_small_levels(org_id, SMALL_FANOUT);
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

      if (text.size() > ctx.max_query_bytes) {
        reply_json(res, 413, {{"error","query too large"}, {"max_bytes",(uint64_t)ctx.max_query_bytes}});
        return;
      }

      l5::SearchOptions opt;
      opt.topk = 20;
      opt.candidates_topn = 2000;
      opt.min_hits = 1;
      opt.span_min_len = 1;
      opt.span_gap = 5;
      opt.max_postings_per_hash = 2000000;
      opt.alpha = 0.60;

      auto r_small = ctx.svc->search_levels(org_id, text, /*query_is_normalized=*/false,
                                            std::vector<int>{1,2,3,4}, std::vector<unsigned>{}, opt);
      auto r_l5 = ctx.svc->search_levels(org_id, text, /*query_is_normalized=*/false,
                                         std::vector<int>{5}, std::vector<unsigned>{}, opt);

      l5::SearchResult r;
      r.query = text;
      r.segments_scanned = r_small.segments_scanned + r_l5.segments_scanned;

      // best per doc_id (выбираем по максимальному покрытию запроса, а не по смешанному C)
      std::unordered_map<std::string, l5::Hit> best;
      best.reserve(r_small.hits.size() + r_l5.hits.size());

      auto push_hits = [&](std::vector<l5::Hit>&& hv) {
        for (auto& h : hv) {
          auto it = best.find(h.doc_id);
          if (it == best.end()) {
            best.emplace(h.doc_id, std::move(h));
            continue;
          }
          const bool better =
              (h.Cq > it->second.Cq) ||
              (h.Cq == it->second.Cq && h.C > it->second.C) ||
              (h.Cq == it->second.Cq && h.C == it->second.C && h.matched_shingles > it->second.matched_shingles);
          if (better) it->second = std::move(h);
        }
      };

      push_hits(std::move(r_small.hits));
      push_hits(std::move(r_l5.hits));

      r.hits.reserve(best.size());
      for (auto& kv : best) r.hits.push_back(std::move(kv.second));

      // сортируем по Cq (важно для UX и для “копия текста”)
      std::sort(r.hits.begin(), r.hits.end(), [](const l5::Hit& a, const l5::Hit& b) {
        if (a.Cq != b.Cq) return a.Cq > b.Cq;
        return a.C > b.C;
      });
      if (r.hits.size() > opt.topk) r.hits.resize(opt.topk);

      std::vector<l5::Hit> hits;
      hits.reserve(r.hits.size());
      for (auto& h : r.hits) {
        if (do_index && !h.external_id.empty() && h.external_id == document_id) continue;
        hits.push_back(std::move(h));
      }

      // BUGFIX #1: plagiarism_percentage должен опираться на Cq (coverage query), а не на C (alpha-mix)
      int plagiarism = 0;
      for (const auto& h : hits) {
        int c = clamp_pct((int)std::lround(h.Cq));
        if (c > plagiarism) plagiarism = c;
      }

      std::vector<int64_t> ids;
      ids.reserve(hits.size());
      for (const auto& h : hits) {
        int64_t id = safe_stoll(h.doc_id);
        if (id > 0) ids.push_back(id);
      }

      auto docs = ctx.svc->get_docs_by_internal_ids(org_id, ids);
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

      // fallback для q_total, если вдруг h.q_total==0
      const uint32_t fallback_q_total = (uint32_t)text.size();

      for (const auto& h : hits) {
        int64_t iid = safe_stoll(h.doc_id);
        const DocRow* meta = nullptr;
        auto it = by_id.find(iid);
        if (it != by_id.end()) meta = &it->second;

        const std::string name = (meta && !meta->title.empty()) ? meta->title : h.source_name;
        const std::string auth = (meta && !meta->author.empty()) ? meta->author : std::string("Нет автора");
        const std::string idx_date = (meta && !meta->stored_at_utc.empty()) ? meta->stored_at_utc : std::string("");

        const std::string mod_id =
            h.meta_path.empty() ? get_module_id("unknown") : get_module_id(h.meta_path);

        sources.push_back({
          {"id", h.doc_id},
          {"source_id", h.external_id},
          {"module_id", mod_id},
          {"name", name},
          {"url", nullptr},
          {"author", auth},
          {"index_date", idx_date}
        });

        // BUGFIX #2: s_offset/s_limit должны идти от документа (match_spans: d_from/d_to), а не из query.
        uint32_t q_off=0, q_lim=0, s_off=0, s_lim=0;
        make_match_ranges(h, fallback_q_total, q_off, q_lim, s_off, s_lim);

        matchsources.push_back({
          {"id", std::to_string(ms_seq++)},
          {"source_id", h.doc_id},
          {"q_offset", (int)q_off},
          {"q_limit",  (int)q_lim},
          {"s_offset", (int)s_off},
          {"s_limit",  (int)s_lim},
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
}
