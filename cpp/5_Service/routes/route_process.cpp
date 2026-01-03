// Back_Last/cpp/5_Service/routes/route_process.cpp
#include "routes.h"
#include "route_utils.h"
#include "core/file_lock.h"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
#include <unordered_map>
#include <vector>

#include "l5/result.h"
#include "core/job_queue.h"

namespace {

static inline int clamp_pct(int v) {
  if (v < 0) return 0;
  if (v > 100) return 100;
  return v;
}

// bbox по match_spans (координаты шинглов), inclusive
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

// q/s offset+limit:
// - если есть spans: bbox по spans
// - иначе: (0, q_total) и (0, d_total)
static void make_match_ranges(const l5::Hit& h,
                              uint32_t fallback_q_total,
                              uint32_t& q_off, uint32_t& q_lim,
                              uint32_t& s_off, uint32_t& s_lim) {
  const uint32_t q_total = (h.q_total > 0) ? h.q_total : fallback_q_total;
  const uint32_t d_total = h.d_total;

  q_off = 0;
  q_lim = q_total;
  s_off = 0;
  s_lim = d_total;

  uint32_t q_from=0, q_to=0, d_from=0, d_to=0;
  if (!bbox_from_spans(h.match_spans, q_from, q_to, d_from, d_to)) return;

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
        json err;
        err["error"] = "json body too large";
        err["max_bytes"] = (uint64_t)ctx.max_json_body_bytes;
        reply_json(res, 413, err);
        return;
      }

      json j;
      try { j = json::parse(req.body); }
      catch (...) {
        json err; err["error"] = "invalid json";
        reply_json(res, 400, err);
        return;
      }

      const std::string org_id = parse_org_id_str(j);
      if (!is_safe_org_id(org_id)) {
        json err; err["error"] = "bad organization_id";
        reply_json(res, 400, err);
        return;
      }

      const std::string document_id = j.value("document_id", "");
      if (document_id.empty()) {
        json err; err["error"] = "document_id is required";
        reply_json(res, 400, err);
        return;
      }

      const std::string file_name = j.value("file_name", "");
      if (file_name.empty()) {
        json err; err["error"] = "file_name is required";
        reply_json(res, 400, err);
        return;
      }

      const std::string title = j.value("title", "");
      const std::string author = j.value("author", "");
      const std::string created_at = j.value("created_at", "");

      const std::string text = j.value("text", "");
      if (text.empty()) {
        json err; err["error"] = "text is required";
        reply_json(res, 400, err);
        return;
      }
      if (text.size() > ctx.max_text_bytes) {
        json err;
        err["error"] = "text too large";
        err["max_bytes"] = (uint64_t)ctx.max_text_bytes;
        reply_json(res, 413, err);
        return;
      }

      const bool do_index  = parse_bool_json(j, "do_index", false);
      const bool do_search = parse_bool_json(j, "do_search", true);

      if (!do_index && !do_search) {
        json err; err["error"] = "nothing to do: both do_index and do_search are false";
        reply_json(res, 400, err);
        return;
      }

      std::optional<int64_t> index_job_id;
      if (do_index) {
        if (!ctx.q) { reply_json(res, 500, {{"error","job queue not configured"}}); return; }

        // enqueue INDEX_TEXT job and persist text to org/jobs/job_<id>/input.txt
        json payload = {
          {"org_id", org_id},
          {"document_id", document_id},
          {"file_name", file_name},
          {"title", title},
          {"author", author},
          {"created_at", created_at},
          {"do_compact_small", 1},
          {"small_fanout", (uint64_t)SMALL_FANOUT}
        };

        const int64_t job_id = ctx.q->enqueue("INDEX_TEXT", payload.dump(), /*priority*/5);
        const fs::path job_dir = ctx.data_root / "orgs" / org_id / "jobs" / ("job_" + std::to_string(job_id));
        ensure_dirs(job_dir);
        const fs::path txt_path = job_dir / "input.txt";
        {
          std::ofstream out(txt_path, std::ios::binary);
          if (!out) { reply_json(res, 500, {{"error","cannot write input text"}, {"path", txt_path.string()}}); return; }
          out.write(text.data(), (std::streamsize)text.size());
          out.flush();
          if (!out) { reply_json(res, 500, {{"error","write input text failed"}, {"path", txt_path.string()}}); return; }
        }
        payload["text_path"] = (fs::path("orgs") / org_id / "jobs" / ("job_" + std::to_string(job_id)) / "input.txt").generic_string();
        (void)ctx.q->update_payload(job_id, payload.dump());
        index_job_id = job_id;
      }

      if (!do_search) {
        json out;
        out["document_id"] = document_id;
        out["status"] = do_index ? "queued" : "noop";
        out["processed_at"] = L5Service::utc_now_iso_utc();
        out["indexed"] = do_index ? 1 : 0;
        out["index_job_id"] = index_job_id.has_value() ? json(std::to_string(*index_job_id)) : json(nullptr);
        reply_json(res, 200, out);
        return;
      }

      FileLock global_lock(ctx.data_root / ".global.lock", FileLock::Mode::Shared);
      FileLock org_lock(ctx.data_root / "orgs" / org_id / ".org.lock", FileLock::Mode::Shared);

      if (text.size() > ctx.max_query_bytes) {
        json err;
        err["error"] = "query too large";
        err["max_bytes"] = (uint64_t)ctx.max_query_bytes;
        reply_json(res, 413, err);
        return;
      }

      l5::SearchOptions opt;
      opt.topk = 20;
      opt.candidates_topn = 800;
      opt.min_hits = 1;
      opt.span_min_len = 1;
      opt.span_gap = 5;
      opt.max_postings_per_hash = 300000;
      opt.alpha = 0.60;

      auto r_small = ctx.svc->search_levels(org_id, text, /*query_is_normalized=*/false,
                                            std::vector<int>{1,2,3,4}, std::vector<unsigned>{}, opt);
      auto r_l5 = ctx.svc->search_levels(org_id, text, /*query_is_normalized=*/false,
                                         std::vector<int>{5}, std::vector<unsigned>{}, opt);

      // best per doc_id: выбираем по лучшему Cq (покрытие запроса)
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

      std::vector<l5::Hit> hits;
      hits.reserve(best.size());
      for (auto& kv : best) hits.push_back(std::move(kv.second));

      std::sort(hits.begin(), hits.end(), [](const l5::Hit& a, const l5::Hit& b) {
        if (a.Cq != b.Cq) return a.Cq > b.Cq;
        return a.C > b.C;
      });
      if (hits.size() > opt.topk) hits.resize(opt.topk);

      // exclude self-hit for do_index
      if (do_index) {
        std::vector<l5::Hit> filtered;
        filtered.reserve(hits.size());
        for (auto& h : hits) {
          if (!h.external_id.empty() && h.external_id == document_id) continue;
          filtered.push_back(std::move(h));
        }
        hits.swap(filtered);
      }

      // plagiarism = max(Cq)
      int plagiarism = 0;
      for (const auto& h : hits) {
        int c = clamp_pct((int)std::lround(h.Cq));
        if (c > plagiarism) plagiarism = c;
      }

      // fetch docs meta
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

      // modules table
      std::unordered_map<std::string, std::string> module_ids;
      std::vector<json> modules;
      modules.reserve(64);
      int mod_seq = 1;

      auto get_module_id = [&](const std::string& name) -> std::string {
        auto it = module_ids.find(name);
        if (it != module_ids.end()) return it->second;

        const std::string id = std::to_string(mod_seq++);
        module_ids[name] = id;

        json m;
        m["id"] = id;
        m["module_name"] = name;
        modules.push_back(std::move(m));
        return id;
      };

      std::vector<json> sources;
      std::vector<json> matchsources;
      sources.reserve(hits.size());
      matchsources.reserve(hits.size());

      int ms_seq = 1;
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

        // sources item in required field order
        json s;
        s["id"] = h.doc_id;
        s["source_id"] = h.external_id;
        s["module_id"] = mod_id;
        s["name"] = name;
        s["url"] = nullptr;
        s["author"] = auth;
        s["index_date"] = idx_date;
        sources.push_back(std::move(s));

        // matchsources item in required field order
        uint32_t q_off=0, q_lim=0, s_off=0, s_lim=0;
        make_match_ranges(h, fallback_q_total, q_off, q_lim, s_off, s_lim);

        json ms;
        ms["id"] = std::to_string(ms_seq++);
        ms["source_id"] = h.doc_id;
        ms["q_offset"] = (int)q_off;
        ms["q_limit"]  = (int)q_lim;
        ms["s_offset"] = (int)s_off;
        ms["s_limit"]  = (int)s_lim;
        ms["type"] = "1";
        matchsources.push_back(std::move(ms));
      }

      // TOP-LEVEL output in required order
      json out;
      out["document_id"] = document_id;
      out["status"] = "completed";
      out["processed_at"] = L5Service::utc_now_iso_utc();
      out["indexed_async"] = do_index ? 1 : 0;
      out["index_job_id"] = index_job_id.has_value() ? json(std::to_string(*index_job_id)) : json(nullptr);
      out["plagiarism_percentage"] = plagiarism;
      out["selfcite_percentage"] = 0;
      out["legal_percentage"] = 0;
      out["unknown_percentage"] = 0;
      out["sources"] = sources;
      out["matchsources"] = matchsources;
      out["modules"] = modules;

      reply_json(res, 200, out);
    } catch (const std::invalid_argument& e) {
      json err; err["error"] = e.what();
      reply_json(res, 400, err);
    } catch (const std::exception& e) {
      json err; err["error"] = e.what();
      reply_json(res, 500, err);
    }
  });
}
