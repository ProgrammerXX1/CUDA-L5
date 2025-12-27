// src/main.cpp
#include <iostream>
#include <filesystem>
#include <stdexcept>

#include "httplib.h"
#include <nlohmann/json.hpp>

#include "service.h"
#include "l5/result.h"

using json = nlohmann::json;

static json doc_to_json(const DocRow& d) {
  return {
    {"org_id", d.org_id},
    {"doc_id", d.doc_id},
    {"external_id", d.external_id},
    {"source_path", d.source_path},
    {"source_name", d.source_name},
    {"stored_path", d.stored_path},
    {"preview", d.preview},
    {"created_at_utc", d.created_at_utc},
    {"deleted", d.deleted},
    {"deleted_at_utc", d.deleted_at_utc},
    {"last_segment", d.last_segment},
  };
}

static json upload_to_json(const UploadResult& r) {
  return {
    {"doc_id", r.doc_id},
    {"external_id", r.external_id},
    {"source_name", r.source_name},
    {"stored_path", r.stored_path},
    {"bytes", r.bytes}
  };
}

static void reply_json(httplib::Response& res, int status, const json& j) {
  res.status = status;
  res.set_content(j.dump(), "application/json");
}

int main(int argc, char** argv) {
  std::string data_root = (argc >= 2) ? argv[1] : "./DATA_ROOT";
  L5Service svc{std::filesystem::path(data_root)};

  httplib::Server app;

  // Simple safety limits (всё равно в памяти, но хотя бы режем DoS)
  constexpr size_t MAX_ZIP_UPLOAD_BYTES = 512ull * 1024 * 1024; // 512MB
  constexpr size_t MAX_JSON_BODY_BYTES  = 1ull * 1024 * 1024;   // 1MB
  constexpr size_t MAX_QUERY_BYTES      = 256ull * 1024;        // 256KB

  // Batch ZIP: one upload => one segment
  // POST /v1/orgs/{org}/ingest_zip  multipart: file=@batch.zip, text_is_normalized=0|1, segment_name(optional)
  app.Post(R"(/v1/orgs/([^/]+)/ingest_zip)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];

      if (!req.is_multipart_form_data()) {
        reply_json(res, 400, {{"error","expected multipart/form-data"}});
        return;
      }
      auto file_it = req.files.find("file");
      if (file_it == req.files.end()) {
        reply_json(res, 400, {{"error","missing file field"}});
        return;
      }

      const auto& f = file_it->second;

      if (f.content.size() > MAX_ZIP_UPLOAD_BYTES) {
        reply_json(res, 413, {{"error","zip too large"}, {"max_bytes", (uint64_t)MAX_ZIP_UPLOAD_BYTES}});
        return;
      }

      bool text_is_normalized = false;
      if (req.has_param("text_is_normalized")) {
        auto v = req.get_param_value("text_is_normalized");
        text_is_normalized = (v == "1" || v == "true");
      }

      std::string segment_name;
      if (req.has_param("segment_name")) segment_name = req.get_param_value("segment_name");

      auto r = svc.ingest_zip_build_segment(org_id, f.filename, f.content, text_is_normalized, segment_name);

      json docs = json::array();
      for (const auto& d : r.docs) docs.push_back(upload_to_json(d));

      json j = {
        {"segment_name", r.build.segment_name},
        {"seg_dir", r.build.seg_dir.string()},
        {"docs", r.build.docs},
        {"post9", r.build.post9},
        {"threads", r.build.threads},
        {"strict_text_is_normalized", r.build.strict_text_is_normalized},
        {"built_at_utc", r.build.built_at_utc},
        {"ingested_docs", docs},
        {"skipped", json::array()}
      };

      for (const auto& s : r.skipped) {
        j["skipped"].push_back({
          {"external_id", s.external_id},
          {"source_name", s.source_name},
          {"reason", s.reason},
        });
      }

      reply_json(res, 200, j);
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // Search
  app.Post(R"(/v1/orgs/([^/]+)/search)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];

      if (req.body.size() > MAX_JSON_BODY_BYTES) {
        reply_json(res, 413, {{"error","json body too large"}, {"max_bytes", (uint64_t)MAX_JSON_BODY_BYTES}});
        return;
      }

      json j;
      try {
        j = json::parse(req.body);
      } catch (...) {
        reply_json(res, 400, {{"error","invalid json"}});
        return;
      }

      std::string query = j.value("query", "");
      if (query.empty()) {
        reply_json(res, 400, {{"error","query is empty"}});
        return;
      }
      if (query.size() > MAX_QUERY_BYTES) {
        reply_json(res, 413, {{"error","query too large"}, {"max_bytes", (uint64_t)MAX_QUERY_BYTES}});
        return;
      }

      bool query_is_normalized = j.value("query_is_normalized", false);

      l5::SearchOptions opt;
      opt.topk = j.value("topk", opt.topk);
      opt.candidates_topn = j.value("candidates_topn", opt.candidates_topn);
      opt.min_hits = j.value("min_hits", opt.min_hits);
      opt.max_postings_per_hash = j.value("max_postings_per_hash", opt.max_postings_per_hash);
      opt.span_min_len = j.value("span_min_len", opt.span_min_len);
      opt.span_gap = j.value("span_gap", opt.span_gap);
      opt.max_spans_per_doc = j.value("max_spans_per_doc", opt.max_spans_per_doc);
      opt.alpha = j.value("alpha", opt.alpha);

      auto r = svc.search(org_id, query, query_is_normalized, opt);
      reply_json(res, 200, l5::to_json(r));
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // List documents
  app.Get(R"(/v1/orgs/([^/]+)/documents)", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];
      int limit = 50, offset = 0;

      auto parse_i = [](const std::string& s) -> int {
        size_t pos = 0;
        int v = std::stoi(s, &pos);
        if (pos != s.size()) throw std::invalid_argument("bad int");
        return v;
      };

      if (req.has_param("limit")) limit = parse_i(req.get_param_value("limit"));
      if (req.has_param("offset")) offset = parse_i(req.get_param_value("offset"));

      if (limit < 1) limit = 1;
      if (limit > 1000) limit = 1000;
      if (offset < 0) offset = 0;

      auto rows = svc.list_docs(org_id, limit, offset);
      json arr = json::array();
      for (auto& r : rows) arr.push_back(doc_to_json(r));
      reply_json(res, 200, {{"items", arr}, {"limit", limit}, {"offset", offset}});
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
    } catch (const std::exception& e) {
      reply_json(res, 500, {{"error", e.what()}});
    }
  });

  // Delete document (by doc_id or external_id)
  app.Delete(R"(/v1/orgs/([^/]+)/documents/([^/]+))", [&](const httplib::Request& req, httplib::Response& res) {
    try {
      std::string org_id = req.matches[1];
      std::string key = req.matches[2];
      svc.delete_doc(org_id, key);
      reply_json(res, 200, {{"ok", true}});
    } catch (const std::invalid_argument& e) {
      reply_json(res, 400, {{"error", e.what()}});
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
