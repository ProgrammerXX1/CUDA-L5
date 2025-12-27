// src/storage.h
#pragma once
#include <string>
#include <vector>
#include <optional>

struct DocRow {
  std::string org_id;
  std::string doc_id;
  std::string external_id;
  std::string source_path;    // stored file path
  std::string source_name;    // original name
  std::string stored_path;    // same as source_path for now
  std::string preview;
  std::string created_at_utc;
  int deleted{0};
  std::string deleted_at_utc;
  std::string last_segment;
};

class Storage {
public:
  explicit Storage(const std::string& db_path);
  ~Storage();

  void init();

  void upsert_doc(const DocRow& d);
  void upsert_docs_bulk(const std::vector<DocRow>& docs); // NEW: fast bulk upsert

  std::optional<DocRow> get_by_doc_or_external(const std::string& org_id, const std::string& key);
  std::vector<DocRow> list_docs(const std::string& org_id, int limit, int offset);

  void mark_deleted(const std::string& org_id, const std::string& key, const std::string& deleted_at_utc);
  void update_last_segment(const std::string& org_id, const std::vector<std::string>& doc_ids, const std::string& seg);

private:
  void* db_{nullptr}; // sqlite3*
  std::string path_;
};
