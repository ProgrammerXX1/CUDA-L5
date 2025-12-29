#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

struct DocRow {
  int64_t id{0};                // INTERNAL numeric id (our)
  std::string org_id;           // organization_id as string
  std::string source_id;        // backend document_id
  std::string file_name;        // required
  std::string title;            // optional
  std::string author;           // optional
  std::string created_at;        // optional (from backend)
  std::string stored_at_utc;     // when inserted/updated in our DB

  int deleted{0};
  std::string deleted_at_utc;

  // relative segment path for debug/ops, e.g. "l1/seg_xxx" or "l5/s00/seg_xxx"
  std::string last_segment;
};

class Storage {
public:
  explicit Storage(const std::string& db_path);
  ~Storage();

  void init();

  // Upsert by (org_id, source_id). Returns internal id.
  int64_t upsert_doc_get_id(DocRow& d);

  std::optional<DocRow> get_by_source_id(const std::string& org_id, const std::string& source_id);
  std::optional<DocRow> get_by_internal_id(const std::string& org_id, int64_t id);

  std::vector<DocRow> get_by_internal_ids(const std::string& org_id, const std::vector<int64_t>& ids);

  std::vector<DocRow> list_docs(const std::string& org_id, int limit, int offset);

  void mark_deleted_by_source_id(const std::string& org_id, const std::string& source_id, const std::string& deleted_at_utc);

  void update_last_segment_by_ids(const std::string& org_id,
                                  const std::vector<int64_t>& ids,
                                  const std::string& last_segment);

private:
  void* db_{nullptr}; // sqlite3*
  std::string path_;
};
