// src/storage.cpp
#include "storage.h"
#include <sqlite3.h>
#include <stdexcept>
#include <filesystem>

static void exec(sqlite3* db, const char* sql) {
  char* err = nullptr;
  if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
    std::string msg = err ? err : "sqlite error";
    sqlite3_free(err);
    throw std::runtime_error(msg);
  }
}

Storage::Storage(const std::string& db_path) : path_(db_path) {
  // ensure parent dirs exist (fix for GET /documents before ingest)
  std::error_code ec;
  std::filesystem::create_directories(std::filesystem::path(path_).parent_path(), ec);

  sqlite3* db = nullptr;
  if (sqlite3_open(path_.c_str(), &db) != SQLITE_OK) {
    throw std::runtime_error("cannot open sqlite: " + path_);
  }
  db_ = db;
}

Storage::~Storage() {
  if (db_) sqlite3_close((sqlite3*)db_);
}

void Storage::init() {
  auto* db = (sqlite3*)db_;
  exec(db, R"SQL(
    PRAGMA journal_mode=WAL;
    PRAGMA synchronous=NORMAL;

    CREATE TABLE IF NOT EXISTS documents (
      org_id TEXT NOT NULL,
      doc_id TEXT NOT NULL,
      external_id TEXT NOT NULL,
      source_path TEXT,
      source_name TEXT,
      stored_path TEXT,
      preview TEXT,
      created_at_utc TEXT,
      deleted INTEGER DEFAULT 0,
      deleted_at_utc TEXT,
      last_segment TEXT,
      PRIMARY KEY(org_id, doc_id)
    );

    CREATE INDEX IF NOT EXISTS idx_docs_org_external ON documents(org_id, external_id);
    CREATE INDEX IF NOT EXISTS idx_docs_org_deleted  ON documents(org_id, deleted);
  )SQL");
}

void Storage::upsert_doc(const DocRow& d) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    INSERT INTO documents(org_id, doc_id, external_id, source_path, source_name, stored_path, preview, created_at_utc, deleted, deleted_at_utc, last_segment)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(org_id, doc_id) DO UPDATE SET
      external_id=excluded.external_id,
      source_path=excluded.source_path,
      source_name=excluded.source_name,
      stored_path=excluded.stored_path,
      preview=excluded.preview,
      created_at_utc=excluded.created_at_utc,
      deleted=excluded.deleted,
      deleted_at_utc=excluded.deleted_at_utc,
      last_segment=excluded.last_segment;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");

  sqlite3_bind_text(st, 1, d.org_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, d.doc_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 3, d.external_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 4, d.source_path.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 5, d.source_name.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 6, d.stored_path.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 7, d.preview.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 8, d.created_at_utc.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(st, 9, d.deleted);
  sqlite3_bind_text(st,10, d.deleted_at_utc.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st,11, d.last_segment.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    sqlite3_finalize(st);
    throw std::runtime_error("sqlite step failed");
  }
  sqlite3_finalize(st);
}

void Storage::upsert_docs_bulk(const std::vector<DocRow>& docs) {
  if (docs.empty()) return;

  auto* db = (sqlite3*)db_;
  sqlite3_busy_timeout(db, 5000);

  const char* sql = R"SQL(
    INSERT INTO documents(org_id, doc_id, external_id, source_path, source_name, stored_path, preview, created_at_utc, deleted, deleted_at_utc, last_segment)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(org_id, doc_id) DO UPDATE SET
      external_id=excluded.external_id,
      source_path=excluded.source_path,
      source_name=excluded.source_name,
      stored_path=excluded.stored_path,
      preview=excluded.preview,
      created_at_utc=excluded.created_at_utc,
      deleted=excluded.deleted,
      deleted_at_utc=excluded.deleted_at_utc,
      last_segment=excluded.last_segment;
  )SQL";

  sqlite3_stmt* st = nullptr;

  exec(db, "BEGIN IMMEDIATE;");
  try {
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) {
      throw std::runtime_error("sqlite prepare failed (bulk upsert)");
    }

    for (const auto& d : docs) {
      sqlite3_reset(st);
      sqlite3_clear_bindings(st);

      sqlite3_bind_text(st, 1, d.org_id.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 2, d.doc_id.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 3, d.external_id.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 4, d.source_path.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 5, d.source_name.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 6, d.stored_path.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 7, d.preview.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 8, d.created_at_utc.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_int (st, 9, d.deleted);
      sqlite3_bind_text(st,10, d.deleted_at_utc.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st,11, d.last_segment.c_str(), -1, SQLITE_TRANSIENT);

      if (sqlite3_step(st) != SQLITE_DONE) {
        throw std::runtime_error("sqlite step failed (bulk upsert)");
      }
    }

    sqlite3_finalize(st);
    st = nullptr;

    exec(db, "COMMIT;");
  } catch (...) {
    if (st) sqlite3_finalize(st);
    try { exec(db, "ROLLBACK;"); } catch (...) {}
    throw;
  }
}

std::optional<DocRow> Storage::get_by_doc_or_external(const std::string& org_id, const std::string& key) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    SELECT org_id, doc_id, external_id, source_path, source_name, stored_path, preview, created_at_utc, deleted, deleted_at_utc, last_segment
    FROM documents
    WHERE org_id=? AND (doc_id=? OR external_id=?)
    LIMIT 1;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");
  sqlite3_bind_text(st, 1, org_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, key.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 3, key.c_str(), -1, SQLITE_TRANSIENT);

  DocRow r;
  int rc = sqlite3_step(st);
  if (rc == SQLITE_ROW) {
    r.org_id = (const char*)sqlite3_column_text(st, 0);
    r.doc_id = (const char*)sqlite3_column_text(st, 1);
    r.external_id = (const char*)sqlite3_column_text(st, 2);

    const unsigned char* sp = sqlite3_column_text(st, 3);
    r.source_path = sp ? (const char*)sp : "";

    const unsigned char* sn = sqlite3_column_text(st, 4);
    r.source_name = sn ? (const char*)sn : "";

    const unsigned char* stp = sqlite3_column_text(st, 5);
    r.stored_path = stp ? (const char*)stp : "";

    const unsigned char* pr = sqlite3_column_text(st, 6);
    r.preview = pr ? (const char*)pr : "";

    const unsigned char* ca = sqlite3_column_text(st, 7);
    r.created_at_utc = ca ? (const char*)ca : "";

    r.deleted = sqlite3_column_int(st, 8);

    const unsigned char* da = sqlite3_column_text(st, 9);
    r.deleted_at_utc = da ? (const char*)da : "";

    const unsigned char* ls = sqlite3_column_text(st, 10);
    r.last_segment = ls ? (const char*)ls : "";

    sqlite3_finalize(st);
    return r;
  }
  sqlite3_finalize(st);
  return std::nullopt;
}

std::vector<DocRow> Storage::list_docs(const std::string& org_id, int limit, int offset) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    SELECT org_id, doc_id, external_id, source_path, source_name, stored_path, preview, created_at_utc, deleted, deleted_at_utc, last_segment
    FROM documents
    WHERE org_id=?
    ORDER BY created_at_utc DESC
    LIMIT ? OFFSET ?;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");
  sqlite3_bind_text(st, 1, org_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(st, 2, limit);
  sqlite3_bind_int(st, 3, offset);

  std::vector<DocRow> out;
  while (sqlite3_step(st) == SQLITE_ROW) {
    DocRow r;
    r.org_id = (const char*)sqlite3_column_text(st, 0);
    r.doc_id = (const char*)sqlite3_column_text(st, 1);
    r.external_id = (const char*)sqlite3_column_text(st, 2);

    const unsigned char* sp = sqlite3_column_text(st, 3);
    r.source_path = sp ? (const char*)sp : "";

    const unsigned char* sn = sqlite3_column_text(st, 4);
    r.source_name = sn ? (const char*)sn : "";

    const unsigned char* stp = sqlite3_column_text(st, 5);
    r.stored_path = stp ? (const char*)stp : "";

    const unsigned char* pr = sqlite3_column_text(st, 6);
    r.preview = pr ? (const char*)pr : "";

    const unsigned char* ca = sqlite3_column_text(st, 7);
    r.created_at_utc = ca ? (const char*)ca : "";

    r.deleted = sqlite3_column_int(st, 8);

    const unsigned char* da = sqlite3_column_text(st, 9);
    r.deleted_at_utc = da ? (const char*)da : "";

    const unsigned char* ls = sqlite3_column_text(st, 10);
    r.last_segment = ls ? (const char*)ls : "";

    out.push_back(std::move(r));
  }
  sqlite3_finalize(st);
  return out;
}

void Storage::mark_deleted(const std::string& org_id, const std::string& key, const std::string& deleted_at_utc) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    UPDATE documents
    SET deleted=1, deleted_at_utc=?
    WHERE org_id=? AND (doc_id=? OR external_id=?);
  )SQL";
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");
  sqlite3_bind_text(st, 1, deleted_at_utc.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, org_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 3, key.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 4, key.c_str(), -1, SQLITE_TRANSIENT);
  if (sqlite3_step(st) != SQLITE_DONE) {
    sqlite3_finalize(st);
    throw std::runtime_error("sqlite step failed");
  }
  sqlite3_finalize(st);
}

void Storage::update_last_segment(const std::string& org_id, const std::vector<std::string>& doc_ids, const std::string& seg) {
  if (doc_ids.empty()) return;
  auto* db = (sqlite3*)db_;

  const char* sql = R"SQL(
    UPDATE documents SET last_segment=? WHERE org_id=? AND doc_id=?;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");

  for (const auto& did : doc_ids) {
    sqlite3_reset(st);
    sqlite3_clear_bindings(st);
    sqlite3_bind_text(st, 1, seg.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, org_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, did.c_str(), -1, SQLITE_TRANSIENT);

    if (sqlite3_step(st) != SQLITE_DONE) {
      sqlite3_finalize(st);
      throw std::runtime_error("sqlite step failed in update_last_segment");
    }
  }

  sqlite3_finalize(st);
}
