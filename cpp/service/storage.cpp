#include "storage.h"
#include <sqlite3.h>

#include <filesystem>
#include <stdexcept>
#include <string>
#include <unordered_set>

static void exec(sqlite3* db, const char* sql) {
  char* err = nullptr;
  if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
    std::string msg = err ? err : "sqlite error";
    sqlite3_free(err);
    throw std::runtime_error(msg);
  }
}

Storage::Storage(const std::string& db_path) : path_(db_path) {
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
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      org_id TEXT NOT NULL,
      source_id TEXT NOT NULL,
      file_name TEXT NOT NULL,
      title TEXT,
      author TEXT,
      created_at TEXT,
      stored_at_utc TEXT,
      deleted INTEGER DEFAULT 0,
      deleted_at_utc TEXT,
      last_segment TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS idx_docs_org_source ON documents(org_id, source_id);
    CREATE INDEX IF NOT EXISTS idx_docs_org_deleted ON documents(org_id, deleted);
    CREATE INDEX IF NOT EXISTS idx_docs_org_id ON documents(org_id, id);
  )SQL");
}

static DocRow row_from_stmt(sqlite3_stmt* st) {
  DocRow r;

  r.id = sqlite3_column_int64(st, 0);

  const unsigned char* c1 = sqlite3_column_text(st, 1);
  r.org_id = c1 ? (const char*)c1 : "";

  const unsigned char* c2 = sqlite3_column_text(st, 2);
  r.source_id = c2 ? (const char*)c2 : "";

  const unsigned char* c3 = sqlite3_column_text(st, 3);
  r.file_name = c3 ? (const char*)c3 : "";

  const unsigned char* c4 = sqlite3_column_text(st, 4);
  r.title = c4 ? (const char*)c4 : "";

  const unsigned char* c5 = sqlite3_column_text(st, 5);
  r.author = c5 ? (const char*)c5 : "";

  const unsigned char* c6 = sqlite3_column_text(st, 6);
  r.created_at = c6 ? (const char*)c6 : "";

  const unsigned char* c7 = sqlite3_column_text(st, 7);
  r.stored_at_utc = c7 ? (const char*)c7 : "";

  r.deleted = sqlite3_column_int(st, 8);

  const unsigned char* c9 = sqlite3_column_text(st, 9);
  r.deleted_at_utc = c9 ? (const char*)c9 : "";

  const unsigned char* c10 = sqlite3_column_text(st, 10);
  r.last_segment = c10 ? (const char*)c10 : "";

  return r;
}

std::optional<DocRow> Storage::get_by_source_id(const std::string& org_id, const std::string& source_id) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    SELECT id, org_id, source_id, file_name, title, author, created_at, stored_at_utc,
           deleted, deleted_at_utc, last_segment
    FROM documents
    WHERE org_id=? AND source_id=?
    LIMIT 1;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");
  sqlite3_bind_text(st, 1, org_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, source_id.c_str(), -1, SQLITE_TRANSIENT);

  int rc = sqlite3_step(st);
  if (rc == SQLITE_ROW) {
    DocRow r = row_from_stmt(st);
    sqlite3_finalize(st);
    return r;
  }
  sqlite3_finalize(st);
  return std::nullopt;
}

std::optional<DocRow> Storage::get_by_internal_id(const std::string& org_id, int64_t id) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    SELECT id, org_id, source_id, file_name, title, author, created_at, stored_at_utc,
           deleted, deleted_at_utc, last_segment
    FROM documents
    WHERE org_id=? AND id=?
    LIMIT 1;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");
  sqlite3_bind_text(st, 1, org_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 2, id);

  int rc = sqlite3_step(st);
  if (rc == SQLITE_ROW) {
    DocRow r = row_from_stmt(st);
    sqlite3_finalize(st);
    return r;
  }
  sqlite3_finalize(st);
  return std::nullopt;
}

int64_t Storage::upsert_doc_get_id(DocRow& d) {
  auto* db = (sqlite3*)db_;
  sqlite3_busy_timeout(db, 5000);

  // First, try find existing row id
  {
    auto ex = get_by_source_id(d.org_id, d.source_id);
    if (ex) {
      d.id = ex->id;

      const char* sql = R"SQL(
        UPDATE documents
        SET file_name=?,
            title=?,
            author=?,
            created_at=?,
            stored_at_utc=?,
            deleted=?,
            deleted_at_utc=?,
            last_segment=?
        WHERE id=?;
      )SQL";

      sqlite3_stmt* st = nullptr;
      if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");

      sqlite3_bind_text(st, 1, d.file_name.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 2, d.title.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 3, d.author.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 4, d.created_at.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 5, d.stored_at_utc.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_int (st, 6, d.deleted);
      sqlite3_bind_text(st, 7, d.deleted_at_utc.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 8, d.last_segment.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_int64(st, 9, d.id);

      if (sqlite3_step(st) != SQLITE_DONE) {
        sqlite3_finalize(st);
        throw std::runtime_error("sqlite step failed (update)");
      }
      sqlite3_finalize(st);
      return d.id;
    }
  }

  // Insert new
  {
    const char* sql = R"SQL(
      INSERT INTO documents(org_id, source_id, file_name, title, author, created_at, stored_at_utc, deleted, deleted_at_utc, last_segment)
      VALUES(?,?,?,?,?,?,?,?,?,?);
    )SQL";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");

    sqlite3_bind_text(st, 1, d.org_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 2, d.source_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 3, d.file_name.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 4, d.title.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 5, d.author.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 6, d.created_at.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st, 7, d.stored_at_utc.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int (st, 8, d.deleted);
    sqlite3_bind_text(st, 9, d.deleted_at_utc.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(st,10, d.last_segment.c_str(), -1, SQLITE_TRANSIENT);

    if (sqlite3_step(st) != SQLITE_DONE) {
      sqlite3_finalize(st);
      throw std::runtime_error("sqlite step failed (insert)");
    }
    sqlite3_finalize(st);

    d.id = sqlite3_last_insert_rowid(db);
    return d.id;
  }
}

std::vector<DocRow> Storage::get_by_internal_ids(const std::string& org_id, const std::vector<int64_t>& ids) {
  std::vector<DocRow> out;
  if (ids.empty()) return out;

  // de-dup ids
  std::vector<int64_t> uniq;
  uniq.reserve(ids.size());
  {
    std::unordered_set<int64_t> seen;
    seen.reserve(ids.size() * 2);
    for (auto id : ids) {
      if (id <= 0) continue;
      if (seen.insert(id).second) uniq.push_back(id);
    }
  }
  if (uniq.empty()) return out;

  auto* db = (sqlite3*)db_;

  std::string sql =
    "SELECT id, org_id, source_id, file_name, title, author, created_at, stored_at_utc, deleted, deleted_at_utc, last_segment "
    "FROM documents WHERE org_id=? AND id IN (";

  for (size_t i = 0; i < uniq.size(); ++i) {
    if (i) sql += ",";
    sql += "?";
  }
  sql += ");";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
    throw std::runtime_error("sqlite prepare failed (get_by_internal_ids)");
  }

  int bind_i = 1;
  sqlite3_bind_text(st, bind_i++, org_id.c_str(), -1, SQLITE_TRANSIENT);
  for (auto id : uniq) sqlite3_bind_int64(st, bind_i++, id);

  while (sqlite3_step(st) == SQLITE_ROW) {
    out.push_back(row_from_stmt(st));
  }
  sqlite3_finalize(st);
  return out;
}

std::vector<DocRow> Storage::list_docs(const std::string& org_id, int limit, int offset) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    SELECT id, org_id, source_id, file_name, title, author, created_at, stored_at_utc,
           deleted, deleted_at_utc, last_segment
    FROM documents
    WHERE org_id=?
    ORDER BY id DESC
    LIMIT ? OFFSET ?;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");
  sqlite3_bind_text(st, 1, org_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int(st, 2, limit);
  sqlite3_bind_int(st, 3, offset);

  std::vector<DocRow> out;
  while (sqlite3_step(st) == SQLITE_ROW) {
    out.push_back(row_from_stmt(st));
  }
  sqlite3_finalize(st);
  return out;
}

void Storage::mark_deleted_by_source_id(const std::string& org_id, const std::string& source_id, const std::string& deleted_at_utc) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    UPDATE documents
    SET deleted=1, deleted_at_utc=?
    WHERE org_id=? AND source_id=?;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) throw std::runtime_error("sqlite prepare failed");
  sqlite3_bind_text(st, 1, deleted_at_utc.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 2, org_id.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text(st, 3, source_id.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    sqlite3_finalize(st);
    throw std::runtime_error("sqlite step failed (mark_deleted)");
  }
  sqlite3_finalize(st);
}

void Storage::update_last_segment_by_ids(const std::string& org_id,
                                        const std::vector<int64_t>& ids,
                                        const std::string& last_segment) {
  if (ids.empty()) return;
  auto* db = (sqlite3*)db_;
  sqlite3_busy_timeout(db, 5000);

  const char* sql = R"SQL(
    UPDATE documents SET last_segment=? WHERE org_id=? AND id=?;
  )SQL";

  sqlite3_stmt* st = nullptr;

  exec(db, "BEGIN IMMEDIATE;");
  try {
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) {
      throw std::runtime_error("sqlite prepare failed (update_last_segment_by_ids)");
    }

    for (auto id : ids) {
      sqlite3_reset(st);
      sqlite3_clear_bindings(st);

      sqlite3_bind_text(st, 1, last_segment.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_text(st, 2, org_id.c_str(), -1, SQLITE_TRANSIENT);
      sqlite3_bind_int64(st, 3, id);

      if (sqlite3_step(st) != SQLITE_DONE) {
        throw std::runtime_error("sqlite step failed (update_last_segment_by_ids)");
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
