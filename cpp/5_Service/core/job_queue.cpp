// NEW FILE: Back_Last/cpp/5_Service/core/job_queue.cpp
#include "core/job_queue.h"

#include <sqlite3.h>

#include <ctime>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace fs = std::filesystem;

static void exec(sqlite3* db, const char* sql) {
  char* err = nullptr;
  if (sqlite3_exec(db, sql, nullptr, nullptr, &err) != SQLITE_OK) {
    std::string msg = err ? err : "sqlite error";
    sqlite3_free(err);
    throw std::runtime_error(msg);
  }
}

static inline std::string col_text(sqlite3_stmt* st, int i) {
  const unsigned char* c = sqlite3_column_text(st, i);
  return c ? std::string((const char*)c) : std::string();
}

static inline int64_t col_i64(sqlite3_stmt* st, int i) {
  return (int64_t)sqlite3_column_int64(st, i);
}

static inline int col_i(sqlite3_stmt* st, int i) {
  return sqlite3_column_int(st, i);
}

int64_t JobQueue::now_unix() { return (int64_t)std::time(nullptr); }

JobQueue::JobQueue(const std::string& db_path) : path_(db_path) { open_(); }
JobQueue::~JobQueue() { close_(); }

void JobQueue::open_() {
  fs::path p(path_);
  std::error_code ec;
  if (!p.parent_path().empty()) fs::create_directories(p.parent_path(), ec);

  sqlite3* db = nullptr;
  if (sqlite3_open(path_.c_str(), &db) != SQLITE_OK) {
    throw std::runtime_error("cannot open sqlite queue: " + path_);
  }
  db_ = db;

  sqlite3_busy_timeout(db_, 5000);
  exec(db_, "PRAGMA journal_mode=WAL;");
  exec(db_, "PRAGMA synchronous=NORMAL;");
  exec(db_, "PRAGMA temp_store=MEMORY;");
}

void JobQueue::close_() {
  if (db_) {
    sqlite3_close(db_);
    db_ = nullptr;
  }
}

void JobQueue::init() {
  auto* db = (sqlite3*)db_;
  exec(db, R"SQL(
    CREATE TABLE IF NOT EXISTS job_queue (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      created_at   INTEGER NOT NULL,
      run_after    INTEGER NOT NULL,
      status       TEXT    NOT NULL,
      type         TEXT    NOT NULL,
      priority     INTEGER NOT NULL DEFAULT 0,
      attempts     INTEGER NOT NULL DEFAULT 0,
      max_attempts INTEGER NOT NULL DEFAULT 3,
      locked_by    TEXT,
      locked_at    INTEGER,
      started_at   INTEGER,
      finished_at  INTEGER,
      payload      TEXT    NOT NULL DEFAULT '{}',
      result       TEXT,
      error        TEXT
    );

    CREATE INDEX IF NOT EXISTS job_queue_pick_idx
      ON job_queue(status, run_after, priority DESC, id);
  )SQL");
}

int64_t JobQueue::enqueue(const std::string& type,
                          const std::string& payload_json,
                          int priority,
                          int64_t run_after_unix,
                          int max_attempts) {
  auto* db = (sqlite3*)db_;
  const int64_t now = now_unix();
  if (run_after_unix <= 0) run_after_unix = now;
  if (max_attempts < 1) max_attempts = 1;

  const char* sql = R"SQL(
    INSERT INTO job_queue(created_at, run_after, status, type, priority, attempts, max_attempts, payload)
    VALUES(?, ?, 'queued', ?, ?, 0, ?, ?);
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) {
    throw std::runtime_error("sqlite prepare failed (enqueue)");
  }

  sqlite3_bind_int64(st, 1, now);
  sqlite3_bind_int64(st, 2, run_after_unix);
  sqlite3_bind_text (st, 3, type.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int  (st, 4, priority);
  sqlite3_bind_int  (st, 5, max_attempts);
  sqlite3_bind_text (st, 6, payload_json.c_str(), -1, SQLITE_TRANSIENT);

  if (sqlite3_step(st) != SQLITE_DONE) {
    sqlite3_finalize(st);
    throw std::runtime_error("sqlite step failed (enqueue)");
  }
  sqlite3_finalize(st);

  return (int64_t)sqlite3_last_insert_rowid(db);
}

bool JobQueue::update_payload(int64_t id, const std::string& payload_json) {
  auto* db = (sqlite3*)db_;
  const char* sql = "UPDATE job_queue SET payload=? WHERE id=?;";
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) return false;
  sqlite3_bind_text (st, 1, payload_json.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 2, id);
  const bool ok = (sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);
  return ok && sqlite3_changes(db) == 1;
}

std::optional<JobRow> JobQueue::get(int64_t id) {
  auto* db = (sqlite3*)db_;
  const char* sql = R"SQL(
    SELECT id, status, type, priority, created_at, run_after,
           attempts, max_attempts,
           COALESCE(locked_by,''), COALESCE(locked_at,0),
           COALESCE(started_at,0), COALESCE(finished_at,0),
           payload, COALESCE(result,''), COALESCE(error,'')
    FROM job_queue
    WHERE id=?
    LIMIT 1;
  )SQL";

  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) return std::nullopt;
  sqlite3_bind_int64(st, 1, id);
  if (sqlite3_step(st) != SQLITE_ROW) {
    sqlite3_finalize(st);
    return std::nullopt;
  }

  JobRow r;
  r.id          = col_i64(st, 0);
  r.status      = col_text(st, 1);
  r.type        = col_text(st, 2);
  r.priority    = col_i  (st, 3);
  r.created_at  = col_i64(st, 4);
  r.run_after   = col_i64(st, 5);
  r.attempts    = col_i  (st, 6);
  r.max_attempts= col_i  (st, 7);
  r.locked_by   = col_text(st, 8);
  r.locked_at   = col_i64(st, 9);
  r.started_at  = col_i64(st,10);
  r.finished_at = col_i64(st,11);
  r.payload     = col_text(st,12);
  r.result      = col_text(st,13);
  r.error       = col_text(st,14);
  sqlite3_finalize(st);
  return r;
}

std::optional<JobRow> JobQueue::claim_next(const std::string& worker_id,
                                           const std::vector<std::string>& allowed_types) {
  auto* db = (sqlite3*)db_;
  const int64_t now = now_unix();

  exec(db, "BEGIN IMMEDIATE;");
  try {
    std::string sql =
      "SELECT id, status, type, priority, created_at, run_after, attempts, max_attempts, payload "
      "FROM job_queue WHERE status='queued' AND run_after<=?";

    if (!allowed_types.empty()) {
      sql += " AND type IN (";
      for (size_t i = 0; i < allowed_types.size(); ++i) {
        if (i) sql += ",";
        sql += "?";
      }
      sql += ")";
    }

    sql += " ORDER BY priority DESC, id LIMIT 1;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql.c_str(), -1, &st, nullptr) != SQLITE_OK) {
      throw std::runtime_error("sqlite prepare failed (claim select)");
    }

    int bi = 1;
    sqlite3_bind_int64(st, bi++, now);
    for (const auto& t : allowed_types) {
      sqlite3_bind_text(st, bi++, t.c_str(), -1, SQLITE_TRANSIENT);
    }

    if (sqlite3_step(st) != SQLITE_ROW) {
      sqlite3_finalize(st);
      exec(db, "COMMIT;");
      return std::nullopt;
    }

    JobRow r;
    r.id           = col_i64(st, 0);
    r.status       = col_text(st, 1);
    r.type         = col_text(st, 2);
    r.priority     = col_i  (st, 3);
    r.created_at   = col_i64(st, 4);
    r.run_after    = col_i64(st, 5);
    r.attempts     = col_i  (st, 6);
    r.max_attempts = col_i  (st, 7);
    r.payload      = col_text(st, 8);
    sqlite3_finalize(st);

    const char* up = R"SQL(
      UPDATE job_queue
      SET status='running',
          locked_by=?,
          locked_at=?,
          started_at=COALESCE(started_at, ?),
          attempts=attempts+1
      WHERE id=? AND status='queued';
    )SQL";

    sqlite3_stmt* st2 = nullptr;
    if (sqlite3_prepare_v2(db, up, -1, &st2, nullptr) != SQLITE_OK) {
      throw std::runtime_error("sqlite prepare failed (claim update)");
    }
    sqlite3_bind_text (st2, 1, worker_id.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st2, 2, now);
    sqlite3_bind_int64(st2, 3, now);
    sqlite3_bind_int64(st2, 4, r.id);

    if (sqlite3_step(st2) != SQLITE_DONE) {
      sqlite3_finalize(st2);
      throw std::runtime_error("sqlite step failed (claim update)");
    }
    sqlite3_finalize(st2);

    if (sqlite3_changes(db) != 1) {
      throw std::runtime_error("claim lost race (unexpected)");
    }

    exec(db, "COMMIT;");

    r.status = "running";
    r.locked_by = worker_id;
    r.locked_at = now;
    r.started_at = now;     // good enough
    r.attempts += 1;        // reflect increment
    return r;

  } catch (...) {
    try { exec(db, "ROLLBACK;"); } catch (...) {}
    throw;
  }
}

bool JobQueue::mark_done(int64_t id, const std::string& result_json) {
  auto* db = (sqlite3*)db_;
  const int64_t now = now_unix();
  const char* sql = R"SQL(
    UPDATE job_queue
    SET status='done',
        result=?,
        error=NULL,
        finished_at=?,
        locked_by=NULL,
        locked_at=NULL
    WHERE id=?;
  )SQL";
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) return false;
  sqlite3_bind_text (st, 1, result_json.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 2, now);
  sqlite3_bind_int64(st, 3, id);
  const bool ok = (sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);
  return ok && sqlite3_changes(db) == 1;
}

bool JobQueue::mark_failed(int64_t id, const std::string& error, int64_t retry_after_unix) {
  auto* db = (sqlite3*)db_;
  const int64_t now = now_unix();

  // read attempts/max_attempts
  int attempts = 0;
  int max_attempts = 0;
  {
    const char* sel = "SELECT attempts, max_attempts FROM job_queue WHERE id=? LIMIT 1;";
    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sel, -1, &st, nullptr) != SQLITE_OK) return false;
    sqlite3_bind_int64(st, 1, id);
    if (sqlite3_step(st) == SQLITE_ROW) {
      attempts = col_i(st, 0);
      max_attempts = col_i(st, 1);
    }
    sqlite3_finalize(st);
    if (max_attempts <= 0) max_attempts = 1;
  }

  const bool can_retry = attempts < max_attempts;
  int64_t run_after = now;
  if (can_retry) {
    if (retry_after_unix > 0) run_after = retry_after_unix;
    else {
      // quadratic backoff: 5s, 20s, 45s... capped 1h
      const int64_t backoff = std::min<int64_t>(3600, 5ll * attempts * attempts);
      run_after = now + backoff;
    }
  }

  const char* up = R"SQL(
    UPDATE job_queue
    SET status=?,
        error=?,
        run_after=?,
        finished_at=?,
        locked_by=NULL,
        locked_at=NULL
    WHERE id=?;
  )SQL";
  sqlite3_stmt* st2 = nullptr;
  if (sqlite3_prepare_v2(db, up, -1, &st2, nullptr) != SQLITE_OK) return false;
  const std::string status = can_retry ? "queued" : "failed";
  sqlite3_bind_text (st2, 1, status.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_text (st2, 2, error.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st2, 3, run_after);
  sqlite3_bind_int64(st2, 4, now);
  sqlite3_bind_int64(st2, 5, id);
  const bool ok = (sqlite3_step(st2) == SQLITE_DONE);
  sqlite3_finalize(st2);
  return ok && sqlite3_changes(db) == 1;
}

bool JobQueue::finalize_if_running(int64_t id, int exit_code, const std::string& note) {
  auto* db = (sqlite3*)db_;
  const int64_t now = now_unix();
  const std::string err = note + " exit_code=" + std::to_string(exit_code);
  const char* sql = R"SQL(
    UPDATE job_queue
    SET status='failed',
        error=?,
        finished_at=?,
        locked_by=NULL,
        locked_at=NULL
    WHERE id=? AND status='running';
  )SQL";
  sqlite3_stmt* st = nullptr;
  if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) return false;
  sqlite3_bind_text (st, 1, err.c_str(), -1, SQLITE_TRANSIENT);
  sqlite3_bind_int64(st, 2, now);
  sqlite3_bind_int64(st, 3, id);
  const bool ok = (sqlite3_step(st) == SQLITE_DONE);
  sqlite3_finalize(st);
  return ok && sqlite3_changes(db) == 1;
}
