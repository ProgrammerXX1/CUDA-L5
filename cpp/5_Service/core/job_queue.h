// NEW FILE: Back_Last/cpp/5_Service/core/job_queue.h
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

struct sqlite3;

struct JobRow {
  int64_t id{0};
  std::string status;
  std::string type;
  int priority{0};
  int64_t created_at{0};
  int64_t run_after{0};
  int attempts{0};
  int max_attempts{0};
  std::string locked_by;
  int64_t locked_at{0};
  int64_t started_at{0};
  int64_t finished_at{0};
  std::string payload;   // JSON string
  std::string result;    // JSON string
  std::string error;     // text
};

class JobQueue {
public:
  explicit JobQueue(const std::string& db_path);
  ~JobQueue();

  void init();

  int64_t enqueue(const std::string& type,
                  const std::string& payload_json,
                  int priority = 0,
                  int64_t run_after_unix = 0,
                  int max_attempts = 3);

  bool update_payload(int64_t id, const std::string& payload_json);

  std::optional<JobRow> get(int64_t id);

  // allowed_types empty => accept any type
  std::optional<JobRow> claim_next(const std::string& worker_id,
                                   const std::vector<std::string>& allowed_types);

  bool mark_done(int64_t id, const std::string& result_json);
  bool mark_failed(int64_t id, const std::string& error, int64_t retry_after_unix = 0);

  // If job is still "running" (e.g., crash before updating), mark it failed.
  bool finalize_if_running(int64_t id, int exit_code, const std::string& note);

  const std::string& path() const { return path_; }

private:
  static int64_t now_unix();
  void open_();
  void close_();

private:
  sqlite3* db_{nullptr};
  std::string path_;
};
