// cpp/5_Service/main.cpp
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <cstdio>
#include <thread>
#include <chrono>
#include <vector>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/types.h>
#include <limits.h>
#include <fstream>
#include <optional>
#include <unordered_set>
#include <stdexcept>
#include <system_error>

#include "httplib.h"

#include "service.h"
#include "routes/routes.h"

#include "core/job_queue.h"
#include "core/file_lock.h"
#include "core/hot_cache.h"

#include <nlohmann/json.hpp>

#include "l5/manifest.h"
#include "l5/format.h"

namespace fs = std::filesystem;
using jjson = nlohmann::json;

static std::mutex g_admin_mu;

static std::string get_hostname_best_effort() {
  char buf[256];
  buf[0] = 0;
  if (::gethostname(buf, sizeof(buf) - 1) != 0) return "host";
  buf[sizeof(buf) - 1] = 0;
  return std::string(buf);
}

static std::string make_node_id() {
  return get_hostname_best_effort() + ":" + std::to_string((uint64_t)::getpid());
}

static fs::path queue_db_path(const fs::path& data_root) {
  return data_root / "queue.sqlite";
}

static unsigned env_u32_local(const char* k, unsigned defv) {
  const char* s = std::getenv(k);
  if (!s || !*s) return defv;
  char* end = nullptr;
  unsigned long v = std::strtoul(s, &end, 10);
  if (!end || *end != '\0') return defv;
  return (unsigned)v;
}

static unsigned l5_shards_count_guess_local() {
  unsigned n = env_u32_local("PLAGIO_L5_SHARDS", 32u);
  if (n < 1) n = 1;
  if (n > 256) n = 256;
  return n;
}

static void ensure_dirs_local(const fs::path& p) {
  std::error_code ec;
  fs::create_directories(p, ec);
  if (ec) throw std::runtime_error("mkdir failed: " + p.string() + " err=" + ec.message());
}

static bool write_text_file_atomic_best_effort_local(const fs::path& path, const std::string& content) {
  try {
    ensure_dirs_local(path.parent_path());
    fs::path tmp = path;
    tmp += ".tmp";

    std::ofstream out(tmp, std::ios::binary);
    if (!out) return false;
    out.write(content.data(), (std::streamsize)content.size());
    out.flush();
    if (!out) return false;

    std::error_code ec;
    fs::rename(tmp, path, ec);
    if (!ec) return true;

    fs::remove(path, ec);
    ec.clear();
    fs::rename(tmp, path, ec);
    return !ec;
  } catch (...) {
    return false;
  }
}

static std::string shard_dir_name_local(unsigned shard) {
  char buf[16];
  std::snprintf(buf, sizeof(buf), "s%02u", shard);
  return std::string(buf);
}

static std::string read_file_all(const fs::path& p) {
  std::ifstream in(p, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open: " + p.string());
  std::string s((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  return s;
}

static fs::path resolve_under(const fs::path& data_root, const std::string& p) {
  fs::path x(p);
  if (x.is_absolute()) return x;
  return data_root / x;
}

static bool write_manifest_file_local(const fs::path& out_root, const l5::Manifest& m) {
  jjson j = jjson::object();
  j["segments"] = jjson::array();
  for (const auto& se : m.segments) {
    jjson e = jjson::object();
    e["segment_name"] = se.segment_name;
    e["path"]         = se.path;
    e["built_at_utc"]  = se.built_at_utc;

    jjson st = jjson::object();
    st["docs"] = se.stats.docs;
    st["k9"]   = se.stats.k9;
    st["k13"]  = se.stats.k13;
    e["stats"] = st;

    j["segments"].push_back(e);
  }
  const fs::path fin = out_root / "level5_manifest.json";
  const fs::path tmp = out_root / "level5_manifest.json.tmp";
  ensure_dirs_local(out_root);

  std::ofstream out(tmp, std::ios::binary);
  if (!out) return false;
  const std::string content = j.dump();
  out.write(content.data(), (std::streamsize)content.size());
  out.flush();
  if (!out) return false;

  return l5::atomic_replace_file_best_effort(tmp, fin);
}
static jjson manifest_to_json_local(const l5::Manifest& m) {
  jjson out = jjson::array();
  for (const auto& se : m.segments) {
    jjson e = jjson::object();
    e["segment_name"] = se.segment_name;
    e["path"]         = se.path;
    e["built_at_utc"]  = se.built_at_utc;

    jjson st = jjson::object();
    st["docs"] = se.stats.docs;
    st["k9"]   = se.stats.k9;
    st["k13"]  = se.stats.k13;
    e["stats"] = st;

    out.push_back(e);
  }
  return out;
}

static std::vector<std::string> worker_types_for_kind(const std::string& kind) {
  if (kind == "compact") return {"COMPACT_SMALL", "COMPACT_L5", "REBUILD_ONE"};
  if (kind == "admin") return {"WIPE_ALL"};
  return {}; // empty => all
}

static int run_job(const fs::path& data_root, int64_t job_id) {
  JobQueue q(queue_db_path(data_root).string());
  q.init();
  auto job = q.get(job_id);
  if (!job) return 2;

  // Global barrier: normal jobs take SH, wipe takes EX
  FileLock global_lock(data_root / ".global.lock",
                       (job->type == "WIPE_ALL") ? FileLock::Mode::Exclusive : FileLock::Mode::Shared);

  try {
    jjson payload = job->payload.empty() ? jjson::object() : jjson::parse(job->payload);
    const std::string org_id = payload.value("org_id", "");

    // org lock for org-scoped jobs
    std::optional<FileLock> org_lock;
    if (!org_id.empty() && job->type != "WIPE_ALL") {
      org_lock.emplace(data_root / "orgs" / org_id / ".org.lock", FileLock::Mode::Exclusive);
    }

    L5Service svc{data_root};

    if (job->type == "INGEST_L5_ZIP") {
      const std::string zip_name = payload.value("zip_name", "");
      const std::string zip_path_s = payload.value("zip_path", "");
      const std::string segment_name = payload.value("segment_name", "");
      const bool normalize = payload.value("normalize", 1) != 0;
      std::optional<unsigned> shard;
      if (payload.contains("l5_shard")) shard = (unsigned)payload["l5_shard"].get<uint64_t>();

      const fs::path zip_path = resolve_under(data_root, zip_path_s);
      const std::string zip_bytes = read_file_all(zip_path);

      auto r = svc.ingest_l5_zip_build_segment(org_id, zip_name, zip_bytes, shard, segment_name, normalize);

      jjson out = {
        {"ok", true},
        {"type", "INGEST_L5_ZIP"},
        {"org_id", org_id},
        {"zip_name", zip_name},
        {"shard", r.shard},
        {"normalize", normalize ? 1 : 0},
        {"files_seen", r.files_seen},
        {"files_skipped", r.files_skipped},
        {"docs_indexed", r.docs_indexed},
        {"segment_name", r.build.segment_name},
        {"seg_dir", r.build.seg_dir.string()},
        {"docs", r.build.docs},
        {"post9", r.build.post9},
        {"built_at_utc", r.build.built_at_utc}
      };

      q.mark_done(job_id, out.dump());
      return 0;
    }

    if (job->type == "INDEX_TEXT") {
      const std::string document_id = payload.value("document_id", "");
      const std::string file_name   = payload.value("file_name", "");
      const std::string title       = payload.value("title", "");
      const std::string author      = payload.value("author", "");
      const std::string created_at  = payload.value("created_at", "");
      const std::string text_path_s = payload.value("text_path", "");
      const bool do_compact_small   = payload.value("do_compact_small", 1) != 0;
      const unsigned fanout         = (unsigned)payload.value("small_fanout", 20u);

      const fs::path text_path = resolve_under(data_root, text_path_s);
      const std::string text = read_file_all(text_path);

      auto r = svc.index_text_document(org_id, document_id, file_name, title, author, created_at, text);
      if (do_compact_small) (void)svc.compact_small_levels(org_id, fanout);

      jjson out = {
        {"ok", true},
        {"type", "INDEX_TEXT"},
        {"org_id", org_id},
        {"document_id", document_id},
        {"indexed_internal_id", std::to_string(r.doc.id)},
        {"last_segment", r.doc.last_segment},
        {"segment_name", r.build.segment_name},
        {"seg_dir", r.build.seg_dir.string()},
        {"docs", r.build.docs},
        {"post9", r.build.post9},
        {"built_at_utc", r.build.built_at_utc},
        {"compact_small", do_compact_small ? 1 : 0},
        {"fanout", fanout}
      };

      q.mark_done(job_id, out.dump());
      return 0;
    }

    if (job->type == "COMPACT_SMALL") {
      const unsigned fanout = (unsigned)payload.value("fanout", 20u);
      auto rep = svc.compact_small_levels(org_id, fanout);
      jjson out = {{"ok", true}, {"type","COMPACT_SMALL"}, {"org_id",org_id}, {"fanout",fanout},
                  {"rounds", rep.rounds}, {"merges", rep.merges}, {"new_segments", rep.new_segments}};
      q.mark_done(job_id, out.dump());
      return 0;
    }

    if (job->type == "COMPACT_L5") {
      const unsigned fanout = (unsigned)payload.value("fanout", 2u);
      auto rep = svc.compact_l5_shards(org_id, fanout);
      jjson out = {{"ok", true}, {"type","COMPACT_L5"}, {"org_id",org_id}, {"fanout",fanout},
                  {"rounds", rep.rounds}, {"merges", rep.merges}, {"new_segments", rep.new_segments}};
      q.mark_done(job_id, out.dump());
      return 0;
    }

    if (job->type == "REBUILD_ONE") {
      unsigned out_shard = (unsigned)payload.value("out_shard", 0u);
      unsigned fanout    = (unsigned)payload.value("fanout", 20u);
      bool do_compact    = payload.value("compact", 1) != 0;
      if (fanout < 2) fanout = 2;
      if (fanout > 200) fanout = 200;

      // org-wide exclusive lock already held
      const fs::path index_root = data_root / "orgs" / org_id / "index";
      const fs::path l5_root = index_root / "l5";
      ensure_dirs_local(l5_root);

      const unsigned nshards = l5_shards_count_guess_local();
      if (out_shard >= nshards) out_shard = 0;

      const fs::path out_root = l5_root / shard_dir_name_local(out_shard);
      ensure_dirs_local(out_root);

      l5::Manifest outm = l5::load_manifest(out_root);
      std::unordered_set<std::string> out_names;
      out_names.reserve(outm.segments.size() * 2 + 16);
      for (const auto& s : outm.segments) out_names.insert(s.segment_name);

      jjson before = jjson::object();
      jjson moved = jjson::array();

      for (unsigned sh = 0; sh < nshards; ++sh) {
        const fs::path shard_root = l5_root / shard_dir_name_local(sh);
        std::error_code ec;
        if (!fs::exists(shard_root, ec) || ec) { ec.clear(); continue; }

        l5::Manifest m = l5::load_manifest(shard_root);
        before[shard_dir_name_local(sh)] = (uint64_t)m.segments.size();

        if (sh == out_shard) continue;

        for (const auto& se : m.segments) {
          const fs::path src_dir = shard_root / se.segment_name;
          const fs::path dst_dir = out_root / se.segment_name;

          if (!fs::exists(src_dir, ec) || ec) { ec.clear(); continue; }
          if (fs::exists(dst_dir, ec) && !ec) {
            throw std::runtime_error("segment name collision in out_shard: " + se.segment_name);
          }
          ec.clear();

          fs::rename(src_dir, dst_dir, ec);
          if (ec) {
            throw std::runtime_error("rename failed: " + src_dir.string() + " -> " + dst_dir.string() + " err=" + ec.message());
          }

          {
            jjson mv = jjson::object();
            mv["from_shard"]    = shard_dir_name_local(sh);
            mv["segment_name"]  = se.segment_name;
            mv["docs"]          = se.stats.docs;
            mv["k9"]            = se.stats.k9;
            moved.push_back(mv);
          }
          

          if (out_names.insert(se.segment_name).second) {
            l5::SegmentEntry add = se;
            add.path = se.segment_name + "/";
            outm.segments.push_back(add);
          }
        }

        l5::Manifest empty;
        (void)write_manifest_file_local(shard_root, empty);
      }

      if (!write_manifest_file_local(out_root, outm)) {
        throw std::runtime_error("failed writing out_shard manifest");
      }

      uint32_t merges = 0;
      jjson compacted = jjson::array();
      if (do_compact) {
        l5::CompactOptions cop;
        cop.fanout = fanout;
        for (int guard = 0; guard < 100000; ++guard) {
          auto rr = l5::compact_once(out_root, out_root, cop);
          if (!rr.did_compact) break;
          merges += 1;
          compacted.push_back(rr.new_segment_name);
        }
      }

      const l5::Manifest fin = l5::load_manifest(out_root);

      jjson report = jjson::object();
      report["ok"] = true;
      report["type"] = "REBUILD_ONE";
      report["org_id"] = org_id;
      report["out_shard"] = out_shard;
      report["out_root"] = out_root.string();
      report["before_segments"] = before;
      report["moved_segments"] = moved;
      report["compaction_fanout"] = fanout;
      report["compaction_enabled"] = do_compact ? 1 : 0;
      report["compaction_merges"] = merges;
      report["new_segments"] = compacted;
      report["final_segments"] = manifest_to_json_local(fin);
      report["built_at_utc"] = l5::utc_now_compact();
      report["processed_at"] = L5Service::utc_now_iso_utc();

      (void)write_text_file_atomic_best_effort_local(l5_root / "rebuild_last.json", report.dump(2));
      q.mark_done(job_id, report.dump());
      return 0;
    }

    if (job->type == "WIPE_ALL") {
      const std::string confirm = payload.value("confirm", "");
      if (confirm != "WIPE_ALL") throw std::runtime_error("confirm required");

      const fs::path orgs_dir = data_root / "orgs";
      std::error_code ec;
      uintmax_t removed = 0;

      if (fs::exists(orgs_dir, ec)) {
        ec.clear();
        removed = fs::remove_all(orgs_dir, ec);
        if (ec) throw std::runtime_error("remove_all failed: " + ec.message());
      }
      ec.clear();
      fs::create_directories(orgs_dir, ec);
      if (ec) throw std::runtime_error("create_directories failed: " + ec.message());

      jjson out = {{"ok", true}, {"type","WIPE_ALL"}, {"removed_entries", (uint64_t)removed}};
      q.mark_done(job_id, out.dump());
      return 0;
    }

    throw std::runtime_error("unknown job type: " + job->type);

  } catch (const std::exception& e) {
    q.mark_failed(job_id, e.what());
    return 1;
  }
}

static int run_worker(const fs::path& data_root, const std::string& worker_kind, const char* argv0) {
  JobQueue q(queue_db_path(data_root).string());
  q.init();
  const std::string wid = make_node_id();
  const auto allowed = worker_types_for_kind(worker_kind);

  std::cout << "Worker kind=" << worker_kind << " node_id=" << wid << " queue=" << q.path() << "\n";

  while (true) {
    std::optional<JobRow> job;
    try {
      job = q.claim_next(wid, allowed);
    } catch (const std::exception& e) {
      std::cerr << "claim_next error: " << e.what() << "\n";
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    if (!job) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      continue;
    }

    pid_t pid = ::fork();
    if (pid < 0) {
      q.mark_failed(job->id, "fork failed");
      continue;
    }

    if (pid == 0) {
      // child -> exec self in job mode
      std::string a_mode = "--mode=job";
      std::string a_job  = "--job-id=" + std::to_string(job->id);
      std::string a_root = "--data-root=" + data_root.string();

      char* const args[] = {
        (char*)argv0,
        (char*)a_mode.c_str(),
        (char*)a_job.c_str(),
        (char*)a_root.c_str(),
        (char*)nullptr
      };
      ::execvp(argv0, args);
      // if exec failed:
      std::cerr << "execv failed\n";
      _exit(127);
    }

    int st = 0;
    ::waitpid(pid, &st, 0);
    int ec = 0;
    if (WIFEXITED(st)) ec = WEXITSTATUS(st);
    else if (WIFSIGNALED(st)) ec = 128 + WTERMSIG(st);

    if (ec != 0) {
      // if job-runner crashed before updating status, flip it from running->failed
      q.finalize_if_running(job->id, ec, "job process terminated");
    }
  }
  return 0;
}

static int run_api(const fs::path& data_root) {
  L5Service svc{data_root};
  JobQueue q(queue_db_path(data_root).string());
  q.init();
  HotCache hot;

  httplib::Server app;

  constexpr size_t MAX_JSON_BODY_BYTES   = 4ull * 1024 * 1024;
  constexpr size_t MAX_TEXT_BYTES        = 2ull * 1024 * 1024;
  constexpr size_t MAX_QUERY_BYTES       = 2ull * 1024 * 1024;
  constexpr size_t MAX_ZIP_UPLOAD_BYTES  = 10ull * 1024 * 1024 * 1024; // httplib keeps multipart in RAM

  app.set_read_timeout(3600, 0);
  app.set_write_timeout(3600, 0);
  app.set_payload_max_length(MAX_ZIP_UPLOAD_BYTES + 256ull * 1024 * 1024); // +multipart overhead

  ServiceRouteContext ctx;
  ctx.data_root = data_root;
  ctx.svc = &svc;
  ctx.admin_mu = &g_admin_mu;
  ctx.q = &q;
  ctx.hot = &hot;
  ctx.node_id = make_node_id();
  ctx.max_json_body_bytes  = MAX_JSON_BODY_BYTES;
  ctx.max_text_bytes       = MAX_TEXT_BYTES;
  ctx.max_query_bytes      = MAX_QUERY_BYTES;
  ctx.max_zip_upload_bytes = MAX_ZIP_UPLOAD_BYTES;

  register_routes(app, ctx);

  const char* host = "0.0.0.0";
  int port = 8088;
  std::cout << "L5 service (API) data_root=" << data_root.string() << " listen " << host << ":" << port << "\n";
  app.listen(host, port);
  return 0;
}

int main(int argc, char** argv) {
  std::string mode = "api";
  std::string worker_kind = "all";
  fs::path data_root = "./DATA_ROOT";
  int64_t job_id = 0;

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a.rfind("--mode=", 0) == 0) mode = a.substr(std::strlen("--mode="));
    else if (a.rfind("--worker-kind=", 0) == 0) worker_kind = a.substr(std::strlen("--worker-kind="));
    else if (a.rfind("--data-root=", 0) == 0) data_root = fs::path(a.substr(std::strlen("--data-root=")));
    else if (a.rfind("--job-id=", 0) == 0) job_id = std::stoll(a.substr(std::strlen("--job-id=")));
    else if (!a.empty() && a[0] != '-') data_root = fs::path(a); // backward compatible positional
  }

  if (mode == "api") return run_api(data_root);
  if (mode == "worker") return run_worker(data_root, worker_kind, argv[0]);
  if (mode == "job") {
    if (job_id <= 0) { std::cerr << "missing --job-id\n"; return 2; }
    return run_job(data_root, job_id);
  }

  std::cerr << "unknown mode: " << mode << "\n";
  return 2;
}
