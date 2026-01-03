// Back_Last/cpp/5_Service/core/service_ingest.cpp
#include "service.h"
#include "core/service_internal.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <zip.h>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace { using namespace svc_detail; }

// -------------------- shell helpers --------------------
static std::string shell_quote(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 8);
  out.push_back('\'');
  for (char c : s) {
    if (c == '\'') out += "'\\''";
    else out.push_back(c);
  }
  out.push_back('\'');
  return out;
}

static int run_cmd_bash(const std::string& cmd) {
  std::string full = "bash -lc " + shell_quote(cmd);
  return std::system(full.c_str());
}

// -------------------- zip slip protection --------------------
static bool zip_entry_name_is_safe(const std::string& name) {
  if (name.empty()) return false;
  if (name.find('\0') != std::string::npos) return false;
  if (!name.empty() && name[0] == '/') return false;
  if (name.find('\\') != std::string::npos) return false;

  fs::path rel = fs::path(name).lexically_normal();
  if (rel.empty()) return false;
  if (rel.is_absolute()) return false;

  for (const auto& part : rel) {
    if (part == "..") return false;
  }
  return true;
}

static void unzip_libzip_safe_buffer(const std::string& zip_bytes,
                                     const fs::path& dst_dir,
                                     size_t max_files,
                                     uint64_t max_total_bytes) {
  ensure_dirs(dst_dir);

  zip_error_t ze;
  zip_error_init(&ze);

  zip_source_t* zs = zip_source_buffer_create(zip_bytes.data(), zip_bytes.size(), 0, &ze);
  if (!zs) { zip_error_fini(&ze); throw std::runtime_error("zip_source_buffer_create failed"); }

  zip_t* za = zip_open_from_source(zs, ZIP_RDONLY, &ze);
  if (!za) { zip_source_free(zs); zip_error_fini(&ze); throw std::runtime_error("zip_open_from_source failed"); }

  auto za_guard = std::unique_ptr<zip_t, decltype(&zip_close)>(za, &zip_close);
  zip_error_fini(&ze);

  const zip_int64_t n = zip_get_num_entries(za, 0);
  if (n < 0) throw std::runtime_error("zip_get_num_entries failed");
  if ((size_t)n > max_files) throw std::runtime_error("zip too many entries");

  uint64_t total = 0;

  for (zip_uint64_t i = 0; i < (zip_uint64_t)n; ++i) {
    zip_stat_t st;
    zip_stat_init(&st);
    if (zip_stat_index(za, i, 0, &st) != 0) continue;

    std::string name = st.name ? st.name : "";
    if (name.empty()) continue;

    // dir
    if (!name.empty() && name.back() == '/') {
      if (!zip_entry_name_is_safe(name)) throw std::runtime_error("unsafe zip dir entry: " + name);
      fs::path rel = fs::path(name).lexically_normal();
      ensure_dirs(dst_dir / rel);
      continue;
    }

    if (!zip_entry_name_is_safe(name)) throw std::runtime_error("unsafe zip entry: " + name);

    total += (uint64_t)st.size;
    if (total > max_total_bytes) throw std::runtime_error("zip exceeds max_total_bytes");

    fs::path rel = fs::path(name).lexically_normal();
    fs::path out = dst_dir / rel;
    ensure_dirs(out.parent_path());

    zip_file_t* zf = zip_fopen_index(za, i, 0);
    if (!zf) throw std::runtime_error("zip_fopen_index failed for entry: " + name);

    std::ofstream fout(out, std::ios::binary);
    if (!fout) { zip_fclose(zf); throw std::runtime_error("cannot write: " + out.string()); }

    char buf[1 << 16];
    while (true) {
      zip_int64_t rd = zip_fread(zf, buf, sizeof(buf));
      if (rd < 0) { zip_fclose(zf); throw std::runtime_error("zip_fread failed for entry: " + name); }
      if (rd == 0) break;
      fout.write(buf, (std::streamsize)rd);
      if (!fout) { zip_fclose(zf); throw std::runtime_error("write failed: " + out.string()); }
    }
    zip_fclose(zf);
  }
}

// -------------------- soffice parallel conversion --------------------
static void soffice_convert_parallel(const fs::path& conv_src,
                                     const fs::path& conv_out,
                                     const fs::path& profiles_base,
                                     unsigned procs,
                                     unsigned batch_n) {
  std::vector<fs::path> inputs;
  inputs.reserve(4096);

  for (auto it = fs::directory_iterator(conv_src); it != fs::directory_iterator(); ++it) {
    std::error_code ec;
    if (!it->is_regular_file(ec) || ec) continue;
    inputs.push_back(it->path());
  }
  if (inputs.empty()) return;

  unsigned hw = std::thread::hardware_concurrency();
  if (hw == 0) hw = 4;

  if (procs == 0) procs = 1;
  if (procs > hw) procs = hw;
  if (procs > (unsigned)inputs.size()) procs = (unsigned)inputs.size();
  if (batch_n == 0) batch_n = (unsigned)inputs.size();

  ensure_dirs(conv_out);
  ensure_dirs(profiles_base);

  const size_t n = inputs.size();
  const size_t per = (n + procs - 1) / procs;

  std::vector<std::thread> th;
  th.reserve(procs);
  std::vector<std::exception_ptr> errs(procs);

  for (unsigned i = 0; i < procs; ++i) {
    const size_t start = (size_t)i * per;
    const size_t end = std::min(n, start + per);
    if (start >= end) break;

    th.emplace_back([&, i, start, end]() {
      try {
        const fs::path profile_dir = profiles_base / ("lo_profile_" + std::to_string(i));
        ensure_dirs(profile_dir);

        const fs::path abs_profile = fs::absolute(profile_dir);
        const std::string profile_uri = "file://" + abs_profile.string();

        const fs::path list_path = profiles_base / ("files_" + std::to_string(i) + ".lst");
        {
          std::ofstream lst(list_path, std::ios::binary);
          if (!lst) throw std::runtime_error("cannot open list file: " + list_path.string());
          for (size_t k = start; k < end; ++k) {
            const std::string s = inputs[k].string();
            lst.write(s.data(), (std::streamsize)s.size());
            lst.put('\0');
          }
          lst.flush();
          if (!lst) throw std::runtime_error("cannot write list file: " + list_path.string());
        }

        std::string cmd =
          "cat " + shell_quote(list_path.string()) +
          " | xargs -0 -n " + std::to_string(batch_n) +
          " soffice --headless --nologo --nolockcheck --nodefault --norestore"
          " -env:UserInstallation=" + shell_quote(profile_uri) +
          " --convert-to " + shell_quote("txt:Text (encoded):UTF8") +
          " --outdir " + shell_quote(conv_out.string());

        const int rc = run_cmd_bash(cmd);
        if (rc != 0) throw std::runtime_error("soffice convert failed rc=" + std::to_string(rc));
      } catch (...) {
        errs[i] = std::current_exception();
      }
    });
  }

  for (auto& t : th) t.join();
  for (auto& e : errs) if (e) std::rethrow_exception(e);
}

static fs::path replace_ext_txt(fs::path p) {
  p.replace_extension(".txt");
  return p;
}

static void copy_file_binary(const fs::path& src, const fs::path& dst) {
  std::ifstream in(src, std::ios::binary);
  if (!in) throw std::runtime_error("cannot read file: " + src.string());
  std::ofstream out(dst, std::ios::binary);
  if (!out) throw std::runtime_error("cannot write file: " + dst.string());
  out << in.rdbuf();
  out.flush();
  if (!out) throw std::runtime_error("copy failed: " + src.string() + " -> " + dst.string());
}

static bool copy_file_best_effort(const fs::path& src, const fs::path& dst) {
  std::error_code ec;
  fs::create_directories(dst.parent_path(), ec);
  ec.clear();
  fs::copy_file(src, dst, fs::copy_options::overwrite_existing, ec);
  return !ec;
}

// -------------------- index_text_document --------------------
IndexTextResult L5Service::index_text_document(const std::string& org_id,
                                               const std::string& source_id,
                                               const std::string& file_name,
                                               const std::string& title,
                                               const std::string& author,
                                               const std::string& created_at,
                                               const std::string& text_in) {
  if (org_id.empty()) throw std::invalid_argument("org_id is empty");
  if (source_id.empty()) throw std::invalid_argument("document_id/source_id is empty");
  if (file_name.empty()) throw std::invalid_argument("file_name is empty");

  ensure_dirs(org_root(org_id));
  ensure_dirs(org_index_base(org_id));
  ensure_dirs(org_level_root(org_id, 1));
  ensure_dirs(org_level_root(org_id, 2));
  ensure_dirs(org_level_root(org_id, 3));
  ensure_dirs(org_level_root(org_id, 4));
  ensure_dirs(org_index_base(org_id) / "l5");

  Storage st(org_sqlite(org_id).string());
  st.init();

  DocRow row;
  row.org_id = org_id;
  row.source_id = source_id;
  row.file_name = file_name;
  row.title = title;
  row.author = author;
  row.created_at = created_at;
  row.stored_at_utc = utc_now_iso_utc();
  row.deleted = 0;
  row.deleted_at_utc = "";
  row.last_segment = "";

  const int64_t internal_id = st.upsert_doc_get_id(row);

  l5::BuildOptions opt;
  opt.segment_name = make_unique_segment_name();
  opt.max_threads = std::min<unsigned>(std::max(1u, std::thread::hardware_concurrency()),
                                      env_u32("PLAGIO_BUILD_THREADS", BUILD_THREADS_DEFAULT));
  opt.ram_limit_bytes = env_u64("PLAGIO_SORT_RAM_BYTES", SORT_RAM_BYTES_DEFAULT);

  const fs::path out_root = org_level_root(org_id, 1);

  const fs::path tmp = fs::path(mk_tmp_dir("l5_text"));
  const fs::path corpus = tmp / "corpus.jsonl";

  std::string text = text_in;
  clip_text_utf8_inplace(text, (size_t)opt.max_text_bytes_per_doc);

  {
    std::ofstream out(corpus, std::ios::binary);
    if (!out) throw std::runtime_error("cannot write corpus.jsonl");

    const char* text_is_normalized_flag = "false"; // core will normalize (stable contract)

    out
      << "{\"doc_id\":" << json(std::to_string(internal_id)).dump()
      << ",\"organization_id\":" << json(org_id).dump()
      << ",\"external_id\":" << json(source_id).dump()
      << ",\"source_path\":" << json("").dump()
      << ",\"source_name\":" << json(file_name).dump()
      << ",\"text\":" << json(text).dump()
      << ",\"text_is_normalized\":" << text_is_normalized_flag
      << "}\n";

    out.flush();
    if (!out) throw std::runtime_error("write failed corpus.jsonl");
  }

  IndexTextResult res;
  {
    std::lock_guard<std::mutex> lk(build_mu_for(org_id));
    res.build = l5::build_segment_jsonl(corpus, out_root, opt);
  }

  const std::string last_segment_rel = "l1/" + res.build.segment_name;
  st.update_last_segment_by_ids(org_id, std::vector<int64_t>{internal_id}, last_segment_rel);

  row.id = internal_id;
  row.last_segment = last_segment_rel;
  res.doc = row;
  res.indexed_level = 1;
  res.l5_shard = std::nullopt;

  {
    std::error_code ec;
    fs::remove_all(tmp, ec);
  }
  return res;
}

// -------------------- ingest_l5_zip_build_segment (parallel) --------------------
L5ZipIngestResult L5Service::ingest_l5_zip_build_segment(const std::string& org_id,
                                                         const std::string& zip_name,
                                                         const std::string& zip_bytes,
                                                         std::optional<unsigned> l5_shard_opt,
                                                         const std::string& segment_name_opt,
                                                         bool normalize) {
  if (org_id.empty()) throw std::invalid_argument("org_id is empty");

  ensure_dirs(org_root(org_id));
  ensure_dirs(org_index_base(org_id));
  ensure_dirs(org_index_base(org_id) / "l5");

  Storage st(org_sqlite(org_id).string());
  st.init();

  const unsigned shard = l5_shard_opt.has_value() ? *l5_shard_opt : pick_l5_shard(zip_name);
  const fs::path out_root = org_l5_shard_root(org_id, shard);
  ensure_dirs(out_root);

  l5::BuildOptions opt;
  opt.segment_name = segment_name_opt.empty() ? make_unique_segment_name() : segment_name_opt;
  opt.max_threads = std::min<unsigned>(std::max(1u, std::thread::hardware_concurrency()),
                                       env_u32("PLAGIO_BUILD_THREADS", BUILD_THREADS_DEFAULT));
  opt.ram_limit_bytes = env_u64("PLAGIO_SORT_RAM_BYTES", SORT_RAM_BYTES_DEFAULT);

  const size_t   ZIP_MAX_FILES = (size_t)env_u32("PLAGIO_L5_ZIP_MAX_FILES", (unsigned)ZIP_MAX_FILES_DEFAULT);
  const uint64_t ZIP_MAX_TOTAL = env_u64("PLAGIO_L5_ZIP_MAX_TOTAL_BYTES", ZIP_MAX_TOTAL_BYTES_DEF);

  const fs::path tmp = fs::path(mk_tmp_dir("l5_zip"));
  const fs::path unpack_dir = tmp / "unpacked";
  const fs::path conv_src = tmp / "conv_src";
  const fs::path conv_out = tmp / "conv_out";
  const fs::path profiles = tmp / "lo_profiles";

  ensure_dirs(unpack_dir);
  ensure_dirs(conv_src);
  ensure_dirs(conv_out);
  ensure_dirs(profiles);

  // 1) unzip to disk (safe)
  unzip_libzip_safe_buffer(zip_bytes, unpack_dir, ZIP_MAX_FILES, ZIP_MAX_TOTAL);

  struct Pending {
    std::string rel_in_zip;
    std::string source_id;
    std::string file_name;
    fs::path src_path;
    bool needs_convert{false};
    fs::path text_path;
    int64_t internal_id{0};
    std::string upload_rel;
  };

  std::vector<Pending> pending;
  pending.reserve(4096);

  // 2) scan files
  {
    std::error_code ec;
    fs::recursive_directory_iterator it(unpack_dir, fs::directory_options::skip_permission_denied, ec);
    fs::recursive_directory_iterator end;
    for (; it != end; it.increment(ec)) {
      if (ec) { ec.clear(); continue; }
      if (!it->is_regular_file(ec) || ec) { ec.clear(); continue; }

      const fs::path p = it->path();
      const std::string ext = lower_ext(p);

      const bool is_txt  = (ext == ".txt");
      const bool is_doc  = (ext == ".doc");
      const bool is_docx = (ext == ".docx");
      if (!is_txt && !is_doc && !is_docx) continue;

      Pending d;
      d.src_path = p;
      d.file_name = basename_of(p);

      std::string rel = fs::relative(p, unpack_dir, ec).generic_string();
      if (ec || rel.empty()) { ec.clear(); rel = d.file_name; }
      d.rel_in_zip = rel;
      d.source_id = zip_name + "::" + rel;
      d.needs_convert = !is_txt;

      pending.push_back(std::move(d));
    }
  }

  L5ZipIngestResult rr;
  rr.shard = shard;
  rr.files_seen = (uint64_t)pending.size();

  if (pending.empty()) throw std::runtime_error("zip has no supported files (.txt/.doc/.docx)");

  // 3) pre-assign internal ids in sqlite (single-thread)
  for (auto& d : pending) {
    DocRow row;
    row.org_id = org_id;
    row.source_id = d.source_id;
    row.file_name = d.file_name;
    row.title = "";
    row.author = "";
    row.created_at = "";
    row.stored_at_utc = utc_now_iso_utc();
    row.deleted = 0;
    row.deleted_at_utc = "";
    row.last_segment = "";
    d.internal_id = st.upsert_doc_get_id(row);
  }

  const fs::path uploads_dir = org_root(org_id) / "uploads";
  ensure_dirs(uploads_dir);

  for (auto& d : pending) {
    const fs::path dst = uploads_dir / d.file_name; // basename
    if (copy_file_best_effort(d.src_path, dst)) {
      d.upload_rel = (fs::path("uploads") / d.file_name).generic_string();
    } else {
      d.upload_rel.clear();
    }
  }

  // 4) prepare doc/docx for conversion
  for (auto& d : pending) {
    if (!d.needs_convert) {
      d.text_path = d.src_path; // txt directly
      continue;
    }
    const fs::path unique_in = conv_src / (std::to_string(d.internal_id) + "_" + d.file_name);
    copy_file_binary(d.src_path, unique_in);
    d.text_path = conv_out / replace_ext_txt(unique_in.filename());
  }

  // 5) convert in parallel (doc/docx -> txt)
  {
    bool has_any = false;
    for (auto it = fs::directory_iterator(conv_src); it != fs::directory_iterator(); ++it) {
      std::error_code ec;
      if (it->is_regular_file(ec) && !ec) { has_any = true; break; }
    }
    if (has_any) {
      unsigned hw = std::thread::hardware_concurrency();
      if (hw == 0) hw = 4;

      const unsigned build_thr = opt.max_threads;
      const unsigned def_procs = std::min<unsigned>(CONVERT_PROCS_FALLBACK, build_thr);
      unsigned procs = env_u32("PLAGIO_L5_CONVERT_PROCS", def_procs);
      unsigned batch = env_u32("PLAGIO_L5_CONVERT_BATCH", CONVERT_BATCH_DEFAULT);

      if (procs == 0) procs = 1;
      if (procs > hw) procs = hw;

      soffice_convert_parallel(conv_src, conv_out, profiles, procs, batch);
    }
  }

  // 6) build corpus parts in parallel
  const unsigned hw = std::max(1u, std::thread::hardware_concurrency());
  const unsigned build_thr = std::min<unsigned>(hw, opt.max_threads);

  unsigned n_threads = env_u32("PLAGIO_L5_EXTRACT_THREADS", std::min<unsigned>(build_thr, EXTRACT_THREADS_CAP));
  if (n_threads == 0) n_threads = 1;
  if (n_threads > (unsigned)pending.size()) n_threads = (unsigned)pending.size();
  if (n_threads == 0) n_threads = 1;

  std::vector<fs::path> part_paths;
  part_paths.reserve(n_threads);
  for (unsigned t = 0; t < n_threads; ++t) {
    part_paths.push_back(tmp / ("corpus_part_" + std::to_string(t) + ".jsonl"));
  }

  struct ThreadAccum {
    uint64_t skipped{0};
    std::vector<int64_t> indexed_ids;
  };

  std::vector<ThreadAccum> acc(n_threads);
  std::vector<std::thread> workers;
  workers.reserve(n_threads);
  std::vector<std::exception_ptr> errs(n_threads);

  std::atomic<size_t> next{0};

  const std::string org_j = json(org_id).dump();
  const char* text_is_normalized_flag = normalize ? "false" : "true";
  const size_t cap = (size_t)opt.max_text_bytes_per_doc;

  for (unsigned t = 0; t < n_threads; ++t) {
    workers.emplace_back([&, t]() {
      try {
        std::ofstream outp(part_paths[t], std::ios::binary);
        if (!outp) throw std::runtime_error("cannot open corpus part: " + part_paths[t].string());

        auto& A = acc[t];
        A.indexed_ids.reserve(1024);

        while (true) {
          size_t i = next.fetch_add(1, std::memory_order_relaxed);
          if (i >= pending.size()) break;

          auto& d = pending[i];

          if (d.needs_convert) {
            std::error_code ec;
            if (!fs::exists(d.text_path, ec) || ec) { A.skipped++; continue; }
          }

          std::string text = read_text_utf8_best_effort(d.text_path, cap);
          if (text.empty()) { A.skipped++; continue; }

          outp
            << "{\"doc_id\":" << json(std::to_string(d.internal_id)).dump()
            << ",\"organization_id\":" << org_j
            << ",\"external_id\":" << json(d.source_id).dump()
            << ",\"source_path\":" << json(d.upload_rel).dump()
            << ",\"source_name\":" << json(d.file_name).dump()
            << ",\"text\":" << json(text).dump()
            << ",\"text_is_normalized\":" << text_is_normalized_flag
            << "}\n";

          A.indexed_ids.push_back(d.internal_id);
        }

        outp.flush();
        if (!outp) throw std::runtime_error("write failed corpus part: " + part_paths[t].string());
      } catch (...) {
        errs[t] = std::current_exception();
      }
    });
  }

  for (auto& th : workers) th.join();
  for (auto& e : errs) if (e) std::rethrow_exception(e);

  // merge parts -> corpus.jsonl
  const fs::path corpus = tmp / "corpus.jsonl";
  {
    std::ofstream corpus_out(corpus, std::ios::binary);
    if (!corpus_out) throw std::runtime_error("cannot open corpus.jsonl");

    for (auto& pp : part_paths) {
      std::ifstream in(pp, std::ios::binary);
      if (!in) continue;
      corpus_out << in.rdbuf();
    }
    corpus_out.flush();
    if (!corpus_out) throw std::runtime_error("failed writing corpus.jsonl");
  }

  std::vector<int64_t> indexed_ids;
  uint64_t skipped = 0;
  for (auto& a : acc) {
    skipped += a.skipped;
    for (auto id : a.indexed_ids) indexed_ids.push_back(id);
  }

  rr.files_skipped = skipped;
  rr.docs_indexed = (uint64_t)indexed_ids.size();
  if (rr.docs_indexed == 0) throw std::runtime_error("no documents extracted for indexing (all skipped)");

  // 7) build segment
  {
    std::lock_guard<std::mutex> lk(build_mu_for(org_id));
    rr.build = l5::build_segment_jsonl(corpus, out_root, opt);
  }

  // 8) update last_segment for successfully indexed docs
  const std::string last_segment_rel = "l5/" + shard_dir_name(shard) + "/" + rr.build.segment_name;
  st.update_last_segment_by_ids(org_id, indexed_ids, last_segment_rel);

  // cleanup tmp
  {
    std::error_code ec;
    fs::remove_all(tmp, ec);
  }

  return rr;
}

// -------------------- ingest_l5_fs_dirs_build_segment --------------------
L5FsIngestResult L5Service::ingest_l5_fs_dirs_build_segment(const std::string& org_id,
                                                            const std::string& dataset_root,
                                                            const std::string& dataset_prefix,
                                                            const std::vector<std::string>& src_dirs,
                                                            unsigned shard,
                                                            const std::string& segment_name,
                                                            bool normalize,
                                                            bool recursive) {
  if (org_id.empty()) throw std::invalid_argument("org_id is empty");
  if (src_dirs.empty()) throw std::invalid_argument("src_dirs is empty");

  ensure_dirs(org_root(org_id));
  ensure_dirs(org_index_base(org_id));
  ensure_dirs(org_index_base(org_id) / "l5");

  Storage st(org_sqlite(org_id).string());
  st.init();

  const fs::path out_root = org_l5_shard_root(org_id, shard);
  ensure_dirs(out_root);

  l5::BuildOptions opt;
  opt.segment_name = segment_name.empty() ? make_unique_segment_name() : segment_name;
  opt.max_threads = std::min<unsigned>(std::max(1u, std::thread::hardware_concurrency()),
                                       env_u32("PLAGIO_BUILD_THREADS", BUILD_THREADS_DEFAULT));
  opt.ram_limit_bytes = env_u64("PLAGIO_SORT_RAM_BYTES", SORT_RAM_BYTES_DEFAULT);

  // idempotency: if segment dir already exists -> skip
  {
    std::error_code ec;
    if (fs::exists(out_root / opt.segment_name, ec) && !ec) {
      L5FsIngestResult rr;
      rr.shard = shard;
      rr.skipped_existing = true;
      rr.build.segment_name = opt.segment_name;
      rr.build.seg_dir = out_root / opt.segment_name;
      rr.build.built_at_utc = l5::utc_now_compact();
      return rr;
    }
  }

  const fs::path tmp = fs::path(mk_tmp_dir("l5_fs"));
  const fs::path corpus = tmp / "corpus.jsonl";

  const fs::path ds_root = dataset_root.empty() ? fs::path() : fs::path(dataset_root);
  const std::string org_j = json(org_id).dump();
  const std::string prefix = dataset_prefix.empty() ? std::string("fs") : dataset_prefix;

  const char* text_is_normalized_flag = normalize ? "false" : "true"; // normalize=true => core will normalize
  const size_t cap = (size_t)opt.max_text_bytes_per_doc;

  // 1) collect files
  std::vector<fs::path> files;
  files.reserve(10000);
  for (const auto& d : src_dirs) {
    fs::path root = fs::path(d);
    std::error_code ec;
    if (!fs::exists(root, ec) || ec) continue;
    if (recursive) {
      fs::recursive_directory_iterator it(root, fs::directory_options::skip_permission_denied, ec), end;
      for (; it != end; it.increment(ec)) {
        if (ec) { ec.clear(); continue; }
        if (!it->is_regular_file(ec) || ec) { ec.clear(); continue; }
        if (lower_ext(it->path()) != ".txt") continue;
        files.push_back(it->path());
      }
    } else {
      fs::directory_iterator it(root, ec), end;
      for (; it != end; it.increment(ec)) {
        if (ec) { ec.clear(); continue; }
        if (!it->is_regular_file(ec) || ec) { ec.clear(); continue; }
        if (lower_ext(it->path()) != ".txt") continue;
        files.push_back(it->path());
      }
    }
  }
  std::sort(files.begin(), files.end());
  files.erase(std::unique(files.begin(), files.end()), files.end());

  L5FsIngestResult rr;
  rr.shard = shard;
  rr.files_seen = (uint64_t)files.size();
  if (files.empty()) throw std::runtime_error("no .txt files found in src_dirs");

  // 2) bulk upsert docs (assign internal ids)
  std::vector<DocRow> rows;
  rows.reserve(files.size());

  const std::string now = utc_now_iso_utc();

  for (const auto& p : files) {
    std::string rel;
    if (!dataset_root.empty()) {
      std::error_code ec;
      rel = fs::relative(p, ds_root, ec).generic_string();
      if (ec || rel.empty() || rel == ".") { ec.clear(); rel = p.filename().generic_string(); }
    } else {
      rel = p.filename().generic_string();
    }

    DocRow r;
    r.org_id = org_id;
    r.source_id = prefix + "::" + rel; // unique across dataset
    r.file_name = basename_of(p);
    r.title = "";
    r.author = "";
    r.created_at = "";
    r.stored_at_utc = now;
    r.deleted = 0;
    r.deleted_at_utc = "";
    r.last_segment = "";
    rows.push_back(std::move(r));
  }

  auto ids = st.upsert_docs_get_ids_bulk(rows);
  if (ids.size() != rows.size()) throw std::runtime_error("bulk upsert mismatch");

  // 3) write corpus.jsonl
  std::ofstream out(corpus, std::ios::binary);
  if (!out) throw std::runtime_error("cannot write corpus.jsonl");

  std::vector<int64_t> indexed_ids;
  indexed_ids.reserve(rows.size());

  for (size_t i = 0; i < files.size(); ++i) {
    const auto& p = files[i];
    auto& row = rows[i];

    std::string rel;
    if (!dataset_root.empty()) {
      std::error_code ec;
      rel = fs::relative(p, ds_root, ec).generic_string();
      if (ec || rel.empty() || rel == ".") { ec.clear(); rel = p.filename().generic_string(); }
    } else {
      rel = p.filename().generic_string();
    }

    std::string text = read_text_utf8_best_effort(p, cap);
    if (text.empty()) { rr.files_skipped++; continue; }

    out
      << "{\"doc_id\":" << json(std::to_string(row.id)).dump()
      << ",\"organization_id\":" << org_j
      << ",\"external_id\":" << json(row.source_id).dump()
      << ",\"source_path\":" << json(rel).dump()
      << ",\"source_name\":" << json(rel).dump()
      << ",\"text\":" << json(text).dump()
      << ",\"text_is_normalized\":" << text_is_normalized_flag
      << "}\n";

    indexed_ids.push_back(row.id);
  }
  out.flush();
  if (!out) throw std::runtime_error("write failed corpus.jsonl");

  rr.docs_indexed = (uint64_t)indexed_ids.size();
  if (rr.docs_indexed == 0) throw std::runtime_error("no documents extracted for indexing (all skipped)");

  // 4) build segment (serialize per org)
  {
    std::lock_guard<std::mutex> lk(build_mu_for(org_id));
    rr.build = l5::build_segment_jsonl(corpus, out_root, opt);
  }

  // 5) update last_segment for indexed docs
  const std::string last_segment_rel = "l5/" + shard_dir_name(shard) + "/" + rr.build.segment_name;
  st.update_last_segment_by_ids(org_id, indexed_ids, last_segment_rel);

  // cleanup tmp
  {
    std::error_code ec;
    fs::remove_all(tmp, ec);
  }
  return rr;
}
