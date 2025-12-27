#pragma once

#include <array>
#include <filesystem>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "l5/builder.h"
#include "l5/search_multi.h"
#include "storage.h"
#include "tombstone.h"

struct UploadResult {
  std::string doc_id;
  std::string external_id;
  std::string source_name;
  std::string stored_path;
  uint64_t bytes{0};
};

struct SkippedDoc {
  std::string external_id; // relative path inside zip
  std::string source_name; // original filename
  std::string reason;      // e.g. "convert_failed_no_txt"
};

struct IngestZipResult {
  l5::BuildStats build;
  std::vector<UploadResult> docs;
  std::vector<SkippedDoc> skipped;
};

class L5Service {
public:
  explicit L5Service(std::filesystem::path data_root);

  UploadResult ingest_file(const std::string& org_id,
                           const std::string& filename,
                           const std::string& bytes,
                           const std::string& external_id_opt,
                           bool text_is_normalized);

  // ZIP batch: one upload => one segment
  IngestZipResult ingest_zip_build_segment(const std::string& org_id,
                                           const std::string& zip_name,
                                           const std::string& zip_bytes,
                                           bool text_is_normalized,
                                           const std::string& segment_name_opt);

  l5::SearchResult search(const std::string& org_id,
                          const std::string& query,
                          bool query_is_normalized,
                          const l5::SearchOptions& opt);

  void delete_doc(const std::string& org_id, const std::string& key);
  std::vector<DocRow> list_docs(const std::string& org_id, int limit, int offset);

private:
  std::filesystem::path org_root(const std::string& org) const;
  std::filesystem::path org_index_root(const std::string& org) const;
  std::filesystem::path org_sqlite(const std::string& org) const;
  std::filesystem::path org_tombstones(const std::string& org) const;
  std::filesystem::path org_uploads_dir(const std::string& org) const;

  static std::string utc_now_iso();
  static std::string gen_uuid_v4(); // единое имя, без *_like

private:
  std::filesystem::path data_root_;

  // Сериализуем:
  // - build (manifest append + сегментные файлы)
  // - tombstones append/load
  // Шардирование по org_id => меньше contention, чем один глобальный mutex.
  static constexpr size_t kMutexShards = 64;

  std::array<std::mutex, kMutexShards> build_mu_{};
  std::array<std::mutex, kMutexShards> tomb_mu_{};

  static size_t shard(const std::string& org) {
    return std::hash<std::string>{}(org) % kMutexShards;
  }

  std::mutex& build_mu_for(const std::string& org) { return build_mu_[shard(org)]; }
  std::mutex& tomb_mu_for (const std::string& org) { return tomb_mu_[shard(org)]; }
};
