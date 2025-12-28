#pragma once

#include <array>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "l5/builder.h"
#include "l5/search_multi.h"
#include "storage.h"
#include "tombstone.h"

struct UploadResult {
  std::string org_id;
  std::string doc_id;
  std::string external_id;
  std::string source_name;
  std::string stored_path;
  uint64_t bytes{0};
};

struct SkippedDoc {
  std::string external_id;
  std::string source_name;
  std::string reason;
};

struct IngestZipResult {
  l5::BuildStats build;
  std::vector<UploadResult> docs;
  std::vector<SkippedDoc> skipped;
};

struct IngestOneResult {
  l5::BuildStats build;
  UploadResult doc;
};

struct CompactReport {
  uint32_t rounds{0};
  uint32_t merges{0};
  std::vector<std::string> new_segments;
};

class L5Service {
public:
  explicit L5Service(std::filesystem::path data_root);

  // Single file -> build one segment into target level (default L1)
  IngestOneResult ingest_file_build_segment(const std::string& org_id,
                                            const std::string& filename,
                                            const std::string& bytes,
                                            const std::string& external_id_opt,
                                            bool text_is_normalized,
                                            int target_level,
                                            std::optional<unsigned> l5_shard_opt,
                                            const std::string& segment_name_opt);

  // ZIP batch -> build one segment into target level (default L5 shard)
  IngestZipResult ingest_zip_build_segment(const std::string& org_id,
                                           const std::string& zip_name,
                                           const std::string& zip_bytes,
                                           bool text_is_normalized,
                                           int target_level,
                                           std::optional<unsigned> l5_shard_opt,
                                           const std::string& segment_name_opt);

  // Search in selected levels (1..4) and/or L5 (level=5). For L5 you can pass shard list; empty => all shards.
  l5::SearchResult search_levels(const std::string& org_id,
                                 const std::string& query,
                                 bool query_is_normalized,
                                 const std::vector<int>& levels,
                                 const std::vector<unsigned>& l5_shards,
                                 const l5::SearchOptions& opt);

  void delete_doc(const std::string& org_id, const std::string& key);
  std::vector<DocRow> list_docs(const std::string& org_id, int limit, int offset);

  // Admin compaction:
  CompactReport compact_small_levels(const std::string& org_id, unsigned fanout);
  CompactReport compact_l5_shards(const std::string& org_id, unsigned fanout);

private:
  std::filesystem::path org_root(const std::string& org) const;
  std::filesystem::path org_sqlite(const std::string& org) const;
  std::filesystem::path org_tombstones(const std::string& org) const;
  std::filesystem::path org_uploads_dir(const std::string& org) const;

  std::filesystem::path org_index_base(const std::string& org) const;
  std::filesystem::path org_level_root(const std::string& org, int level) const;
  std::filesystem::path org_l5_shard_root(const std::string& org, unsigned shard) const;

  unsigned l5_shards_count() const;
  unsigned pick_l5_shard(const std::string& seed) const;

  static std::string utc_now_iso();
  static std::string gen_uuid_v4();

private:
  std::filesystem::path data_root_;

  static constexpr size_t kMutexShards = 64;
  std::array<std::mutex, kMutexShards> build_mu_{};
  std::array<std::mutex, kMutexShards> tomb_mu_{};

  static size_t shard(const std::string& org) {
    return std::hash<std::string>{}(org) % kMutexShards;
  }

  std::mutex& build_mu_for(const std::string& org) { return build_mu_[shard(org)]; }
  std::mutex& tomb_mu_for (const std::string& org) { return tomb_mu_[shard(org)]; }
};
