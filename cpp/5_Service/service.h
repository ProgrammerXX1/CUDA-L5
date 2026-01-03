#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "l5/builder.h"
#include "l5/search_multi.h"
#include "l5/search_segment.h"
#include "l5/compactor.h"

#include "storage.h"
#include "tombstone.h"

struct IndexTextResult {
  l5::BuildStats build;
  DocRow doc;                  // includes internal id + last_segment
  int indexed_level{1};         // 1 or 5 (heuristic)
  std::optional<unsigned> l5_shard;
};

struct CompactReport {
  uint32_t rounds{0};
  uint32_t merges{0};
  std::vector<std::string> new_segments;
};

struct L5ZipIngestResult {
  l5::BuildStats build;
  uint64_t files_seen{0};
  uint64_t files_skipped{0};
  uint64_t docs_indexed{0};
  unsigned shard{0};
};

struct L5FsIngestResult {
  l5::BuildStats build;
  uint64_t files_seen{0};
  uint64_t files_skipped{0};
  uint64_t docs_indexed{0};
  unsigned shard{0};
  bool skipped_existing{false};
};

class L5Service {
public:
  explicit L5Service(std::filesystem::path data_root);

  // L5: ingest .txt/.doc/.docx files from ZIP into ONE segment (normalization enabled)
  L5ZipIngestResult ingest_l5_zip_build_segment(const std::string& org_id,
                                                const std::string& zip_name,
                                                const std::string& zip_bytes,
                                                std::optional<unsigned> l5_shard_opt,
                                                const std::string& segment_name_opt,bool normalize);

  // L5: ingest from local filesystem dirs (.txt) into ONE segment (10k docs per job recommended)
  L5FsIngestResult ingest_l5_fs_dirs_build_segment(const std::string& org_id,
                                                   const std::string& dataset_root,
                                                   const std::string& dataset_prefix,
                                                   const std::vector<std::string>& src_dirs,
                                                   unsigned shard,
                                                   const std::string& segment_name,
                                                   bool normalize,
                                                   bool recursive);
  // Index one document provided as TEXT (backend-normalized; no core normalization)
  IndexTextResult index_text_document(const std::string& org_id,
                                      const std::string& source_id,
                                      const std::string& file_name,
                                      const std::string& title,
                                      const std::string& author,
                                      const std::string& created_at,
                                      const std::string& text);

  // Search in selected levels (1..4) and/or L5 (level=5). For L5 you can pass shard list; empty => all shards.
  l5::SearchResult search_levels(const std::string& org_id,
                                 const std::string& query,
                                 bool query_is_normalized,
                                 const std::vector<int>& levels,
                                 const std::vector<unsigned>& l5_shards,
                                 const l5::SearchOptions& opt);

  std::vector<DocRow> get_docs_by_internal_ids(const std::string& org_id,
                                               const std::vector<int64_t>& ids);

  CompactReport compact_small_levels(const std::string& org_id, unsigned fanout);
  CompactReport compact_l5_shards(const std::string& org_id, unsigned fanout);

  std::filesystem::path data_root() const { return data_root_; }

  // public timestamp helper (main.cpp uses it)
  static std::string utc_now_iso_utc();

private:
  std::filesystem::path org_root(const std::string& org) const;
  std::filesystem::path org_index_base(const std::string& org) const;
  std::filesystem::path org_level_root(const std::string& org, int level) const;
  std::filesystem::path org_l5_shard_root(const std::string& org, unsigned shard) const;

  std::filesystem::path org_sqlite(const std::string& org) const;
  std::filesystem::path org_tombstones(const std::string& org) const;

  unsigned l5_shards_count() const;
  unsigned pick_l5_shard(const std::string& seed) const;

  static std::string mk_tmp_dir(const std::string& prefix);

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
