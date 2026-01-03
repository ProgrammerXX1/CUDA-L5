// NEW FILE: Back_Last/cpp/4_Search/segobj_cache.h
#pragma once

#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "l5/search_segment.h" // SegmentData

namespace l5 {

class SegmentObjectCache {
public:
  struct Item {
    std::string seg_dir;
    uint64_t bin_bytes{0};
    uint64_t mtime_ns{0};
    bool pinned{false};
  };

  struct Stats {
    uint64_t entries{0};
    uint64_t bytes{0};
    uint64_t pinned_entries{0};
    uint64_t pinned_bytes{0};
  };

  explicit SegmentObjectCache(uint64_t budget_bytes = 0);

  uint64_t budget_bytes() const {
    std::lock_guard<std::mutex> lk(mu_);
    return budget_;
  }
  void set_budget(uint64_t budget_bytes);
  void clear();

  Stats stats() const;
  std::vector<Item> list() const;

  // loader must return shared_ptr<SegmentData> or nullptr on fail.
  std::shared_ptr<const SegmentData> get_or_load(
      const std::filesystem::path& seg_dir,
      const std::function<std::shared_ptr<SegmentData>(std::string* err)>& loader,
      std::string* err = nullptr);

  bool pin(const std::filesystem::path& seg_dir);
  bool unpin(const std::filesystem::path& seg_dir);

private:
  struct Entry {
    std::shared_ptr<SegmentData> seg;
    uint64_t bin_bytes{0};
    uint64_t mtime_ns{0};
    bool pinned{false};
    std::list<std::string>::iterator it;
  };

  struct Inflight {
    std::mutex mu;
    std::condition_variable cv;
    bool done{false};
    std::shared_ptr<SegmentData> seg;
    std::string err;
  };

  static std::string key_of(const std::filesystem::path& seg_dir);
  static bool stat_bin(const std::filesystem::path& seg_dir, uint64_t& bytes, uint64_t& mtime_ns, std::string* err);

  void touch_lru_(Entry& e);
  void evict_if_needed_();

private:
  mutable std::mutex mu_;
  uint64_t budget_{0};
  uint64_t total_{0};
  std::list<std::string> lru_; // front = MRU
  std::unordered_map<std::string, Entry> map_;
  std::unordered_map<std::string, std::shared_ptr<Inflight>> inflight_;
};
// Global per-process cache instance (used by search + admin routes)
SegmentObjectCache& segobj_cache();
uint64_t segobj_cache_budget_from_env();


} // namespace l5
