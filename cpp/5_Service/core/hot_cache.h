// NEW FILE: cpp/5_Service/core/hot_cache.h
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

class HotCache {
public:
  enum class Mode { Mmap, Mlock, Read };

  struct Item {
    std::string path;
    uint64_t size{0};
    uint64_t mtime_ns{0};
    Mode mode{Mode::Mmap};
    bool locked{false}; // mlock succeeded
  };

  HotCache();
  ~HotCache();

  HotCache(const HotCache&) = delete;
  HotCache& operator=(const HotCache&) = delete;

  bool pin_file(const std::filesystem::path& path, Mode mode, std::string* err = nullptr);

  bool pin_segment_dir(const std::filesystem::path& seg_dir,
                       bool pin_bin, bool pin_docids, bool pin_meta,
                       Mode mode, std::string* err = nullptr);

  bool unpin_file(const std::filesystem::path& path);
  void clear();
  void refresh_best_effort(); // drop missing/changed

  std::vector<Item> list() const;
  uint64_t total_bytes() const;

  static Mode parse_mode(const std::string& s, Mode def = Mode::Mmap);
  static const char* mode_name(Mode m);

private:
  struct PinnedFile;
  static std::string norm_key(const std::filesystem::path& p);
  static uint64_t stat_mtime_ns(const std::filesystem::path& p, uint64_t* size_out, std::string* err);

  bool pin_one_unlocked_(const std::filesystem::path& path, Mode mode, std::string* err);
  void unpin_one_unlocked_(const std::string& key);

private:
  mutable std::mutex mu_;
  std::unordered_map<std::string, std::unique_ptr<PinnedFile>> files_;
};
