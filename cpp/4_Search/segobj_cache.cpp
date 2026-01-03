// NEW FILE: Back_Last/cpp/4_Search/segobj_cache.cpp
#include "segobj_cache.h"

#include <sys/stat.h>
#include <cstdlib>
#include <cerrno>
#include <cstring>

namespace fs = std::filesystem;

namespace l5 {

static uint64_t to_ns(time_t sec, long nsec) {
  return (uint64_t)sec * 1000000000ull + (uint64_t)nsec;
}

SegmentObjectCache::SegmentObjectCache(uint64_t budget_bytes) : budget_(budget_bytes) {}

void SegmentObjectCache::set_budget(uint64_t budget_bytes) {
  std::lock_guard<std::mutex> lk(mu_);
  budget_ = budget_bytes;
  evict_if_needed_();
}

void SegmentObjectCache::clear() {
  std::lock_guard<std::mutex> lk(mu_);
  map_.clear();
  inflight_.clear();
  lru_.clear();
  total_ = 0;
}

SegmentObjectCache::Stats SegmentObjectCache::stats() const {
  std::lock_guard<std::mutex> lk(mu_);
  Stats s;
  s.entries = (uint64_t)map_.size();
  s.bytes = total_;
  for (const auto& kv : map_) {
    if (kv.second.pinned) {
      s.pinned_entries++;
      s.pinned_bytes += kv.second.bin_bytes;
    }
  }
  return s;
}

std::vector<SegmentObjectCache::Item> SegmentObjectCache::list() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<Item> out;
  out.reserve(map_.size());
  for (const auto& kv : map_) {
    Item it;
    it.seg_dir = kv.first;
    it.bin_bytes = kv.second.bin_bytes;
    it.mtime_ns = kv.second.mtime_ns;
    it.pinned = kv.second.pinned;
    out.push_back(std::move(it));
  }
  return out;
}

std::string SegmentObjectCache::key_of(const fs::path& seg_dir) {
  return fs::absolute(seg_dir).lexically_normal().generic_string();
}

bool SegmentObjectCache::stat_bin(const fs::path& seg_dir, uint64_t& bytes, uint64_t& mtime_ns, std::string* err) {
  const fs::path p = seg_dir / "index_native.bin";
  struct stat st{};
  if (::stat(p.c_str(), &st) != 0) {
    if (err) *err = std::string("stat failed: ") + p.string() + " : " + std::strerror(errno);
    return false;
  }
  bytes = (uint64_t)st.st_size;
#if defined(__linux__)
  mtime_ns = to_ns(st.st_mtim.tv_sec, st.st_mtim.tv_nsec);
#else
  mtime_ns = (uint64_t)st.st_mtime * 1000000000ull;
#endif
  return true;
}

void SegmentObjectCache::touch_lru_(Entry& e) {
  lru_.splice(lru_.begin(), lru_, e.it);
}

void SegmentObjectCache::evict_if_needed_() {
  if (budget_ == 0) return; // disabled
  while (total_ > budget_ && !lru_.empty()) {
    const std::string key = lru_.back();
    auto it = map_.find(key);
    if (it == map_.end()) { lru_.pop_back(); continue; }
    if (it->second.pinned) { 
      // can't evict pinned -> stop
      break;
    }
    total_ -= it->second.bin_bytes;
    lru_.pop_back();
    map_.erase(it);
  }
}

std::shared_ptr<const SegmentData> SegmentObjectCache::get_or_load(
    const fs::path& seg_dir,
    const std::function<std::shared_ptr<SegmentData>(std::string* err)>& loader,
    std::string* err) {

  const std::string key = key_of(seg_dir);
  uint64_t bytes = 0, mt = 0;
  std::string st_err;
  if (!stat_bin(seg_dir, bytes, mt, &st_err)) {
    if (err) *err = st_err;
    return {};
  }

  std::shared_ptr<Inflight> inf;
  bool i_am_loader = false;

  // 1) Try cache / join inflight / create inflight
  {
    std::unique_lock<std::mutex> lk(mu_);

    // cache hit?
    auto it = map_.find(key);
    if (it != map_.end()) {
      if (it->second.mtime_ns == mt && it->second.bin_bytes == bytes && it->second.seg) {
        lru_.splice(lru_.begin(), lru_, it->second.it);
        return it->second.seg;
      }
      // stale -> drop
      total_ -= it->second.bin_bytes;
      lru_.erase(it->second.it);
      map_.erase(it);
    }

    auto it_inf = inflight_.find(key);
    if (it_inf != inflight_.end()) {
      inf = it_inf->second;
    } else {
      inf = std::make_shared<Inflight>();
      inflight_[key] = inf;
      i_am_loader = true;
    }
  }

  // 2) If not loader -> wait and return inflight result
  if (!i_am_loader) {
    std::unique_lock<std::mutex> lk(inf->mu);
    inf->cv.wait(lk, [&]{ return inf->done; });
    if (!inf->seg) {
      if (err) *err = inf->err.empty() ? "load failed" : inf->err;
      return {};
    }
    return inf->seg;
  }

  // 3) Loader path: load outside locks
  std::string load_err;
  auto seg = loader(&load_err);

  // 4) Publish to cache + clear inflight entry
  {
    std::unique_lock<std::mutex> lk(mu_);
    inflight_.erase(key);

    if (seg && budget_ != 0) {
      Entry e;
      e.seg = seg;
      e.bin_bytes = bytes;
      e.mtime_ns = mt;
      e.pinned = false;
      lru_.push_front(key);
      e.it = lru_.begin();
      map_[key] = std::move(e);
      total_ += bytes;
      evict_if_needed_();
    }
  }

  // 5) Notify waiters
  {
    std::lock_guard<std::mutex> lk(inf->mu);
    inf->seg = seg;
    inf->err = load_err;
    inf->done = true;
  }
  inf->cv.notify_all();

  if (!seg) {
    if (err) *err = load_err.empty() ? "load failed" : load_err;
    return {};
  }
  return seg;

  // wait
  {
    std::unique_lock<std::mutex> lk(inf->mu);
    inf->cv.wait(lk, [&]{ return inf->done; });
  }

  if (!inf->seg) {
    if (err) *err = inf->err.empty() ? "load failed" : inf->err;
    return {};
  }

  // cache might already contain it; return from cache for consistency
  {
    std::lock_guard<std::mutex> lk(mu_);
    inflight_.erase(key);
    auto it = map_.find(key);
    if (it != map_.end() && it->second.seg) return it->second.seg;
    Entry e;
    e.seg = inf->seg;
    e.bin_bytes = bytes;
    e.mtime_ns = mt;
    e.pinned = false;
    lru_.push_front(key);
    e.it = lru_.begin();
    map_[key] = std::move(e);
    total_ += bytes;
    evict_if_needed_();
    return map_[key].seg;
  }
}

bool SegmentObjectCache::pin(const fs::path& seg_dir) {
  const std::string key = key_of(seg_dir);
  std::lock_guard<std::mutex> lk(mu_);
  auto it = map_.find(key);
  if (it == map_.end()) return false;
  it->second.pinned = true;
  return true;
}

bool SegmentObjectCache::unpin(const fs::path& seg_dir) {
  const std::string key = key_of(seg_dir);
  std::lock_guard<std::mutex> lk(mu_);
  auto it = map_.find(key);
  if (it == map_.end()) return false;
  it->second.pinned = false;
  evict_if_needed_();
  return true;
}
uint64_t segobj_cache_budget_from_env() {
  const char* s = std::getenv("PLAGIO_SEGOBJ_CACHE_BYTES");
  if (!s || !*s) return 0;
  return (uint64_t)std::strtoull(s, nullptr, 10);
}

SegmentObjectCache& segobj_cache() {
  static SegmentObjectCache g(segobj_cache_budget_from_env());
  return g;
}

} // namespace l5
