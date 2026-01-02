// NEW FILE: cpp/5_Service/core/hot_cache.cpp
#include "core/hot_cache.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace fs = std::filesystem;

struct HotCache::PinnedFile {
  std::string path;
  uint64_t size{0};
  uint64_t mtime_ns{0};
  Mode mode{Mode::Mmap};
  bool locked{false};

  int fd{-1};
  void* addr{nullptr};
  std::vector<char> buf; // for Read mode

  ~PinnedFile() {
    if (addr && size) {
      if (locked) ::munlock(addr, (size_t)size);
      ::munmap(addr, (size_t)size);
      addr = nullptr;
    }
    if (fd >= 0) { ::close(fd); fd = -1; }
  }
};

HotCache::HotCache() = default;
HotCache::~HotCache() = default;

const char* HotCache::mode_name(Mode m) {
  switch (m) {
    case Mode::Mmap:  return "mmap";
    case Mode::Mlock: return "mlock";
    case Mode::Read:  return "read";
  }
  return "mmap";
}

HotCache::Mode HotCache::parse_mode(const std::string& s, Mode def) {
  if (s == "mmap")  return Mode::Mmap;
  if (s == "mlock") return Mode::Mlock;
  if (s == "read")  return Mode::Read;
  return def;
}

std::string HotCache::norm_key(const fs::path& p) {
  fs::path a = fs::absolute(p).lexically_normal();
  return a.generic_string();
}

uint64_t HotCache::stat_mtime_ns(const fs::path& p, uint64_t* size_out, std::string* err) {
  struct stat st{};
  if (::stat(p.c_str(), &st) != 0) {
    if (err) *err = std::string("stat failed: ") + std::strerror(errno);
    return 0;
  }
  if (size_out) *size_out = (uint64_t)st.st_size;
#if defined(__linux__)
  const uint64_t sec = (uint64_t)st.st_mtim.tv_sec;
  const uint64_t nsec = (uint64_t)st.st_mtim.tv_nsec;
  return sec * 1000000000ull + nsec;
#else
  return (uint64_t)st.st_mtime * 1000000000ull;
#endif
}

bool HotCache::pin_one_unlocked_(const fs::path& path, Mode mode, std::string* err) {
  const std::string key = norm_key(path);

  uint64_t sz = 0;
  std::string st_err;
  const uint64_t mt = stat_mtime_ns(path, &sz, &st_err);
  if (mt == 0) { if (err) *err = st_err; return false; }
  if (sz == 0) { if (err) *err = "file size is 0"; return false; }

  // already pinned and unchanged?
  auto it = files_.find(key);
  if (it != files_.end()) {
    auto& pf = it->second;
    if (pf && pf->mtime_ns == mt && pf->size == sz && pf->mode == mode) return true;
    unpin_one_unlocked_(key);
  }

  auto pf = std::make_unique<PinnedFile>();
  pf->path = key;
  pf->size = sz;
  pf->mtime_ns = mt;
  pf->mode = mode;

  if (mode == Mode::Read) {
    pf->buf.resize((size_t)sz);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { if (err) *err = std::string("open failed: ") + std::strerror(errno); return false; }
    pf->fd = fd;

    size_t off = 0;
    while (off < (size_t)sz) {
      const ssize_t rd = ::read(fd, pf->buf.data() + off, (size_t)sz - off);
      if (rd < 0) { if (err) *err = std::string("read failed: ") + std::strerror(errno); return false; }
      if (rd == 0) break;
      off += (size_t)rd;
    }
    // best-effort warm: touch
    if (!pf->buf.empty()) {
      volatile unsigned char x = 0;
      x ^= (unsigned char)pf->buf[0];
      (void)x;
    }

    files_[key] = std::move(pf);
    return true;
  }

  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) { if (err) *err = std::string("open failed: ") + std::strerror(errno); return false; }
  pf->fd = fd;

  int flags = MAP_PRIVATE;
#ifdef MAP_POPULATE
  flags |= MAP_POPULATE; // prefault into RAM
#endif
  void* addr = ::mmap(nullptr, (size_t)sz, PROT_READ, flags, fd, 0);
  if (addr == MAP_FAILED) {
    if (err) *err = std::string("mmap failed: ") + std::strerror(errno);
    return false;
  }
  pf->addr = addr;

  // best-effort hints
#ifdef POSIX_FADV_WILLNEED
  ::posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
#endif
#ifdef MADV_WILLNEED
  ::madvise(addr, (size_t)sz, MADV_WILLNEED);
#endif

  if (mode == Mode::Mlock) {
    if (::mlock(addr, (size_t)sz) == 0) pf->locked = true;
    else pf->locked = false; // not fatal
  }

  files_[key] = std::move(pf);
  return true;
}

void HotCache::unpin_one_unlocked_(const std::string& key) {
  auto it = files_.find(key);
  if (it != files_.end()) files_.erase(it);
}

bool HotCache::pin_file(const fs::path& path, Mode mode, std::string* err) {
  std::lock_guard<std::mutex> lk(mu_);
  return pin_one_unlocked_(path, mode, err);
}

bool HotCache::pin_segment_dir(const fs::path& seg_dir,
                               bool pin_bin, bool pin_docids, bool pin_meta,
                               Mode mode, std::string* err) {
  std::lock_guard<std::mutex> lk(mu_);

  bool any = false;
  std::string errs;

  auto pin_if = [&](const fs::path& p) {
    std::string e;
    if (!fs::exists(p)) { errs += "missing " + p.string() + "; "; return; }
    if (pin_one_unlocked_(p, mode, &e)) any = true;
    else errs += p.string() + " => " + e + "; ";
  };

  if (pin_bin)    pin_if(seg_dir / "index_native.bin");
  if (pin_docids) pin_if(seg_dir / "index_native_docids.json");
  if (pin_meta)   pin_if(seg_dir / "index_native_meta.json");

  if (!errs.empty() && err) *err = errs;
  return any;
}

bool HotCache::unpin_file(const fs::path& path) {
  std::lock_guard<std::mutex> lk(mu_);
  const std::string key = norm_key(path);
  const size_t before = files_.size();
  unpin_one_unlocked_(key);
  return files_.size() != before;
}

void HotCache::clear() {
  std::lock_guard<std::mutex> lk(mu_);
  files_.clear();
}

void HotCache::refresh_best_effort() {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<std::string> drop;
  drop.reserve(files_.size());
  for (const auto& kv : files_) {
    const auto& pf = kv.second;
    if (!pf) { drop.push_back(kv.first); continue; }
    uint64_t sz = 0;
    std::string e;
    const uint64_t mt = stat_mtime_ns(pf->path, &sz, &e);
    if (mt == 0 || sz != pf->size || mt != pf->mtime_ns) drop.push_back(kv.first);
  }
  for (const auto& k : drop) files_.erase(k);
}

std::vector<HotCache::Item> HotCache::list() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::vector<Item> out;
  out.reserve(files_.size());
  for (const auto& kv : files_) {
    const auto& pf = kv.second;
    if (!pf) continue;
    Item it;
    it.path = pf->path;
    it.size = pf->size;
    it.mtime_ns = pf->mtime_ns;
    it.mode = pf->mode;
    it.locked = pf->locked;
    out.push_back(std::move(it));
  }
  return out;
}

uint64_t HotCache::total_bytes() const {
  std::lock_guard<std::mutex> lk(mu_);
  uint64_t sum = 0;
  for (const auto& kv : files_) if (kv.second) sum += kv.second->size;
  return sum;
}
