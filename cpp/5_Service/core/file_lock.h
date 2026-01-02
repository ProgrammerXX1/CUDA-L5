// NEW FILE: Back_Last/cpp/5_Service/core/file_lock.h
#pragma once

#include <filesystem>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

class FileLock {
public:
  enum class Mode { Shared, Exclusive };

  FileLock(const std::filesystem::path& path, Mode mode) {
    std::error_code ec;
    if (!path.parent_path().empty()) std::filesystem::create_directories(path.parent_path(), ec);

    fd_ = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd_ < 0) throw std::runtime_error("open lock failed: " + path.string());

    const int op = (mode == Mode::Shared) ? LOCK_SH : LOCK_EX;
    if (::flock(fd_, op) != 0) {
      ::close(fd_);
      fd_ = -1;
      throw std::runtime_error("flock failed: " + path.string());
    }
  }

  ~FileLock() {
    if (fd_ >= 0) {
      ::flock(fd_, LOCK_UN);
      ::close(fd_);
      fd_ = -1;
    }
  }

  FileLock(const FileLock&) = delete;
  FileLock& operator=(const FileLock&) = delete;

private:
  int fd_{-1};
};
