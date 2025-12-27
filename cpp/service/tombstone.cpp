// src/tombstone.cpp
#include "tombstone.h"
#include <fstream>

Tombstones::Tombstones(std::filesystem::path file) : file_(std::move(file)) {}

void Tombstones::load() {
  set_.clear();
  std::ifstream in(file_);
  if (!in) return;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty()) set_.insert(line);
  }
}

void Tombstones::append(const std::string& key) {
  std::filesystem::create_directories(file_.parent_path());
  std::ofstream out(file_, std::ios::app | std::ios::binary);
  out << key << "\n";
  set_.insert(key);
}

bool Tombstones::contains(const std::string& key) const {
  return set_.find(key) != set_.end();
}

