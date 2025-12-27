// src/tombstone.h
#pragma once
#include <string>
#include <unordered_set>
#include <filesystem>

class Tombstones {
public:
  explicit Tombstones(std::filesystem::path file);
  void load();
  void append(const std::string& key); // doc_id
  bool contains(const std::string& key) const;

private:
  std::filesystem::path file_;
  std::unordered_set<std::string> set_;
};
