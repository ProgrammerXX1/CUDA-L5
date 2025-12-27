// src/extractor.h
#pragma once

#include <filesystem>
#include <string>

struct ExtractedText {
  std::string text;
  bool text_is_normalized{true};
  std::string preview;
};

// Currently supports: .txt only.
ExtractedText extract_text_from_file(const std::filesystem::path& p, bool assume_normalized);
