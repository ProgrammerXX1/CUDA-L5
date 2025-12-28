#pragma once

#include <cstddef>
#include <filesystem>
#include <string>

struct ExtractedText {
  std::string text;
  bool text_is_normalized{true};
  std::string preview;
};

// Supports: .txt only (for now).
// max_bytes:
//   0 => read full file (legacy behavior)
//   >0 => read at most max_bytes (UTF-8 safe boundary)
ExtractedText extract_text_from_file(const std::filesystem::path& p,
                                    bool assume_normalized,
                                    size_t max_bytes = 0);
