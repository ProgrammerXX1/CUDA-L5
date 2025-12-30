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
// Input is expected to be valid UTF-8. If invalid UTF-8 is detected, throws runtime_error.
// max_bytes:
//   0  => read full file (legacy behavior; guarded by hard cap to prevent OOM)
//   >0 => read at most max_bytes bytes and trim to a UTF-8-safe boundary (<= max_bytes)
 
ExtractedText extract_text_from_file(const std::filesystem::path& p,
                                    bool assume_normalized,
                                    size_t max_bytes = 0);
