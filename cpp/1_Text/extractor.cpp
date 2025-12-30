// extractor.cpp (UTF-8 only, .txt only)
#include "extractor.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace fs = std::filesystem;

namespace {

// extractor = read + UTF-8 guard only (no text normalization here)
static constexpr size_t kPreviewBytes     = 240;
static constexpr size_t kHardMaxReadBytes = 256ull * 1024 * 1024; // 256 MiB

struct Utf8Check {
  bool   ok{false};
  bool   incomplete_tail{false}; // only possible at the very end of the prefix
  size_t last_good{0};           // end offset of last complete codepoint
  size_t error_pos{0};           // byte offset where parsing failed (or start of incomplete tail)
};

static inline bool is_cont(unsigned char c) { return (c & 0xC0) == 0x80; }

// Validates a prefix; distinguishes "incomplete tail at end" from true invalid sequences.
static Utf8Check utf8_check_prefix(std::string_view s) {
  size_t i = 0;
  while (i < s.size()) {
    const unsigned char c0 = (unsigned char)s[i];

    size_t len = 1;
    if (c0 < 0x80) len = 1;
    else if (c0 >= 0xC2 && c0 <= 0xDF) len = 2;
    else if (c0 >= 0xE0 && c0 <= 0xEF) len = 3;
    else if (c0 >= 0xF0 && c0 <= 0xF4) len = 4;
    else return Utf8Check{false, false, i, i};

    if (i + len > s.size()) {
      // We ended inside a codepoint at the end of the prefix.
      return Utf8Check{false, true, i, i};
    }

    if (len == 1) {
      ++i;
      continue;
    }

    const unsigned char c1 = (unsigned char)s[i + 1];
    if (!is_cont(c1)) return Utf8Check{false, false, i, i};

    if (len == 2) {
      i += 2;
      continue;
    }

    const unsigned char c2 = (unsigned char)s[i + 2];
    if (!is_cont(c2)) return Utf8Check{false, false, i, i};

    if (len == 3) {
      // overlong / surrogate checks
      if (c0 == 0xE0 && c1 < 0xA0) return Utf8Check{false, false, i, i};
      if (c0 == 0xED && c1 >= 0xA0) return Utf8Check{false, false, i, i};
      i += 3;
      continue;
    }

    const unsigned char c3 = (unsigned char)s[i + 3];
    if (!is_cont(c3)) return Utf8Check{false, false, i, i};

    // 4-byte strict checks
    if (c0 == 0xF0 && c1 < 0x90) return Utf8Check{false, false, i, i};
    if (c0 == 0xF4 && c1 > 0x8F) return Utf8Check{false, false, i, i};

    const uint32_t cp =
        ((uint32_t)(c0 & 0x07) << 18) |
        ((uint32_t)(c1 & 0x3F) << 12) |
        ((uint32_t)(c2 & 0x3F) << 6)  |
        (uint32_t)(c3 & 0x3F);
    if (cp > 0x10FFFF) return Utf8Check{false, false, i, i};

    i += 4;
  }

  return Utf8Check{true, false, s.size(), s.size()};
}

static size_t utf8_safe_prefix_len(std::string_view s, size_t max_bytes) {
  const size_t n = std::min(max_bytes, s.size());
  const Utf8Check chk = utf8_check_prefix(std::string_view(s.data(), n));
  if (chk.ok) return n;
  if (chk.incomplete_tail) return chk.last_good;
  return 0;
}

static std::string safe_preview_utf8(const std::string& s, size_t max_bytes) {
  if (s.size() <= max_bytes) return s;
  const size_t cut = utf8_safe_prefix_len(s, max_bytes);
  return s.substr(0, cut);
}

static std::string lower_ext(const fs::path& p) {
  std::string ext = p.extension().string();
  for (char& c : ext) c = (char)std::tolower((unsigned char)c);
  return ext;
}

static inline void strip_utf8_bom(std::string& s) {
  if (s.size() >= 3 &&
      (unsigned char)s[0] == 0xEF &&
      (unsigned char)s[1] == 0xBB &&
      (unsigned char)s[2] == 0xBF) {
    s.erase(0, 3);
  }
}

struct ReadPrefixResult {
  std::string bytes;
  bool truncated{false}; // true if file has more bytes after this prefix
};

static std::string read_entire_file_limited(const fs::path& p) {
  std::ifstream in(p, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open file: " + p.string());

  std::error_code ec;
  const uintmax_t fsz = fs::file_size(p, ec);
  if (!ec && fsz > (uintmax_t)kHardMaxReadBytes) {
    throw std::runtime_error(
        "file too large: " + p.string() +
        " (" + std::to_string((unsigned long long)fsz) +
        " bytes, hard limit " + std::to_string((unsigned long long)kHardMaxReadBytes) + ")");
  }

  std::string out;
  const size_t chunk = 1u << 20; // 1 MiB
  if (!ec) out.reserve((size_t)std::min<uintmax_t>(fsz, (uintmax_t)kHardMaxReadBytes));
  else out.reserve(chunk);

  std::vector<char> buf(chunk);
  while (in) {
    in.read(buf.data(), (std::streamsize)buf.size());
    const std::streamsize got = in.gcount();
    if (got <= 0) break;

    if (out.size() + (size_t)got > kHardMaxReadBytes) {
      throw std::runtime_error("file too large (exceeds hard limit): " + p.string());
    }

    const size_t need = out.size() + (size_t)got;
    if (out.capacity() < need) out.reserve(need); // avoid huge geometric over-alloc
    out.append(buf.data(), (size_t)got);
  }

  return out;
}

static ReadPrefixResult read_file_prefix_limited(const fs::path& p, size_t max_bytes) {
  if (max_bytes == 0) {
    return ReadPrefixResult{read_entire_file_limited(p), false};
  }
  if (max_bytes > kHardMaxReadBytes) {
    throw std::runtime_error(
        "max_bytes too large: " + std::to_string((unsigned long long)max_bytes) +
        " (hard limit " + std::to_string((unsigned long long)kHardMaxReadBytes) + ")");
  }
  if (max_bytes > (size_t)std::numeric_limits<std::streamsize>::max()) {
    throw std::runtime_error("max_bytes too large for stream read");
  }

  std::ifstream in(p, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open file: " + p.string());

  bool truncated = false;
  size_t to_read = max_bytes;

  std::error_code ec;
  const uintmax_t fsz = fs::file_size(p, ec);
  if (!ec) {
    truncated = (fsz > (uintmax_t)max_bytes);
    if (fsz < (uintmax_t)to_read) to_read = (size_t)fsz;
  }

  std::string buf;
  buf.resize(to_read);
  in.read(buf.data(), (std::streamsize)to_read);
  buf.resize((size_t)in.gcount());

  if (ec) {
    // file_size unavailable: detect truncation cheaply
    if (buf.size() == max_bytes) {
      const int next = in.peek();
      if (next != std::char_traits<char>::eof()) truncated = true;
    }
  }

  return ReadPrefixResult{std::move(buf), truncated};
}

[[noreturn]] static void throw_invalid_utf8(const fs::path& p, size_t pos) {
  throw std::runtime_error("invalid UTF-8 in file: " + p.string() +
                           " at byte " + std::to_string((unsigned long long)pos));
}

} // namespace

ExtractedText extract_text_from_file(const fs::path& p, bool assume_normalized, size_t max_bytes) {
  const std::string ext = lower_ext(p);
  if (ext != ".txt") {
    throw std::runtime_error("unsupported file type: " + ext + " (only .txt for now)");
  }

  ReadPrefixResult rr = read_file_prefix_limited(p, max_bytes);
  std::string text = std::move(rr.bytes);

  if (max_bytes == 0) {
    const Utf8Check chk = utf8_check_prefix(text);
    if (!chk.ok) throw_invalid_utf8(p, chk.error_pos);
  } else {
    if (text.size() > max_bytes) text.resize(max_bytes);

    const Utf8Check chk = utf8_check_prefix(text);
    if (chk.ok) {
      // ok
    } else if (chk.incomplete_tail && rr.truncated) {
      // We cut inside a multi-byte UTF-8 codepoint at the end of the prefix.
      text.resize(chk.last_good);
    } else {
      throw_invalid_utf8(p, chk.error_pos);
    }
  }

  // Remove UTF-8 BOM if present (decoding stage, not normalization).
  strip_utf8_bom(text);

  ExtractedText out;
  out.text = std::move(text);
  out.text_is_normalized = assume_normalized;
  out.preview = safe_preview_utf8(out.text, kPreviewBytes);
  return out;
}
