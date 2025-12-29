#include "extractor.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cstdint>
#include <fstream>
#include <limits>
#include <iterator>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <vector>

namespace fs = std::filesystem;

static constexpr size_t kReadExtraBytes  = 16;
static constexpr size_t kPreviewBytes    = 240;
static constexpr size_t kHardMaxReadBytes = 256ull * 1024 * 1024; // 256 MiB hard safety cap

static bool utf8_is_valid(std::string_view s) {
  size_t i = 0;
  while (i < s.size()) {
    const unsigned char c = (unsigned char)s[i];

    size_t len = 1;
    if (c < 0x80) len = 1;
    else if (c >= 0xC2 && c <= 0xDF) len = 2;
    else if (c >= 0xE0 && c <= 0xEF) len = 3;
    else if (c >= 0xF0 && c <= 0xF4) len = 4;
    else return false;

    if (i + len > s.size()) return false;

    for (size_t j = 1; j < len; ++j) {
      const unsigned char cc = (unsigned char)s[i + j];
      if ((cc & 0xC0) != 0x80) return false;
    }

    if (len == 3) {
      const unsigned char c1 = (unsigned char)s[i + 1];
      if (c == 0xE0 && c1 < 0xA0) return false;
      if (c == 0xED && c1 >= 0xA0) return false;
    } else if (len == 4) {
      const unsigned char c1 = (unsigned char)s[i + 1];
      if (c == 0xF0 && c1 < 0x90) return false;
      if (c == 0xF4 && c1 > 0x8F) return false;
    }

    i += len;
  }
  return true;
}

static size_t utf8_safe_prefix_len(std::string_view s, size_t max_bytes) {
  const size_t n = std::min(max_bytes, s.size());
  size_t i = 0;
  size_t last_good = 0;

  while (i < n) {
    const unsigned char c = (unsigned char)s[i];

    size_t len = 1;
    if (c < 0x80) len = 1;
    else if (c >= 0xC2 && c <= 0xDF) len = 2;
    else if (c >= 0xE0 && c <= 0xEF) len = 3;
    else if (c >= 0xF0 && c <= 0xF4) len = 4;
    else break;

    if (i + len > n) break;

    bool ok = true;
    for (size_t j = 1; j < len; ++j) {
      const unsigned char cc = (unsigned char)s[i + j];
      if ((cc & 0xC0) != 0x80) { ok = false; break; }
    }
    if (!ok) break;

    i += len;
    last_good = i;
  }
  return last_good;
}

static inline void append_utf8(uint32_t cp, std::string& out) {
  if (cp <= 0x7F) {
    out.push_back((char)cp);
  } else if (cp <= 0x7FF) {
    out.push_back((char)(0xC0 | ((cp >> 6) & 0x1F)));
    out.push_back((char)(0x80 | (cp & 0x3F)));
  } else if (cp <= 0xFFFF) {
    out.push_back((char)(0xE0 | ((cp >> 12) & 0x0F)));
    out.push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
    out.push_back((char)(0x80 | (cp & 0x3F)));
  } else if (cp <= 0x10FFFF) {
    out.push_back((char)(0xF0 | ((cp >> 18) & 0x07)));
    out.push_back((char)(0x80 | ((cp >> 12) & 0x3F)));
    out.push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
    out.push_back((char)(0x80 | (cp & 0x3F)));
  } else {
    out.push_back('?');
  }
}

// CP1251 -> Unicode codepoint (0..0xFFFF). ASCII passthrough.
static uint16_t cp1251_to_unicode(unsigned char c) {
  if (c < 0x80) return (uint16_t)c;

  static const uint16_t tbl[128] = {
    0x0402,0x0403,0x201A,0x0453,0x201E,0x2026,0x2020,0x2021,
    0x20AC,0x2030,0x0409,0x2039,0x040A,0x040C,0x040B,0x040F,
    0x0452,0x2018,0x2019,0x201C,0x201D,0x2022,0x2013,0x2014,
    0x0000,0x2122,0x0459,0x203A,0x045A,0x045C,0x045B,0x045F,
    0x00A0,0x040E,0x045E,0x0408,0x00A4,0x0490,0x00A6,0x00A7,
    0x0401,0x00A9,0x0404,0x00AB,0x00AC,0x00AD,0x00AE,0x0407,
    0x00B0,0x00B1,0x0406,0x0456,0x0491,0x00B5,0x00B6,0x00B7,
    0x0451,0x2116,0x0454,0x00BB,0x0458,0x0405,0x0455,0x0457,
    0x0410,0x0411,0x0412,0x0413,0x0414,0x0415,0x0416,0x0417,
    0x0418,0x0419,0x041A,0x041B,0x041C,0x041D,0x041E,0x041F,
    0x0420,0x0421,0x0422,0x0423,0x0424,0x0425,0x0426,0x0427,
    0x0428,0x0429,0x042A,0x042B,0x042C,0x042D,0x042E,0x042F,
    0x0430,0x0431,0x0432,0x0433,0x0434,0x0435,0x0436,0x0437,
    0x0438,0x0439,0x043A,0x043B,0x043C,0x043D,0x043E,0x043F,
    0x0440,0x0441,0x0442,0x0443,0x0444,0x0445,0x0446,0x0447,
    0x0448,0x0449,0x044A,0x044B,0x044C,0x044D,0x044E,0x044F
  };

  const uint16_t cp = tbl[c - 0x80];
  return cp ? cp : (uint16_t)'?';
}

static std::string cp1251_to_utf8(std::string_view s) {
  std::string out;
  out.reserve(s.size() * 2);
  for (unsigned char c : s) {
    uint16_t cp = cp1251_to_unicode(c);
    append_utf8((uint32_t)cp, out);
  }
  return out;
}

static std::string safe_preview_utf8(const std::string& s, size_t max_bytes) {
  if (s.size() <= max_bytes) return s;
  const size_t cut = utf8_safe_prefix_len(s, max_bytes);
  return s.substr(0, cut);
}

static std::string read_file_prefix(const fs::path& p, size_t max_bytes) {
  std::ifstream in(p, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open file: " + p.string());

  if (max_bytes == 0) {
    // legacy full read, but protect process from OOM on huge .txt
    std::error_code ec;
    const uintmax_t fsz = fs::file_size(p, ec);
    if (!ec && fsz > (uintmax_t)kHardMaxReadBytes) {
      throw std::runtime_error(
        "file too large for full read: " + p.string() +
        " (" + std::to_string((unsigned long long)fsz) + " bytes). "
        "Pass max_bytes>0 to read a safe prefix."
      );
    }
    // Even if file_size is unavailable, enforce hard cap during read.
    std::string out;
    if (!ec && fsz <= (uintmax_t)kHardMaxReadBytes) {
      out.reserve((size_t)fsz);
    }
    const size_t chunk = 1u << 20; // 1 MiB
    std::vector<char> buf(chunk);
    while (in) {
      in.read(buf.data(), (std::streamsize)buf.size());
      const std::streamsize got = in.gcount();
      if (got <= 0) break;
      if (out.size() + (size_t)got > kHardMaxReadBytes) {
        throw std::runtime_error(
          "file too large for full read: " + p.string() +
          " (exceeds hard limit " + std::to_string((unsigned long long)kHardMaxReadBytes) + " bytes)"
        );
      }
      out.append(buf.data(), (size_t)got);
    }
    return out;
  }
  
  if (max_bytes > kHardMaxReadBytes) {
    throw std::runtime_error(
      "max_bytes too large: " + std::to_string((unsigned long long)max_bytes) +
      " (hard limit " + std::to_string((unsigned long long)kHardMaxReadBytes) + ")"
    );
  }
  if (max_bytes > std::numeric_limits<size_t>::max() - kReadExtraBytes) {
    throw std::runtime_error("max_bytes overflow");
  }
  const size_t want = max_bytes + kReadExtraBytes;
  if (want > (size_t)std::numeric_limits<std::streamsize>::max()) {
    throw std::runtime_error("max_bytes too large for stream read");
  }

  std::string buf;
  buf.resize(want);

  in.read(buf.data(), (std::streamsize)want);
  buf.resize((size_t)in.gcount());
  return buf;
}

static std::string lower_ext(const fs::path& p) {
  std::string ext = p.extension().string();
  for (auto& c : ext) c = (char)std::tolower((unsigned char)c);
  return ext;
}

ExtractedText extract_text_from_file(const fs::path& p, bool assume_normalized, size_t max_bytes) {
  const std::string ext = lower_ext(p);

  if (ext != ".txt") {
    throw std::runtime_error("unsupported file type: " + ext + " (only .txt for now)");
  }

  ExtractedText r;

  std::string raw = read_file_prefix(p, max_bytes);

  std::string text;

  if (utf8_is_valid(raw)) {
    if (max_bytes > 0 && raw.size() > max_bytes) {
      const size_t cut = utf8_safe_prefix_len(raw, max_bytes);// cut <= n
      raw.resize(cut);
    }
    text = std::move(raw);
  } else {
    // try: maybe file is UTF-8 but we cut mid-sequence. Use safe prefix <= max_bytes and recheck.
    if (max_bytes > 0) {
      const size_t n = std::min(max_bytes, raw.size());
      std::string_view bytes(raw.data(), n);

      // If the first n bytes are valid UTF-8, keep them as-is.
      if (utf8_is_valid(bytes)) {
        text.assign(bytes.data(), bytes.size());
      } else {
        // Find a UTF-8-safe boundary inside the first n bytes.
        const size_t cut = utf8_safe_prefix_len(bytes, n);

        bool trunc_fix = false;
        if (cut > 0 && cut < n && (n - cut) <= 4) {
          const unsigned char lead = (unsigned char)raw[cut];
          size_t len = 0;
          if (lead >= 0xC2 && lead <= 0xDF) len = 2;
          else if (lead >= 0xE0 && lead <= 0xEF) len = 3;
          else if (lead >= 0xF0 && lead <= 0xF4) len = 4;

          // We consider it a "truncated UTF-8 tail" only if:
          // - lead looks like a UTF-8 starter,
          // - the sequence would not fit into the first n bytes,
          // - bytes that are present after lead (within n) are all continuation bytes,
          // - and prefix before cut is valid UTF-8.
          if (len != 0 && cut + len > n) {
            bool cont_ok = true;
            for (size_t j = cut + 1; j < n; ++j) {
              const unsigned char cc = (unsigned char)raw[j];
              if ((cc & 0xC0) != 0x80) { cont_ok = false; break; }
            }
            if (cont_ok) {
              const std::string_view prefix(raw.data(), cut);
              if (utf8_is_valid(prefix)) {
                // If we have enough bytes (we read max_bytes+16), validate the full sequence.
                // If we don't (EOF truncation), still keep the valid prefix.
                if (cut + len <= raw.size()) {
                  const std::string_view seq(raw.data() + cut, len);
                  if (utf8_is_valid(seq)) {
                    trunc_fix = true;
                    text.assign(prefix.data(), prefix.size());
                  }
                } else {
                  trunc_fix = true;
                  text.assign(prefix.data(), prefix.size());
                }
              }
            }
          }
        }

        if (!trunc_fix) {
          // Fallback: treat the first n bytes as CP1251.
          text = cp1251_to_utf8(bytes);
        }
      }

    } else {
      text = cp1251_to_utf8(raw);
    }
    // enforce hard cap on output too (UTF-8 safe)
    if (max_bytes > 0 && text.size() > max_bytes) {
      const size_t cut = utf8_safe_prefix_len(text, max_bytes);
      text.resize(cut);
    }
  }

  r.text = std::move(text);
  r.text_is_normalized = assume_normalized;
  r.preview = safe_preview_utf8(r.text, kPreviewBytes);
  return r;
}
