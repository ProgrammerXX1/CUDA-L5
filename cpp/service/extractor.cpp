// src/extractor.cpp
#include "extractor.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string_view>
#include <vector>

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

    // continuation bytes
    for (size_t j = 1; j < len; ++j) {
      const unsigned char cc = (unsigned char)s[i + j];
      if ((cc & 0xC0) != 0x80) return false;
    }

    // minimal extra checks (overlong/surrogate/out-of-range) for better validity
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

  // table for 0x80..0xFF
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

static std::string safe_preview_utf8(const std::string& s, size_t max_bytes) {
  if (s.size() <= max_bytes) return s;
  const size_t cut = utf8_safe_prefix_len(s, max_bytes);
  return s.substr(0, cut);
}

static std::string read_all(const std::filesystem::path& p) {
  std::ifstream in(p, std::ios::binary);
  if (!in) throw std::runtime_error("cannot open file: " + p.string());
  return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

static std::string lower_ext(const std::filesystem::path& p) {
  std::string ext = p.extension().string();
  for (auto& c : ext) c = (char)std::tolower((unsigned char)c);
  return ext;
}

ExtractedText extract_text_from_file(const std::filesystem::path& p, bool assume_normalized) {
  const std::string ext = lower_ext(p);

  if (ext == ".txt") {
    ExtractedText r;
    std::string raw = read_all(p);

    // Нормализуем кодировку до UTF-8:
    // - если уже валидный UTF-8 -> оставить
    // - иначе пробуем CP1251 -> UTF-8 (частый кейс для RU/KZ .txt)
    if (utf8_is_valid(raw)) {
      r.text = std::move(raw);
    } else {
      r.text = cp1251_to_utf8(raw);
    }

    r.text_is_normalized = assume_normalized;
    r.preview = safe_preview_utf8(r.text, 240);
    return r;
  }

  throw std::runtime_error("unsupported file type: " + ext + " (only .txt for now)");
}
