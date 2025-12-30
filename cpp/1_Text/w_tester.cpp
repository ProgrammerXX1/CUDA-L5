// text_io_smoke.cpp
#include "extractor.h"
#include "text_common.h"

#include <cassert>
#include <string>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string_view>
#include <vector>

namespace fs = std::filesystem;

static std::string u8s(std::u8string_view sv) {
  return std::string(reinterpret_cast<const char*>(sv.data()), sv.size());
}

static void write_bytes(const fs::path& p, const std::vector<unsigned char>& b) {
  std::ofstream out(p, std::ios::binary);
  assert(out);
  out.write(reinterpret_cast<const char*>(b.data()), (std::streamsize)b.size());
}

static void write_text(const fs::path& p, const std::string& s) {
  std::ofstream out(p, std::ios::binary);
  assert(out);
  out.write(s.data(), (std::streamsize)s.size());
}

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

int main() {
  fs::path dir = "tmp_text_io_smoke";
  fs::create_directories(dir);

  // 1) UTF-8 валидный
  fs::path p_utf8 = dir / "utf8.txt";
  const std::string s_utf8 = u8s(u8"ABC Привет 🌍");
  write_text(p_utf8, s_utf8);
  {
    auto r = extract_text_from_file(p_utf8, false, 0);
    assert(r.text == s_utf8);
    assert(utf8_is_valid(r.text));
  }

  // 2) UTF-8: резка посреди 2-байт символа
  fs::path p_cut = dir / "cut_utf8.txt";
  const std::string s_cut = u8s(u8"aЖb");
  write_text(p_cut, s_cut);
  {
    auto r = extract_text_from_file(p_cut, false, 2); // "a" + 1 байт от 'Ж'
    assert(utf8_is_valid(r.text));
    assert(r.text == "a");
  }

  // 3) UTF-8: обрезанный хвост в конце файла (EOF truncation)
  fs::path p_broken = dir / "broken_utf8.txt";
  // "п" = D0 BF, "р" = D1 80 -> оставим D1 без 80
  write_bytes(p_broken, {0xD0, 0xBF, 0xD1});
  {
    auto r = extract_text_from_file(p_broken, false, 0);
    assert(utf8_is_valid(r.text));
    assert(r.text == std::string("\xD0\xBF", 2)); // "п"
  }

  // 4) CP1251 -> UTF-8
  fs::path p_cp = dir / "cp1251.txt";
  // "Привет" в CP1251: CF F0 E8 E2 E5 F2
  write_bytes(p_cp, {0xCF, 0xF0, 0xE8, 0xE2, 0xE5, 0xF2});
  {
    auto r = extract_text_from_file(p_cp, false, 0);
    assert(utf8_is_valid(r.text));
    assert(r.text == u8s(u8"Привет"));
  }

  // 5) Мусорные байты: не падать, вернуть валидный UTF-8 (после fallback/обрезки)
  fs::path p_g = dir / "garbage.txt";
  write_bytes(p_g, {0xFF, 0xFE, 0x80, 0xC0, 0xC1, 0xF5, 0x00, 0x41});
  {
    auto r = extract_text_from_file(p_g, false, 64);
    assert(utf8_is_valid(r.text)); // после fallback должен быть валидный UTF-8
    assert(r.text.size() <= 64);
  }

  // 6) text_common: нормализация + токены + simhash
  {
    const std::string mixed = u8s(u8"Hello, WORLD! ПрИвЕт ӘҒҚҢӨҰҮҺ ÇĞİÖŞÜ ١٢٣ ۴۵۶ ـَ");
    std::string norm = normalize_for_shingles_simple(mixed);
    assert(!norm.empty());
    std::vector<TokenSpan> spans;
    tokenize_spans(norm, spans);
    std::vector<uint64_t> th;
    hash_tokens_bytes_spans(norm, spans, th);
    auto sh = simhash128_token_hashes(th);
    (void)sh;
  }

  std::cout << "OK\n";
  return 0;
}
