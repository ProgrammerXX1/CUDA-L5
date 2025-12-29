// Back_L5/cpp/common/text_common.cpp
#include "text_common.h"

#include <algorithm>
#include <cassert>
#include <cctype>
#include <limits>
#include <stdexcept>
#include <string_view>

namespace {

struct Utf8Dec {
    uint32_t cp{0};
    size_t   len{1};
    bool     ok{false};
};

static inline bool is_cont(unsigned char c) { return (c & 0xC0) == 0x80; }

static inline Utf8Dec decode_utf8(std::string_view s, size_t i) {
    Utf8Dec r{};
    if (i >= s.size()) return r;

    const unsigned char c0 = (unsigned char)s[i];
    if (c0 < 0x80) {
        r.cp = c0; r.len = 1; r.ok = true;
        return r;
    }

    size_t len = 0;
    if (c0 >= 0xC2 && c0 <= 0xDF) len = 2;
    else if (c0 >= 0xE0 && c0 <= 0xEF) len = 3;
    else if (c0 >= 0xF0 && c0 <= 0xF4) len = 4;
    else return r;

    if (i + len > s.size()) return r;

    const unsigned char c1 = (unsigned char)s[i + 1];
    if (!is_cont(c1)) return r;

    if (len == 2) {
        uint32_t cp = ((uint32_t)(c0 & 0x1F) << 6) | (uint32_t)(c1 & 0x3F);
        r.cp = cp; r.len = 2; r.ok = true;
        return r;
    }

    const unsigned char c2 = (unsigned char)s[i + 2];
    if (!is_cont(c2)) return r;

    if (len == 3) {
        // overlong / surrogate checks
        if (c0 == 0xE0 && c1 < 0xA0) return r;
        if (c0 == 0xED && c1 >= 0xA0) return r;
        uint32_t cp = ((uint32_t)(c0 & 0x0F) << 12)
                    | ((uint32_t)(c1 & 0x3F) << 6)
                    |  (uint32_t)(c2 & 0x3F);
        r.cp = cp; r.len = 3; r.ok = true;
        return r;
    }

    const unsigned char c3 = (unsigned char)s[i + 3];
    if (!is_cont(c3)) return r;

    if (c0 == 0xF0 && c1 < 0x90) return r;
    if (c0 == 0xF4 && c1 > 0x8F) return r;

    uint32_t cp = ((uint32_t)(c0 & 0x07) << 18)
                | ((uint32_t)(c1 & 0x3F) << 12)
                | ((uint32_t)(c2 & 0x3F) << 6)
                |  (uint32_t)(c3 & 0x3F);
    if (cp > 0x10FFFF) return r;

    r.cp = cp; r.len = 4; r.ok = true;
    return r;
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
    } else {
        out.push_back((char)(0xF0 | ((cp >> 18) & 0x07)));
        out.push_back((char)(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back((char)(0x80 | (cp & 0x3F)));
    }
}

static inline bool is_ascii_alnum_lower(unsigned char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z');
}

static inline bool is_space_cp(uint32_t cp) {
    if (cp == 0x20) return true;
    if (cp == '\t' || cp == '\n' || cp == '\r' || cp == '\f' || cp == '\v') return true;
    if (cp == 0x00A0) return true; // NBSP
    return false;
}

static inline bool is_cyrillicish(uint32_t cp) {
    // Cyrillic + Supplement + Extended-A
    return (cp >= 0x0400 && cp <= 0x052F);
}

static inline bool is_turkish_latin_lower(uint32_t cp) {
    // ç ğ ı ö ş ü
    return (cp == 0x00E7)  // ç
        || (cp == 0x011F)  // ğ
        || (cp == 0x0131)  // ı (dotless i)
        || (cp == 0x00F6)  // ö
        || (cp == 0x015F)  // ş
        || (cp == 0x00FC); // ü
}

static inline bool is_arabic_diacritic_or_tatweel(uint32_t cp) {
    // Tatweel
    if (cp == 0x0640) return true;
    // Arabic combining marks (common ranges)
    if (cp >= 0x0610 && cp <= 0x061A) return true;
    if (cp >= 0x064B && cp <= 0x065F) return true;
    if (cp == 0x0670) return true;
    if (cp >= 0x06D6 && cp <= 0x06DC) return true;
    if (cp >= 0x06DF && cp <= 0x06E4) return true;
    if (cp >= 0x06E7 && cp <= 0x06E8) return true;
    if (cp >= 0x06EA && cp <= 0x06ED) return true;
    return false;
}

static inline uint32_t arabic_digit_to_ascii(uint32_t cp) {
    // Arabic-Indic digits: ٠١٢٣٤٥٦٧٨٩
    if (cp >= 0x0660 && cp <= 0x0669) return (uint32_t)('0' + (cp - 0x0660));
    // Eastern Arabic-Indic digits: ۰۱۲۳۴۵۶۷۸۹
    if (cp >= 0x06F0 && cp <= 0x06F9) return (uint32_t)('0' + (cp - 0x06F0));
    return cp;
}

static inline bool is_arabic_letter(uint32_t cp) {
    // Minimal “keep” ranges for Arabic letters (not full Unicode categories).
    // Core letters:
    if (cp >= 0x0621 && cp <= 0x063A) return true;
    if (cp >= 0x0641 && cp <= 0x064A) return true;
    // Additional letters:
    if (cp >= 0x066E && cp <= 0x066F) return true;
    if (cp >= 0x0671 && cp <= 0x06D3) return true;
    if (cp == 0x06D5) return true;
    if (cp >= 0x06EE && cp <= 0x06EF) return true;
    if (cp >= 0x06FA && cp <= 0x06FC) return true;
    // Arabic Supplement / Extended-A (rough keep; may include some marks, but diacritics are filtered earlier)
    if (cp >= 0x0750 && cp <= 0x077F) return true;
    if (cp >= 0x08A0 && cp <= 0x08FF) return true;
    // Presentation forms (OCR/legacy sometimes emits these)
    if (cp >= 0xFB50 && cp <= 0xFDFF) return true;
    if (cp >= 0xFE70 && cp <= 0xFEFF) return true;
    return false;
}

static inline uint32_t to_lower_ru_kz(uint32_t cp) {
    // ASCII
    if (cp >= 'A' && cp <= 'Z') return cp - 'A' + 'a';

    // Cyrillic А..Я -> а..я
    if (cp >= 0x0410 && cp <= 0x042F) return cp + 0x20;
    // Ё
    if (cp == 0x0401) return 0x0451;
    // І
    if (cp == 0x0406) return 0x0456;

    // Kazakh uppercase -> lowercase
    if (cp == 0x04D8) return 0x04D9; // Ә
    if (cp == 0x0492) return 0x0493; // Ғ
    if (cp == 0x049A) return 0x049B; // Қ
    if (cp == 0x04A2) return 0x04A3; // Ң
    if (cp == 0x04E8) return 0x04E9; // Ө
    if (cp == 0x04B0) return 0x04B1; // Ұ
    if (cp == 0x04AE) return 0x04AF; // Ү
    if (cp == 0x04BA) return 0x04BB; // Һ

    // Turkish uppercase -> lowercase (minimal)
    if (cp == 0x00C7) return 0x00E7; // Ç -> ç
    if (cp == 0x011E) return 0x011F; // Ğ -> ğ
    if (cp == 0x0130) return 0x0069; // İ -> i
    if (cp == 0x00D6) return 0x00F6; // Ö -> ö
    if (cp == 0x015E) return 0x015F; // Ş -> ş
    if (cp == 0x00DC) return 0x00FC; // Ü -> ü

    return cp;
}

} // namespace

void normalize_for_shingles_simple_to(std::string_view s, std::string& out) {
    out.clear();
    out.reserve(s.size());

    bool prev_space = true;

    for (size_t i = 0; i < s.size();) {
        const unsigned char b = (unsigned char)s[i];

        // ASCII fast path
        if (b < 0x80) {
            unsigned char c = (unsigned char)s[i];
            if (c >= 'A' && c <= 'Z') c = (unsigned char)(c - 'A' + 'a');

            if (is_ascii_alnum_lower(c)) {
                out.push_back((char)c);
                prev_space = false;
            } else if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v') {
                if (!prev_space) {
                    out.push_back(' ');
                    prev_space = true;
                }
            } else {
                if (!prev_space) {
                    out.push_back(' ');
                    prev_space = true;
                }
            }
            ++i;
            continue;
        }

        // UTF-8 decode
        Utf8Dec d = decode_utf8(s, i);
        if (!d.ok) {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
            ++i;
            continue;
        }

        uint32_t cp = to_lower_ru_kz(d.cp);
        cp = arabic_digit_to_ascii(cp);

        // Arabic diacritics/tatweel: ignore (do not break tokens with spaces)
        if (is_arabic_diacritic_or_tatweel(cp)) {
            i += d.len;
            continue;
        }
        bool keep = false;
        if (cp <= 0x7F) {
            keep = is_ascii_alnum_lower((unsigned char)cp);
        } else {
            keep = is_cyrillicish(cp) || is_turkish_latin_lower(cp) || is_arabic_letter(cp);
        }

        if (keep) {
            append_utf8(cp, out);
            prev_space = false;
        } else if (is_space_cp(cp)) {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
        } else {
            if (!prev_space) {
                out.push_back(' ');
                prev_space = true;
            }
        }

        i += d.len;
    }

    if (!out.empty() && out.back() == ' ') out.pop_back();
}

std::string normalize_for_shingles_simple(std::string_view s) {
    std::string out;
    normalize_for_shingles_simple_to(s, out);
    return out;
}

void tokenize_spans(const std::string& s, std::vector<TokenSpan>& out) {
    out.clear();
    const size_t n = s.size();
    size_t i = 0;

    auto is_ws_at = [&](size_t idx, size_t& step) -> bool {
        const unsigned char c = (unsigned char)s[idx];
        // ASCII whitespace
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v') {
            step = 1;
            return true;
        }
        // UTF-8 NBSP: U+00A0 => 0xC2 0xA0
        if (c == 0xC2 && idx + 1 < n && (unsigned char)s[idx + 1] == 0xA0) {
            step = 2;
            return true;
        }
        step = 1;
        return false;
    };

    while (i < n) {
        size_t step = 1;
        while (i < n && is_ws_at(i, step)) i += step;
        if (i >= n) break;

        const size_t start = i;
        while (i < n) {
            size_t st2 = 1;
            if (is_ws_at(i, st2)) break;
            ++i;
        }
        const size_t len = i - start;

        if (len > 0) {
            const uint32_t u32max = std::numeric_limits<uint32_t>::max();
            if (start > (size_t)u32max || len > (size_t)u32max) {
                throw std::runtime_error("token span overflow (string too large)");
            }
            TokenSpan ts;
            ts.start = (uint32_t)start;
            ts.len = (uint32_t)len;
            out.push_back(ts);
        }
    }
}

// FNV-1a 64-bit
static inline uint64_t fnv1a64_init() { return 1469598103934665603ULL; }
static inline uint64_t fnv1a64_mix(uint64_t h, unsigned char c) {
    h ^= (uint64_t)c;
    h *= 1099511628211ULL;
    return h;
}

static inline uint64_t hash_token_bytes_internal(const std::string& s, const TokenSpan& t) {
    uint64_t h = fnv1a64_init();
    const uint32_t a = t.start;
    const uint32_t b = t.start + t.len;
    for (uint32_t i = a; i < b; ++i) h = fnv1a64_mix(h, (unsigned char)s[i]);
    return h;
}

uint64_t hash_shingle_tokens_spans(const std::string& s,
                                  const std::vector<TokenSpan>& spans,
                                  int pos,
                                  int K) {
    assert(pos >= 0);
    assert(K > 0);
    assert((size_t)pos <= spans.size());
    assert((size_t)(pos + K) <= spans.size());
    uint64_t h = 0x9E3779B97F4A7C15ULL;
    const int end = pos + K;
    for (int i = pos; i < end; ++i) {
        uint64_t th = hash_token_bytes_internal(s, spans[(size_t)i]);
        h ^= th + 0x9E3779B97F4A7C15ULL + (h << 6) + (h >> 2);
    }
    return h;
}

std::pair<uint64_t, uint64_t> simhash128_spans(const std::string& s,
                                               const std::vector<TokenSpan>& spans) {
    int v0[64] = {0};
    int v1[64] = {0};

    for (const auto& sp : spans) {
        uint64_t th = hash_token_bytes_internal(s, sp);
        uint64_t a = th;
        uint64_t b = th ^ 0xD6E8FEB86659FD93ULL;

        for (int i = 0; i < 64; ++i) v0[i] += ((a >> i) & 1ULL) ? 1 : -1;
        for (int i = 0; i < 64; ++i) v1[i] += ((b >> i) & 1ULL) ? 1 : -1;
    }

    uint64_t hi = 0, lo = 0;
    for (int i = 0; i < 64; ++i) if (v0[i] > 0) hi |= (1ULL << i);
    for (int i = 0; i < 64; ++i) if (v1[i] > 0) lo |= (1ULL << i);
    return {hi, lo};
}

// ускорение
void hash_tokens_bytes_spans(const std::string& s,
                             const std::vector<TokenSpan>& spans,
                             std::vector<uint64_t>& out_hashes) {
    out_hashes.resize(spans.size());
    for (size_t i = 0; i < spans.size(); ++i) {
        out_hashes[i] = hash_token_bytes_internal(s, spans[i]);
    }
}

uint64_t hash_shingle_token_hashes(const std::vector<uint64_t>& token_hashes,
                                   int pos,
                                   int K) {
    assert(pos >= 0);
    assert(K > 0);
    assert((size_t)pos <= token_hashes.size());
    assert((size_t)(pos + K) <= token_hashes.size());
    uint64_t h = 0x9E3779B97F4A7C15ULL;
    const int end = pos + K;
    for (int i = pos; i < end; ++i) {
        const uint64_t th = token_hashes[(size_t)i];
        h ^= th + 0x9E3779B97F4A7C15ULL + (h << 6) + (h >> 2);
    }
    return h;
}

std::pair<uint64_t, uint64_t> simhash128_token_hashes(const std::vector<uint64_t>& token_hashes) {
    int v0[64] = {0};
    int v1[64] = {0};

    for (uint64_t th : token_hashes) {
        uint64_t a = th;
        uint64_t b = th ^ 0xD6E8FEB86659FD93ULL;

        for (int i = 0; i < 64; ++i) v0[i] += ((a >> i) & 1ULL) ? 1 : -1;
        for (int i = 0; i < 64; ++i) v1[i] += ((b >> i) & 1ULL) ? 1 : -1;
    }

    uint64_t hi = 0, lo = 0;
    for (int i = 0; i < 64; ++i) if (v0[i] > 0) hi |= (1ULL << i);
    for (int i = 0; i < 64; ++i) if (v1[i] > 0) lo |= (1ULL << i);
    return {hi, lo};
}
