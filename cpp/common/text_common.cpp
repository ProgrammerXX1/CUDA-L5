#include "text_common.h"

#include <algorithm>
#include <cctype>

// ─────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────

static inline bool is_ascii_alnum(unsigned char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z');
}

static inline unsigned char to_ascii_lower(unsigned char c) {
    if (c >= 'A' && c <= 'Z') return (unsigned char)(c - 'A' + 'a');
    return c;
}

// Очень упрощённо для UTF-8: мы не декодируем кириллицу полноценно.
// Но для русских/казахских текстов в UTF-8 это не идеально.
// Если тебе нужно качество 10/10 — позже заменим на твой robust UTF-8 decode.
// Сейчас задача: сборка/контракт/пайплайн.
static inline bool is_utf8_cont(unsigned char c) { return (c & 0xC0) == 0x80; }

// Пропускаем байты UTF-8 как “буквенные” в диапазонах, грубо оставляя последовательности.
// Для стабильности: любые байты >= 0xC2 считаем частью “слова”.
static inline bool is_utf8_letterish(unsigned char c) {
    return c >= 0xC2; // начало многобайтного символа
}

std::string normalize_for_shingles_simple(std::string_view s) {
    std::string out;
    out.reserve(s.size());

    bool prev_space = true;
    for (size_t i = 0; i < s.size(); ++i) {
        unsigned char c = (unsigned char)s[i];

        // ASCII
        if (c < 0x80) {
            c = to_ascii_lower(c);
            if (is_ascii_alnum(c)) {
                out.push_back((char)c);
                prev_space = false;
            } else if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                if (!prev_space) {
                    out.push_back(' ');
                    prev_space = true;
                }
            } else {
                // punctuation -> space
                if (!prev_space) {
                    out.push_back(' ');
                    prev_space = true;
                }
            }
            continue;
        }

        // UTF-8: сохраняем последовательности как "буквы" (очень грубо)
        if (is_utf8_letterish(c)) {
            // копируем текущий байт и последующие continuation bytes
            out.push_back((char)c);
            prev_space = false;

            // копируем continuation bytes
            size_t j = i + 1;
            while (j < s.size() && is_utf8_cont((unsigned char)s[j])) {
                out.push_back(s[j]);
                ++j;
            }
            i = j - 1;
            continue;
        }

        // иначе -> space
        if (!prev_space) {
            out.push_back(' ');
            prev_space = true;
        }
    }

    // trim trailing space
    if (!out.empty() && out.back() == ' ') out.pop_back();
    return out;
}

void tokenize_spans(const std::string& s, std::vector<TokenSpan>& out) {
    out.clear();
    const size_t n = s.size();
    size_t i = 0;

    while (i < n) {
        while (i < n && s[i] == ' ') ++i;
        if (i >= n) break;

        const size_t start = i;
        while (i < n && s[i] != ' ') ++i;
        const size_t len = i - start;

        if (len > 0) {
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

static uint64_t hash_token_bytes(const std::string& s, const TokenSpan& t) {
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
    // Хэшируем последовательность K токенов: mix( token_hash[i] )
    uint64_t h = 0x9E3779B97F4A7C15ULL; // seed
    const int end = pos + K;
    for (int i = pos; i < end; ++i) {
        uint64_t th = hash_token_bytes(s, spans[(size_t)i]);
        // combine
        h ^= th + 0x9E3779B97F4A7C15ULL + (h << 6) + (h >> 2);
    }
    return h;
}

std::pair<uint64_t, uint64_t> simhash128_spans(const std::string& s,
                                               const std::vector<TokenSpan>& spans) {
    // Простой simhash: берём token-hash, голосуем по битам
    // 128 = 2x64
    int v0[64] = {0};
    int v1[64] = {0};

    for (const auto& sp : spans) {
        uint64_t th = hash_token_bytes(s, sp);
        // используем два разных "потока" через смешивание
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
