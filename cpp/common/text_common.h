#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// Минимальный набор под L5.
// Важно: TokenSpan использует offsets в строке norm.
// tokenize_spans() даёт токены, разделённые пробелами.

struct TokenSpan {
    uint32_t start{0};
    uint32_t len{0};
};

// Простая нормализация: lower + оставить [a-z0-9а-яё] и пробелы.
// Всё остальное -> пробел. Сжимаем пробелы.
std::string normalize_for_shingles_simple(std::string_view s);

// Токенизация по пробелам: каждый токен = непрерывная последовательность не-пробелов
void tokenize_spans(const std::string& s, std::vector<TokenSpan>& out);

// Хэш K токенов начиная с позиции pos (pos = индекс токена, не символа)
uint64_t hash_shingle_tokens_spans(const std::string& s,
                                  const std::vector<TokenSpan>& spans,
                                  int pos,
                                  int K);

// Simhash 128 (2x64) по токенам; используется только как meta
std::pair<uint64_t, uint64_t> simhash128_spans(const std::string& s,
                                               const std::vector<TokenSpan>& spans);
