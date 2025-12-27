// Back_L5/cpp/common/text_common.h
#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

struct TokenSpan {
    uint32_t start{0};
    uint32_t len{0};
};

// Нормализация (RU/KZ-friendly, UTF-8 decode):
// - ASCII: lower, оставить [a-z0-9]
// - Кириллица (в т.ч. казахские буквы): lower для основных букв, оставить как "буквы"
// - Всё остальное -> пробел
// - Сжимаем пробелы
std::string normalize_for_shingles_simple(std::string_view s);

// То же самое, но пишет в out (reuse capacity, без лишних аллокаций)
void normalize_for_shingles_simple_to(std::string_view s, std::string& out);

// Токенизация по пробелам
void tokenize_spans(const std::string& s, std::vector<TokenSpan>& out);

// Hash K токенов начиная с позиции pos (token spans)
uint64_t hash_shingle_tokens_spans(const std::string& s,
                                  const std::vector<TokenSpan>& spans,
                                  int pos,
                                  int K);

// Simhash 128 по токенам (token spans)
std::pair<uint64_t, uint64_t> simhash128_spans(const std::string& s,
                                               const std::vector<TokenSpan>& spans);

// --------------------
// ускорение (чтобы не хэшировать токен K раз на каждый шингл)
// --------------------

void hash_tokens_bytes_spans(const std::string& s,
                             const std::vector<TokenSpan>& spans,
                             std::vector<uint64_t>& out_hashes);

uint64_t hash_shingle_token_hashes(const std::vector<uint64_t>& token_hashes,
                                   int pos,
                                   int K);

std::pair<uint64_t, uint64_t> simhash128_token_hashes(const std::vector<uint64_t>& token_hashes);
