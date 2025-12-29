// Back_L5/cpp/include/l5/query.h
#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace l5 {

struct QueryHash {
    uint64_t h{0};
    std::vector<uint32_t> qpos; // позиции шингла в запросе
};

struct QueryShingles {
    std::vector<QueryHash> items; // уникальные хэши
    uint32_t total_shingles{0};   // общее число шинглов (с повторами)
};

QueryShingles build_query_shingles(const std::string& query_text, bool text_is_normalized);

} // namespace l5
