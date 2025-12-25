#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace l5 {

// Для каждого hash храним позиции в query (pos_query).
struct QueryHash {
    uint64_t h{0};
    std::vector<uint32_t> qpos; // все позиции шингла в query
};

struct QueryShingles {
    std::vector<QueryHash> items;   // уникальные hash
    uint32_t total_shingles{0};     // общее число шинглов (с учетом повторов по позициям)
};

QueryShingles build_query_shingles(const std::string& query_text, bool text_is_normalized);

} // namespace l5
