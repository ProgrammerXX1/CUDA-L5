#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace l5 {

struct QueryShingles {
    std::vector<uint64_t> h; // k=9 shingle hashes
};

QueryShingles build_query_shingles(const std::string& query_text, bool text_is_normalized);

} // namespace l5
