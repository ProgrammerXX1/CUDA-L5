#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

namespace l5 {

struct Match {
    std::string doc_id;
    std::string segment_name;
    double score{0.0};
    uint32_t hits{0};
};

struct SearchResult {
    std::string query;
    uint64_t segments_scanned{0};
    std::vector<Match> matches;
};

nlohmann::json to_json(const SearchResult& r);

} // namespace l5
