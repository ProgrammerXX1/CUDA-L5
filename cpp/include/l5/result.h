#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

namespace l5 {

struct Span {
    uint32_t q_start{0};
    uint32_t q_end{0};
    uint32_t d_start{0};
    uint32_t d_end{0};
    uint32_t len_shingles{0};
};

struct Match {
    std::string doc_id;
    std::string segment_name;

    double score{0.0};
    double coverage_query{0.0};
    double coverage_doc{0.0};

    uint32_t hits{0};                 // raw hits (stage A)
    uint32_t matched_shingles{0};     // sum(span.len_shingles)

    std::vector<Span> spans;
};

struct SearchResult {
    std::string query;
    uint64_t segments_scanned{0};
    std::vector<Match> matches;
};

nlohmann::json to_json(const SearchResult& r);

} // namespace l5
