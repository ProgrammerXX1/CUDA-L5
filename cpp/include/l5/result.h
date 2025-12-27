// Back_L5/cpp/include/l5/result.h
#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

namespace l5 {

struct MatchSpan {
    uint32_t q_from{0};
    uint32_t q_to{0};
    uint32_t d_from{0};
    uint32_t d_to{0};
    uint32_t length{0};
};

struct Hit {
    std::string doc_id;
    double C{0.0}; // percent 0..100

    std::vector<MatchSpan> match_spans;

    std::string organization_id;
    std::string external_id;

    // segment provenance
    std::string meta_path;     // "seg_xxx/"
    // file provenance
    std::string source_path;   // stored file path
    std::string source_name;   // original file name

    // small snippet
    std::string preview;
};

struct SearchResult {
    std::string query;
    uint64_t segments_scanned{0};
    std::vector<Hit> hits;
};

nlohmann::json to_json(const SearchResult& r);

} // namespace l5
