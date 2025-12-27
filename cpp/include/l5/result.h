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

    // Итоговая метрика (как сейчас)
    double C{0.0}; // percent 0..100

    // Новые метрики:
    double Cq{0.0}; // percent 0..100 (coverage query)
    double Cd{0.0}; // percent 0..100 (coverage doc)

    uint32_t matched_shingles{0}; // сумма len_shingles по spans
    uint32_t q_total{0};          // q.total_shingles
    uint32_t d_total{0};          // doc_shingles_count(tok_len)
    double   alpha{0.0};          // фактически использованный alpha

    std::vector<MatchSpan> match_spans;

    std::string organization_id;
    std::string external_id;

    std::string meta_path;
    std::string source_path;
    std::string source_name;

    std::string preview;
};

struct SearchResult {
    std::string query;
    uint64_t segments_scanned{0};
    std::vector<Hit> hits;
};

nlohmann::json to_json(const SearchResult& r);

} // namespace l5
