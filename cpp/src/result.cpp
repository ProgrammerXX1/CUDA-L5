#include "l5/result.h"

namespace l5 {

nlohmann::json to_json(const SearchResult& r) {
    nlohmann::json j;
    j["query"] = r.query;
    j["segments_scanned"] = r.segments_scanned;

    nlohmann::json arr = nlohmann::json::array();
    for (const auto& m : r.matches) {
        nlohmann::json e;
        e["doc_id"] = m.doc_id;
        e["segment_name"] = m.segment_name;

        e["score"] = m.score;
        e["coverage_query"] = m.coverage_query;
        e["coverage_doc"] = m.coverage_doc;

        e["hits"] = m.hits;
        e["matched_shingles"] = m.matched_shingles;

        nlohmann::json sp = nlohmann::json::array();
        for (const auto& s : m.spans) {
            nlohmann::json se;
            se["q_start"] = s.q_start;
            se["q_end"] = s.q_end;
            se["d_start"] = s.d_start;
            se["d_end"] = s.d_end;
            se["len_shingles"] = s.len_shingles;
            sp.push_back(std::move(se));
        }
        e["spans"] = std::move(sp);

        arr.push_back(std::move(e));
    }
    j["matches"] = std::move(arr);
    return j;
}

} // namespace l5
