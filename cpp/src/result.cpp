// Back_L5/cpp/src/result.cpp
#include "l5/result.h"

namespace l5 {

nlohmann::json to_json(const SearchResult& r) {
    nlohmann::json j;
    j["query"] = r.query;
    j["segments_scanned"] = r.segments_scanned;

    nlohmann::json arr = nlohmann::json::array();
    for (const auto& h : r.hits) {
        nlohmann::json e;
        e["doc_id"] = h.doc_id;
        e["C"] = h.C;

        nlohmann::json sp = nlohmann::json::array();
        for (const auto& s : h.match_spans) {
            nlohmann::json se;
            se["q_from"] = s.q_from;
            se["q_to"] = s.q_to;
            se["d_from"] = s.d_from;
            se["d_to"] = s.d_to;
            se["length"] = s.length;
            sp.push_back(std::move(se));
        }
        e["match_spans"] = std::move(sp);

        e["organization_id"] = h.organization_id;
        e["external_id"] = h.external_id;

        e["meta_path"] = h.meta_path;
        e["segment"] = h.meta_path; // alias for convenience

        e["source_path"] = h.source_path;
        e["source_name"] = h.source_name;

        e["preview"] = h.preview;

        arr.push_back(std::move(e));
    }
    j["hits"] = std::move(arr);
    return j;
}

} // namespace l5
