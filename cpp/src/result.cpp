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
        e["hits"] = m.hits;
        arr.push_back(std::move(e));
    }
    j["matches"] = std::move(arr);
    return j;
}

} // namespace l5
