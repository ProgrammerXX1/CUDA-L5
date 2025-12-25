#include "l5/search_multi.h"
#include "l5/manifest.h"
#include "l5/reader.h"
#include "l5/query.h"

#include <algorithm>
#include <unordered_map>

namespace l5 {

SearchResult search_out_root(const std::filesystem::path& out_root,
                            const std::string& query,
                            bool query_is_normalized,
                            const SearchOptions& opt) {
    SearchResult res;
    res.query = query;

    auto manifest = load_manifest(out_root);
    QueryShingles q = build_query_shingles(query, query_is_normalized);

    std::unordered_map<std::string, Match> best; // doc_id -> best match
    best.reserve(1024);

    for (const auto& seg : manifest.segments) {
        const auto seg_dir = out_root / seg.segment_name;

        SegmentData segdata;
        std::string err;
        if (!load_segment_bin(seg_dir, segdata, &err)) {
            continue;
        }
        std::vector<std::string> docids;
        if (!load_docids_json(seg_dir, docids, &err)) {
            continue;
        }

        ++res.segments_scanned;

        auto matches = search_in_segment(segdata, docids, q, opt);
        for (auto& m : matches) {
            auto it = best.find(m.doc_id);
            if (it == best.end() || m.score > it->second.score) {
                best[m.doc_id] = m;
            }
        }
    }

    res.matches.reserve(best.size());
    for (auto& kv : best) res.matches.push_back(std::move(kv.second));

    std::sort(res.matches.begin(), res.matches.end(), [](const Match& a, const Match& b) {
        if (a.score != b.score) return a.score > b.score;
        return a.hits > b.hits;
    });
    if (res.matches.size() > opt.topk) res.matches.resize(opt.topk);

    return res;
}

} // namespace l5
