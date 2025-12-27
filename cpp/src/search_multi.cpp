// Back_L5/cpp/src/search_multi.cpp
#include "l5/search_multi.h"
#include "l5/manifest.h"
#include "l5/reader.h"
#include "l5/query.h"
#include "l5/search_segment.h"

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

    // best by doc_id
    std::unordered_map<std::string, Hit> best;
    best.reserve(1024);

    for (const auto& seg : manifest.segments) {
        const auto seg_dir = out_root / seg.segment_name;

        SegmentData segdata;
        std::string err;
        if (!load_segment_bin(seg_dir, segdata, &err)) continue;

        std::vector<DocInfo> docinfo;
        if (!load_docids_json(seg_dir, docinfo, &err)) continue;

        ++res.segments_scanned;

        auto hits = search_in_segment(segdata, docinfo, q, opt);
        for (auto& h : hits) {
            auto it = best.find(h.doc_id);
            if (it == best.end() || h.C > it->second.C) {
                best[h.doc_id] = std::move(h);
            }
        }
    }

    res.hits.reserve(best.size());
    for (auto& kv : best) res.hits.push_back(std::move(kv.second));

    std::sort(res.hits.begin(), res.hits.end(), [](const Hit& a, const Hit& b) {
        return a.C > b.C;
    });
    if (res.hits.size() > opt.topk) res.hits.resize(opt.topk);

    return res;
}

} // namespace l5
