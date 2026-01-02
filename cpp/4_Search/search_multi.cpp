// Back_L5/cpp/src/search_multi.cpp
#include "l5/search_multi.h"
#include "l5/manifest.h"
#include "l5/reader.h"
#include "l5/query.h"
#include "l5/search_segment.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <unordered_map>

namespace l5 {

namespace {
static inline bool search_trace_enabled() {
    static int enabled = []() -> int {
        const char* v = std::getenv("L5_SEARCH_TRACE");
        if (!v || !*v) return 0;
        if (v[0] == '0') return 0;
        return 1;
    }();
    return enabled != 0;
}
} // namespace

SearchResult search_out_root(const std::filesystem::path& out_root,
                            const std::string& query,
                            bool query_is_normalized,
                            const SearchOptions& opt) {
    SearchResult res;
    res.query = query;

    auto manifest = load_manifest(out_root);
    QueryShingles q = build_query_shingles(query, query_is_normalized);
    if (search_trace_enabled()) {
        std::fprintf(stderr,
            "[L5_SEARCH] out_root=%s segs=%zu q_items=%zu q_total=%u query_norm=%d\n",
            out_root.string().c_str(),
            manifest.segments.size(),
            q.items.size(),
            (unsigned)q.total_shingles,
            query_is_normalized ? 1 : 0
        );
    }

    // best by doc_id
    std::unordered_map<std::string, Hit> best;
    best.reserve(1024);

    for (const auto& seg : manifest.segments) {
        const auto seg_dir = out_root / seg.segment_name;

        SegmentData segdata;
        std::string err;
        if (!load_segment_bin(seg_dir, segdata, &err)) {
            if (search_trace_enabled()) {
                std::fprintf(stderr, "[L5_SEARCH] load_segment_bin FAIL seg=%s err=%s\n",
                             seg.segment_name.c_str(), err.c_str());
            }
            continue;
        }

        std::vector<DocInfo> docinfo;
        if (!load_docids_json(seg_dir, docinfo, &err)) {
            if (search_trace_enabled()) {
                std::fprintf(stderr, "[L5_SEARCH] load_docids_json FAIL seg=%s err=%s\n",
                             seg.segment_name.c_str(), err.c_str());
            }
            continue;
        }

        ++res.segments_scanned;

        auto hits = search_in_segment(segdata, docinfo, q, opt);
        if (search_trace_enabled()) {
            std::fprintf(stderr, "[L5_SEARCH] seg=%s hits=%zu\n",
                         seg.segment_name.c_str(), hits.size());
        }
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
        if (a.C != b.C) return a.C > b.C;
        return a.doc_id < b.doc_id;
    });
    if (res.hits.size() > opt.topk) res.hits.resize(opt.topk);

    return res;
}

} // namespace l5
