#include "l5/search_segment.h"

#include <algorithm>
#include <cstdint>
#include <vector>

namespace l5 {

static inline std::pair<size_t, size_t> range_for_hash_safe(
    const std::vector<Posting9>& postings,
    uint64_t h
) {
    auto it = std::lower_bound(
        postings.begin(), postings.end(), h,
        [](const Posting9& p, uint64_t key) { return p.h < key; }
    );

    size_t l = (size_t)std::distance(postings.begin(), it);
    size_t r = l;
    while (r < postings.size() && postings[r].h == h) ++r;
    return {l, r};
}

std::vector<Match> search_in_segment(
    const SegmentData& seg,
    const std::vector<std::string>& docids,
    const QueryShingles& q,
    const SearchOptions& opt
) {
    std::vector<Match> out;

    const uint32_t n_docs = seg.header.n_docs;
    if (n_docs == 0) return out;
    if (seg.postings9.empty()) return out;
    if (q.h.empty()) return out;
    if (docids.empty()) return out;

    // Берём безопасный n_docs на случай несоответствия docids.
    const uint32_t n_docs_safe = std::min<uint32_t>(n_docs, (uint32_t)docids.size());
    if (n_docs_safe == 0) return out;

    std::vector<uint32_t> hits(n_docs_safe, 0);

    for (uint64_t h : q.h) {
        auto [l, r] = range_for_hash_safe(seg.postings9, h);
        for (size_t i = l; i < r; ++i) {
            const uint32_t did = seg.postings9[i].did;
            if (did < n_docs_safe) ++hits[did];
        }
    }

    const double denom = q.h.empty() ? 1.0 : (double)q.h.size();
    const std::string seg_name = seg.seg_dir.filename().string();

    out.reserve(256);
    for (uint32_t did = 0; did < n_docs_safe; ++did) {
        const uint32_t cnt = hits[did];
        if (cnt < opt.min_hits) continue;

        Match m;
        m.doc_id = docids[did];
        m.segment_name = seg_name;
        m.hits = cnt;
        m.score = (double)cnt / denom;
        out.push_back(std::move(m));
    }

    std::sort(out.begin(), out.end(), [](const Match& a, const Match& b) {
        if (a.score != b.score) return a.score > b.score;
        return a.hits > b.hits;
    });

    if (out.size() > opt.topk) out.resize(opt.topk);
    return out;
}

} // namespace l5
