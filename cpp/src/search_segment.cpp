// Back_L5/cpp/src/search_segment.cpp
#include "l5/search_segment.h"
#include "l5/format.h"

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace l5 {

static inline std::pair<size_t, size_t> range_for_hash_safe(
    const std::vector<Posting9>& postings,
    uint64_t h
) {
    auto l_it = std::lower_bound(
        postings.begin(), postings.end(), h,
        [](const Posting9& p, uint64_t key) { return p.h < key; }
    );

    auto r_it = std::upper_bound(
        l_it, postings.end(), h,
        [](uint64_t key, const Posting9& p) { return key < p.h; }
    );

    return {
        (size_t)std::distance(postings.begin(), l_it),
        (size_t)std::distance(postings.begin(), r_it)
    };
}

static inline uint32_t doc_shingles_count(uint32_t tok_len) {
    if (tok_len < (uint32_t)K_SHINGLE) return 0;
    return tok_len - (uint32_t)K_SHINGLE + 1;
}

struct Point {
    uint32_t qpos;
    uint32_t dpos;
};

struct SpanTmp {
    uint32_t q_start{0};
    uint32_t q_end{0};
    uint32_t d_start{0};
    uint32_t d_end{0};
    uint32_t len_shingles{0};
};

static std::vector<SpanTmp> build_spans_for_doc(
    std::vector<Point>& pts,
    const SearchOptions& opt
) {
    // group by delta = dpos - qpos
    std::unordered_map<int32_t, std::vector<Point>> by_delta;
    by_delta.reserve(64);

    for (const auto& p : pts) {
        int32_t delta = (int32_t)p.dpos - (int32_t)p.qpos;
        by_delta[delta].push_back(p);
    }

    std::vector<SpanTmp> spans;
    spans.reserve(16);

    for (auto& kv : by_delta) {
        auto& v = kv.second;
        if (v.empty()) continue;

        std::sort(v.begin(), v.end(), [](const Point& a, const Point& b) {
            if (a.qpos != b.qpos) return a.qpos < b.qpos;
            return a.dpos < b.dpos;
        });

        const uint32_t gap = opt.span_gap;

        uint32_t cur_qs = v[0].qpos, cur_qe = v[0].qpos;
        uint32_t cur_ds = v[0].dpos, cur_de = v[0].dpos;

        for (size_t i = 1; i < v.size(); ++i) {
            const auto& p = v[i];

            const bool cont_q = (p.qpos <= cur_qe + 1 + gap);
            const bool cont_d = (p.dpos <= cur_de + 1 + gap);

            if (cont_q && cont_d) {
                if (p.qpos > cur_qe) cur_qe = p.qpos;
                if (p.dpos > cur_de) cur_de = p.dpos;
            } else {
                SpanTmp s;
                s.q_start = cur_qs;
                s.q_end = cur_qe;
                s.d_start = cur_ds;
                s.d_end = cur_de;
                s.len_shingles = (cur_qe >= cur_qs) ? (cur_qe - cur_qs + 1) : 0;
                if (s.len_shingles >= opt.span_min_len) spans.push_back(s);

                cur_qs = cur_qe = p.qpos;
                cur_ds = cur_de = p.dpos;
            }
        }

        SpanTmp s;
        s.q_start = cur_qs;
        s.q_end = cur_qe;
        s.d_start = cur_ds;
        s.d_end = cur_de;
        s.len_shingles = (cur_qe >= cur_qs) ? (cur_qe - cur_qs + 1) : 0;
        if (s.len_shingles >= opt.span_min_len) spans.push_back(s);
    }

    std::sort(spans.begin(), spans.end(), [](const SpanTmp& a, const SpanTmp& b) {
        if (a.len_shingles != b.len_shingles) return a.len_shingles > b.len_shingles;
        return a.q_start < b.q_start;
    });

    if (spans.size() > opt.max_spans_per_doc) spans.resize(opt.max_spans_per_doc);
    return spans;
}

std::vector<Hit> search_in_segment(
    const SegmentData& seg,
    const std::vector<DocInfo>& docinfo,
    const QueryShingles& q,
    const SearchOptions& opt
) {
    std::vector<Hit> out;

    const uint32_t n_docs = seg.header.n_docs;
    if (n_docs == 0) return out;
    if (seg.postings9.empty()) return out;
    if (q.items.empty() || q.total_shingles == 0) return out;
    if (docinfo.empty()) return out;

    const uint32_t n_docs_safe = std::min<uint32_t>(n_docs, (uint32_t)docinfo.size());
    if (n_docs_safe == 0) return out;

    // -------------------------
    // Stage A: hits per doc
    // -------------------------
    std::vector<uint32_t> hits(n_docs_safe, 0);

    for (const auto& qi : q.items) {
        auto [l, r] = range_for_hash_safe(seg.postings9, qi.h);
        const uint64_t range_len = (uint64_t)(r - l);
        if (range_len == 0) continue;
        if (range_len > (uint64_t)opt.max_postings_per_hash) continue; // stop-hash

        for (size_t i = l; i < r; ++i) {
            uint32_t did = seg.postings9[i].did;
            if (did < n_docs_safe) ++hits[did];
        }
    }

    std::vector<uint32_t> cand;
    cand.reserve(1024);
    for (uint32_t did = 0; did < n_docs_safe; ++did) {
        if (hits[did] >= opt.min_hits) cand.push_back(did);
    }
    if (cand.empty()) return out;

    const uint32_t topN = std::min<uint32_t>(opt.candidates_topn, (uint32_t)cand.size());
    if (topN == 0) return out;

    // FIX: nth_element требует nth в [begin, end), а не == end.
    if (cand.size() > topN) {
        std::nth_element(
            cand.begin(), cand.begin() + topN, cand.end(),
            [&](uint32_t a, uint32_t b) { return hits[a] > hits[b]; }
        );
        cand.resize(topN);
    } else {
        // cand.size() == topN => оставляем как есть
        cand.resize(topN);
    }

    std::unordered_set<uint32_t> cand_set;
    cand_set.reserve(cand.size() * 2);
    for (uint32_t did : cand) cand_set.insert(did);

    // -------------------------
    // Stage B: collect points and build spans
    // -------------------------
    std::unordered_map<uint32_t, std::vector<Point>> points_by_doc;
    points_by_doc.reserve(cand.size() * 2);

    for (const auto& qi : q.items) {
        auto [l, r] = range_for_hash_safe(seg.postings9, qi.h);
        const uint64_t range_len = (uint64_t)(r - l);
        if (range_len == 0) continue;
        if (range_len > (uint64_t)opt.max_postings_per_hash) continue;

        for (size_t i = l; i < r; ++i) {
            const auto& p = seg.postings9[i];
            const uint32_t did = p.did;
            if (did >= n_docs_safe) continue;
            if (cand_set.find(did) == cand_set.end()) continue;

            auto& vec = points_by_doc[did];
            if (vec.capacity() < 64) vec.reserve(64);

            for (uint32_t qpos : qi.qpos) {
                vec.push_back(Point{qpos, p.pos});
            }
        }
    }

    out.reserve(cand.size());

    for (uint32_t did : cand) {
        auto it = points_by_doc.find(did);
        if (it == points_by_doc.end()) continue;

        auto& pts = it->second;
        if (pts.empty()) continue;

        auto spans = build_spans_for_doc(pts, opt);
        if (spans.empty()) continue;

        uint32_t matched = 0;
        for (const auto& s : spans) matched += s.len_shingles;

        const uint32_t q_total = q.total_shingles;
        const uint32_t d_total = doc_shingles_count(seg.docmeta[did].tok_len);

        double cov_q = (q_total > 0) ? (double)matched / (double)q_total : 0.0;
        double cov_d = (d_total > 0) ? (double)matched / (double)d_total : 0.0;

        // matched может быть > total из-за перекрытий/dup points/allow gap.
        if (cov_q > 1.0) cov_q = 1.0;
        if (cov_d > 1.0) cov_d = 1.0;
        if (cov_q < 0.0) cov_q = 0.0;
        if (cov_d < 0.0) cov_d = 0.0;

        double score = opt.alpha * cov_q + (1.0 - opt.alpha) * cov_d; // 0..1
        if (score < 0.0) score = 0.0;
        if (score > 1.0) score = 1.0;

        const auto& di = docinfo[did];

        Hit h;
        h.doc_id = di.doc_id;
        h.organization_id = di.organization_id;
        h.external_id = di.external_id.empty() ? di.doc_id : di.external_id;
        h.meta_path = di.meta_path.empty() ? seg.seg_dir.filename().string() + "/" : di.meta_path;

        h.source_path = di.source_path;
        h.source_name = di.source_name;

        h.preview = di.preview_text;
        h.C = score * 100.0;

        h.match_spans.reserve(spans.size());
        for (const auto& s : spans) {
            MatchSpan ms;
            ms.q_from = s.q_start;
            ms.q_to = s.q_end;
            ms.d_from = s.d_start;
            ms.d_to = s.d_end;
            ms.length = s.len_shingles;
            h.match_spans.push_back(ms);
        }

        out.push_back(std::move(h));
    }

    std::sort(out.begin(), out.end(), [](const Hit& a, const Hit& b) {
        return a.C > b.C;
    });
    if (out.size() > opt.topk) out.resize(opt.topk);

    return out;
}

} // namespace l5
