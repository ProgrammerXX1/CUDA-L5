// Back_L5/cpp/src/search_segment.cpp
#include "l5/search_segment.h"
#include "l5/format.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <utility>
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

static inline int64_t delta64(const Point& p) {
    return (int64_t)p.dpos - (int64_t)p.qpos;
}

static uint32_t union_len_inclusive_from_spans(const std::vector<SpanTmp>& spans, bool query_side) {
    if (spans.empty()) return 0;
    std::vector<std::pair<uint32_t, uint32_t>> iv;
    iv.reserve(spans.size());
    for (const auto& s : spans) {
        const uint32_t a = query_side ? s.q_start : s.d_start;
        const uint32_t b = query_side ? s.q_end   : s.d_end;
        if (b >= a) iv.emplace_back(a, b);
    }
    if (iv.empty()) return 0;
    std::sort(iv.begin(), iv.end(), [](const auto& x, const auto& y) {
        if (x.first != y.first) return x.first < y.first;
        return x.second < y.second;
    });
    uint32_t total = 0;
    uint32_t cur_a = iv[0].first;
    uint32_t cur_b = iv[0].second;
    for (size_t i = 1; i < iv.size(); ++i) {
        const uint32_t a = iv[i].first;
        const uint32_t b = iv[i].second;
        if (a <= cur_b + 1) {
            if (b > cur_b) cur_b = b;
        } else {
            total += (cur_b >= cur_a) ? (cur_b - cur_a + 1) : 0;
            cur_a = a;
            cur_b = b;
        }
    }
    total += (cur_b >= cur_a) ? (cur_b - cur_a + 1) : 0;
    return total;
}

static std::vector<SpanTmp> build_spans_for_doc(
    std::vector<Point>& pts,
    const SearchOptions& opt
) {
    // In-place sort by (delta, qpos, dpos) and scan.
    // This avoids unordered_map + copying all points into per-delta vectors.
    std::sort(pts.begin(), pts.end(), [](const Point& a, const Point& b) {
        const int64_t da = delta64(a);
        const int64_t db = delta64(b);
        if (da != db) return da < db;
        if (a.qpos != b.qpos) return a.qpos < b.qpos;
        return a.dpos < b.dpos;
    });
    pts.erase(std::unique(pts.begin(), pts.end(), [](const Point& a, const Point& b) {
        return a.qpos == b.qpos && a.dpos == b.dpos;
    }), pts.end());

    std::vector<SpanTmp> spans;
    spans.reserve(16);

    const uint32_t gap = opt.span_gap;
    size_t i = 0;
    while (i < pts.size()) {
        const int64_t d = delta64(pts[i]);
        size_t j = i + 1;
        while (j < pts.size() && delta64(pts[j]) == d) ++j; // [i, j)
        if (j == i) { ++i; continue; }

        uint32_t cur_qs = pts[i].qpos, cur_qe = pts[i].qpos;
        uint32_t cur_ds = pts[i].dpos, cur_de = pts[i].dpos;

        for (size_t k = i + 1; k < j; ++k) {
            const auto& p = pts[k];

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
        i = j;
    }

    std::sort(spans.begin(), spans.end(), [](const SpanTmp& a, const SpanTmp& b) {
        if (a.len_shingles != b.len_shingles) return a.len_shingles > b.len_shingles;
        if (a.q_start != b.q_start) return a.q_start < b.q_start;
        if (a.d_start != b.d_start) return a.d_start < b.d_start;
        if (a.q_end != b.q_end) return a.q_end < b.q_end;
        return a.d_end < b.d_end;
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

    const uint32_t n_docs_safe = std::min<uint32_t>({
        n_docs,
        (uint32_t)docinfo.size(),
        (uint32_t)seg.docmeta.size()
    });
    if (n_docs_safe == 0) return out;

    struct HashRange {
        const QueryHash* qi{nullptr};
        size_t l{0};
        size_t r{0};
    };
    std::vector<HashRange> q_ranges;
    q_ranges.reserve(q.items.size());
    for (const auto& qi : q.items) {
        auto [l, r] = range_for_hash_safe(seg.postings9, qi.h);
        const uint64_t range_len = (uint64_t)(r - l);
        if (range_len == 0) continue;
        if (range_len > (uint64_t)opt.max_postings_per_hash) continue; // stop-hash
        q_ranges.push_back(HashRange{&qi, l, r});
    }
    if (q_ranges.empty()) return out;

    // -------------------------
    // Stage A: hits per doc
    // -------------------------
    std::vector<uint32_t> hits(n_docs_safe, 0);

    for (const auto& hr : q_ranges) {
        const auto& qi = *hr.qi;
        const uint32_t w = (uint32_t)qi.qpos.size(); // weight by query-occurrences of this hash
        size_t i = hr.l;
        while (i < hr.r) {
            const uint32_t did = seg.postings9[i].did;
            size_t j = i + 1;
            while (j < hr.r && seg.postings9[j].did == did) ++j; // skip same-doc postings
            if (did < n_docs_safe) hits[did] += w;
            i = j;
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

    // nth_element требует nth в [begin, end), а не == end
    const auto cand_better = [&](uint32_t a, uint32_t b) {
        const uint32_t ha = hits[a];
        const uint32_t hb = hits[b];
        if (ha != hb) return ha > hb;
        return a < b; // deterministic tie-break
    };
    if (cand.size() > topN) {
        std::nth_element(cand.begin(), cand.begin() + topN, cand.end(), cand_better);
        cand.resize(topN);
    }
    std::sort(cand.begin(), cand.end(), cand_better);

    // did -> slot index, to avoid unordered_map<uint32_t, vector<Point>>
    std::unordered_map<uint32_t, uint32_t> cand_slot;
    cand_slot.reserve(cand.size() * 2);
    for (uint32_t i = 0; i < (uint32_t)cand.size(); ++i) cand_slot.emplace(cand[i], i);

    std::vector<std::vector<Point>> points_by_cand;
    points_by_cand.resize(cand.size());

    // -------------------------
    // Stage B: collect points and build spans
    // -------------------------
    for (const auto& hr : q_ranges) {
        const auto& qi = *hr.qi;
        for (size_t i = hr.l; i < hr.r; ++i) {
            const auto& p = seg.postings9[i];
            const uint32_t did = p.did;
            if (did >= n_docs_safe) continue;
            auto it_slot = cand_slot.find(did);
            if (it_slot == cand_slot.end()) continue;
            auto& vec = points_by_cand[it_slot->second];
            if (vec.empty()) vec.reserve(128);
            for (uint32_t qpos : qi.qpos) vec.push_back(Point{qpos, p.pos});
        }
    }

    out.reserve(cand.size());

    for (uint32_t idx = 0; idx < (uint32_t)cand.size(); ++idx) {
        const uint32_t did = cand[idx];
        auto& pts = points_by_cand[idx];
        if (pts.empty()) continue;

        auto spans = build_spans_for_doc(pts, opt);
        if (spans.empty()) continue;

        const uint32_t matched_q = union_len_inclusive_from_spans(spans, /*query_side=*/true);
        const uint32_t matched_d = union_len_inclusive_from_spans(spans, /*query_side=*/false);

        const uint32_t q_total = q.total_shingles;
        const uint32_t d_total = doc_shingles_count(seg.docmeta[did].tok_len);

        double cov_q = (q_total > 0) ? (double)matched_q / (double)q_total : 0.0;
        double cov_d = (d_total > 0) ? (double)matched_d / (double)d_total : 0.0;
        if (cov_q > 1.0) cov_q = 1.0;
        if (cov_d > 1.0) cov_d = 1.0;

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

        // debug metrics / explainability
        h.alpha = opt.alpha;
        h.matched_shingles = matched_q;
        h.q_total = q_total;
        h.d_total = d_total;
        h.Cq = cov_q * 100.0;
        h.Cd = cov_d * 100.0;
        h.C  = score * 100.0;

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
        if (a.C != b.C) return a.C > b.C;
        return a.doc_id < b.doc_id;
    });
    if (out.size() > opt.topk) out.resize(opt.topk);

    return out;
}

} // namespace l5
