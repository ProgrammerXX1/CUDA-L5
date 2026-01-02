// Back_L5/cpp/src/query.cpp
#include "l5/query.h"
#include "l5/format.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "text_common.h"

namespace l5 {

QueryShingles build_query_shingles(const std::string& query_text, bool text_is_normalized) {
    // avoid extra copy when caller already provides normalized text
    std::string norm;
    const std::string* ptxt = &query_text;
    if (!text_is_normalized) {
        norm = normalize_for_shingles_simple(query_text);
        ptxt = &norm;
    }
    const std::string& txt = *ptxt;

    std::vector<TokenSpan> spans;
    spans.reserve(256);
    tokenize_spans(txt, spans);

    QueryShingles q;
    if (spans.size() < (size_t)K_SHINGLE) return q;

    const int n = (int)spans.size();
    const int cnt = n - K_SHINGLE + 1;
    if (cnt <= 0) return q;

    // Collect (hash, qpos), then sort/group:
    //  - deterministic
    //  - fewer allocations than unordered_map-of-vectors
    std::vector<std::pair<uint64_t, uint32_t>> hv;
    hv.reserve((size_t)cnt);


    for (int pos = 0; pos < cnt; ++pos) {
        uint64_t h = hash_shingle_tokens_spans(txt, spans, pos, K_SHINGLE);
        hv.emplace_back(h, (uint32_t)pos);
        q.total_shingles++;
    }

    std::sort(hv.begin(), hv.end(), [](const auto& a, const auto& b) {
        if (a.first != b.first) return a.first < b.first;
        return a.second < b.second;
    });

    // фиксируем детерминизм: сортируем items по hash
    q.items.reserve(hv.size()); // upper bound
    size_t i = 0;
    while (i < hv.size()) {
        const uint64_t h = hv[i].first;
        size_t j = i + 1;
        while (j < hv.size() && hv[j].first == h) ++j;

        QueryHash it;
        it.h = h;
        it.qpos.reserve(j - i);
        for (size_t k = i; k < j; ++k) it.qpos.push_back(hv[k].second); // already sorted
        q.items.push_back(std::move(it));
        i = j;
    }

    return q;
}

} // namespace l5
