// Back_L5/cpp/src/query.cpp
#include "l5/query.h"
#include "l5/format.h"

#include <algorithm>
#include <unordered_map>
#include <vector>

#include "text_common.h"

namespace l5 {

QueryShingles build_query_shingles(const std::string& query_text, bool text_is_normalized) {
    std::string norm;
    if (text_is_normalized) norm = query_text;
    else norm = normalize_for_shingles_simple(query_text);

    std::vector<TokenSpan> spans;
    spans.reserve(256);
    tokenize_spans(norm, spans);

    QueryShingles q;
    if (spans.size() < (size_t)K_SHINGLE) return q;

    const int n = (int)spans.size();
    const int cnt = n - K_SHINGLE + 1;
    if (cnt <= 0) return q;

    // hash -> list of positions
    std::unordered_map<uint64_t, std::vector<uint32_t>> mp;
    mp.reserve((size_t)cnt);

    for (int pos = 0; pos < cnt; ++pos) {
        uint64_t h = hash_shingle_tokens_spans(norm, spans, pos, K_SHINGLE);
        mp[h].push_back((uint32_t)pos);
        q.total_shingles++;
    }

    q.items.reserve(mp.size());
    for (auto& kv : mp) {
        QueryHash it;
        it.h = kv.first;
        it.qpos = std::move(kv.second);
        // для удобства span-builder: сортируем позиции
        std::sort(it.qpos.begin(), it.qpos.end());
        q.items.push_back(std::move(it));
    }

    // фиксируем детерминизм: сортируем items по hash
    std::sort(q.items.begin(), q.items.end(), [](const QueryHash& a, const QueryHash& b) {
        return a.h < b.h;
    });

    return q;
}

} // namespace l5
