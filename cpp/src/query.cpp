#include "l5/query.h"
#include "l5/format.h"

#include <vector>
#include <string>

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
    q.h.reserve((size_t)cnt);

    for (int pos = 0; pos < cnt; ++pos) {
        uint64_t h = hash_shingle_tokens_spans(norm, spans, pos, K_SHINGLE);
        q.h.push_back(h);
    }
    return q;
}

} // namespace l5
