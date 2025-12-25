#pragma once
#include <cstdint>
#include <string>
#include <vector>

#include "l5/reader.h"
#include "l5/query.h"
#include "l5/result.h"

namespace l5 {

struct SearchOptions {
    uint32_t topk{20};

    // stage A candidates
    uint32_t candidates_topn{200};      // сколько документов прогонять в span-builder
    uint32_t min_hits{2};               // минимальные hits для кандидата

    // stop-hash filter
    uint32_t max_postings_per_hash{50000}; // если hash слишком частый — игнорируем

    // span builder
    uint32_t span_min_len{6};           // минимальная длина спана в шинглах
    uint32_t span_gap{0};               // допустимый разрыв (0 = строго подряд)
    uint32_t max_spans_per_doc{10};

    // scoring
    double alpha{0.60};                 // score = alpha*cov_q + (1-alpha)*cov_d
};

std::vector<Match> search_in_segment(const SegmentData& seg,
                                    const std::vector<std::string>& docids,
                                    const QueryShingles& q,
                                    const SearchOptions& opt);

} // namespace l5
