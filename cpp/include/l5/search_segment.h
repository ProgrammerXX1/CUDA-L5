// Back_L5/cpp/include/l5/search_segment.h
#pragma once
#include <cstdint>
#include <vector>

#include "l5/reader.h"
#include "l5/query.h"
#include "l5/result.h"
#include "l5/docinfo.h"

namespace l5 {

struct SearchOptions {
    uint32_t topk{20};
    uint32_t candidates_topn{200};
    uint32_t min_hits{2};
    uint32_t max_postings_per_hash{50000};

    uint32_t span_min_len{6};
    uint32_t span_gap{0};
    uint32_t max_spans_per_doc{10};

    double alpha{0.60};
};

std::vector<Hit> search_in_segment(const SegmentData& seg,
                                  const std::vector<DocInfo>& docinfo,
                                  const QueryShingles& q,
                                  const SearchOptions& opt);

} // namespace l5
