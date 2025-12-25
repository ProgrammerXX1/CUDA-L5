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
    uint32_t min_hits{1};
};

std::vector<Match> search_in_segment(const SegmentData& seg,
                                    const std::vector<std::string>& docids,
                                    const QueryShingles& q,
                                    const SearchOptions& opt);

} // namespace l5
