#pragma once
#include <filesystem>
#include <string>

#include "l5/result.h"
#include "l5/search_segment.h"

namespace l5 {

SearchResult search_out_root(const std::filesystem::path& out_root,
                            const std::string& query,
                            bool query_is_normalized,
                            const SearchOptions& opt);

} // namespace l5
