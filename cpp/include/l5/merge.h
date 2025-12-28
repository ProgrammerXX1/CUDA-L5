#pragma once
#include <filesystem>
#include <string>
#include <vector>

#include "l5/manifest.h"

namespace l5 {

// Merge already-sorted segments into one new segment (k-way merge postings).
// Does NOT re-tokenize. Bounded memory.
SegmentEntry merge_segments_sorted(const std::filesystem::path& dst_root,
                                  const std::string& new_segment_name,
                                  const std::vector<std::filesystem::path>& src_seg_dirs);

} // namespace l5
