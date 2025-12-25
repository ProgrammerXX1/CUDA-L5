#pragma once
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "l5/format.h"

namespace l5 {

struct SegmentData {
    std::filesystem::path seg_dir;
    HeaderV2 header{};
    std::vector<DocMeta> docmeta;
    std::vector<Posting9> postings9;
};

bool load_segment_bin(const std::filesystem::path& seg_dir, SegmentData& out, std::string* err);

bool load_docids_json(const std::filesystem::path& seg_dir,
                      std::vector<std::string>& docids,
                      std::string* err);

} // namespace l5
