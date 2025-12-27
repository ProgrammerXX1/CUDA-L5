// Back_L5/cpp/include/l5/reader.h
#pragma once
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "l5/format.h"
#include "l5/docinfo.h"

namespace l5 {

struct SegmentData {
    std::filesystem::path seg_dir;
    HeaderV2 header{};
    std::vector<DocMeta> docmeta;
    std::vector<Posting9> postings9;
};

bool load_segment_bin(const std::filesystem::path& seg_dir, SegmentData& out, std::string* err);

// Теперь читаем массив DocInfo (новый формат) + поддерживаем старый (array of strings)
bool load_docids_json(const std::filesystem::path& seg_dir,
                      std::vector<DocInfo>& docs,
                      std::string* err);

} // namespace l5
