#pragma once
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

namespace l5 {

struct SegmentStats {
    uint64_t docs{0};
    uint64_t k9{0};
    uint64_t k13{0};
};

struct SegmentEntry {
    std::string segment_name;
    std::string path;         // "seg_xxx/"
    std::string built_at_utc; // compact
    SegmentStats stats;
};

struct Manifest {
    std::vector<SegmentEntry> segments;
};

Manifest load_manifest(const std::filesystem::path& out_root);
bool append_segment_to_manifest(const std::filesystem::path& out_root, const SegmentEntry& e);

} // namespace l5
