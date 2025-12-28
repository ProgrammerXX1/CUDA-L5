#pragma once
#include <cstdint>
#include <filesystem>
#include <string>

namespace l5 {

struct CompactOptions {
    unsigned fanout{20}; // merge N segments into 1
};

struct CompactResult {
    bool did_compact{false};
    unsigned merged_segments{0};
    std::string new_segment_name;
    uint64_t out_docs{0};
    uint64_t out_k9{0};
};

// If dst_root == src_root -> compact within same root (like L5 shard).
CompactResult compact_once(const std::filesystem::path& src_root,
                           const std::filesystem::path& dst_root,
                           const CompactOptions& opt);

} // namespace l5
