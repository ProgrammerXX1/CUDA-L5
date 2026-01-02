#pragma once
#include <cstdint>
#include <filesystem>
#include <string>
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

bool load_manifest_strict(const std::filesystem::path& out_root, Manifest& out, std::string* err);


Manifest load_manifest(const std::filesystem::path& out_root);

// append (как было)
bool append_segment_to_manifest(const std::filesystem::path& out_root, const SegmentEntry& e);

// NEW: rewrite whole manifest atomically
bool save_manifest(const std::filesystem::path& out_root, const Manifest& m);

// NEW: remove a set of segments and rewrite manifest atomically
bool remove_segments_from_manifest(const std::filesystem::path& out_root,
                                   const std::vector<std::string>& segment_names);
// Atomic manifest update: remove a set of segments and append one new segment in a single locked commit.
bool replace_segments_in_manifest(const std::filesystem::path& out_root,
                                 const std::vector<std::string>& remove_segment_names,
                                 const SegmentEntry& add_entry,
                                 std::string* err = nullptr);
} // namespace l5
