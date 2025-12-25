#pragma once
#include <cstdint>
#include <filesystem>
#include <string>

namespace l5 {

struct BuildOptions {
    std::string segment_name; // if empty => auto
    bool strict_text_is_normalized{false}; // env control (will set env in service usually)
    uint32_t max_tokens_per_doc{100000};
    uint32_t max_shingles_per_doc{50000};
    int shingle_stride{1};
    unsigned max_threads{16};
};

struct BuildStats {
    std::string segment_name;
    std::filesystem::path seg_dir;
    uint64_t docs{0};
    uint64_t post9{0};
    unsigned threads{0};
    int strict_text_is_normalized{0};
    std::string built_at_utc;
};

BuildStats build_segment_jsonl(const std::filesystem::path& corpus_jsonl,
                              const std::filesystem::path& out_root,
                              const BuildOptions& opt);

} // namespace l5
