// Back_L5/cpp/include/l5/builder.h
#pragma once
#include <cstdint>
#include <filesystem>
#include <string>

namespace l5 {

struct BuildOptions {
    std::string segment_name; // if empty => auto

    // if true -> if doc has no "text_is_normalized"/"normalized" flag => treat as NOT normalized
    bool strict_text_is_normalized{false};

    // hard limits / degradation
    uint32_t max_text_bytes_per_doc{8u * 1024u * 1024u}; // 8 MiB, truncate input text
    uint32_t max_tokens_per_doc{100000};                 // truncate tokens
    uint32_t max_shingles_per_doc{50000};                // cap postings per doc
    uint32_t max_docs_in_segment{0};                     // 0 => unlimited

    // shingling
    int shingle_stride{1};

    // parallelism + bounded pipeline memory
    unsigned max_threads{16};
    uint32_t inflight_docs{0}; // 0 => auto (4*threads), bounds queue sizes

    // sorting budget (builder process RAM cap)
    uint64_t ram_limit_bytes{512ull * 1024ull * 1024ull}; // 512 MiB
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
