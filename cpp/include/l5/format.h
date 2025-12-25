#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

namespace l5 {

constexpr int K_SHINGLE = 9;

struct HeaderV2 {
    char     magic[4];      // "PLAG"
    uint32_t version;       // 2
    uint32_t n_docs;        // N_docs
    uint64_t n_post9;       // N_post9
    uint64_t n_post13;      // N_post13 (0)
};

struct DocMeta {
    uint32_t tok_len;
    uint64_t simhash_hi;
    uint64_t simhash_lo;
};

struct Posting9 {
    uint64_t h;
    uint32_t did;
    uint32_t pos;
};

// Важно: на диск/с диска пишем/читаем ПО ПОЛЯМ, не sizeof(struct).
bool read_header_v2(std::ifstream& in, HeaderV2& out);
bool write_header_v2(std::ofstream& out, const HeaderV2& h);

std::string utc_now_compact();

bool atomic_replace_file_best_effort(const std::filesystem::path& tmp,
                                     const std::filesystem::path& fin);

} // namespace l5
