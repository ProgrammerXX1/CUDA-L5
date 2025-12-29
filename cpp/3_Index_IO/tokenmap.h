// cpp/include/l5/tokenmap.h
#pragma once
#include <cstdint>

namespace l5 {

// Token -> byte span in ORIGINAL (extracted) document text (UTF-8 bytes).
// start/len are relative to doc start (0..).
struct TokOff {
    uint32_t start{0};
    uint32_t len{0};
};

// Per-doc index into tokmap.bin
// off = offset in TokOff records (not bytes)
// n   = number of tokens in this doc
struct TokIdx {
    uint64_t off{0};
    uint32_t n{0};
};

} // namespace l5
