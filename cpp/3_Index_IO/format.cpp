// Back_L5/cpp/src/format.cpp
#include "l5/format.h"

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>

namespace l5 {

bool read_header_v2(std::ifstream& in, HeaderV2& out) {
    in.read(reinterpret_cast<char*>(out.magic), 4);
    if (!in) return false;

    in.read(reinterpret_cast<char*>(&out.version), sizeof(out.version));
    in.read(reinterpret_cast<char*>(&out.n_docs), sizeof(out.n_docs));
    in.read(reinterpret_cast<char*>(&out.n_post9), sizeof(out.n_post9));
    in.read(reinterpret_cast<char*>(&out.n_post13), sizeof(out.n_post13));
    if (!in) return false;

    if (std::memcmp(out.magic, "PLAG", 4) != 0) return false;
    if (out.version != 2) return false;
    return true;
}

static bool mul_overflow_u64(uint64_t a, uint64_t b, uint64_t& out) {
    if (a == 0 || b == 0) { out = 0; return false; }
    if (a > (std::numeric_limits<uint64_t>::max() / b)) return true;
    out = a * b;
    return false;
}

bool header_v2_sane(const HeaderV2& h, uint64_t file_bytes, std::string* err) {
    if (std::memcmp(h.magic, "PLAG", 4) != 0) {
        if (err) *err = "bad magic";
        return false;
    }
    if (h.version != 2) {
        if (err) *err = "bad version";
        return false;
    }
    // Prevent pathological allocations on corrupted headers.
    // (Optional) add project hard caps here if you have them.

    uint64_t dm = 0, p9 = 0, p13 = 0;
    if (mul_overflow_u64((uint64_t)h.n_docs,  DOCMETA_V2_BYTES,  dm))  { if (err) *err="docmeta bytes overflow"; return false; }
    if (mul_overflow_u64((uint64_t)h.n_post9, POSTING9_V2_BYTES, p9))  { if (err) *err="post9 bytes overflow";   return false; }
    if (h.n_post13 != 0) {
        // пока не используете post13, но проверка на будущее
        if (mul_overflow_u64((uint64_t)h.n_post13, 1u, p13)) { if (err) *err="post13 bytes overflow"; return false; }
    }

    uint64_t expected = HEADER_V2_BYTES;
    if (expected > std::numeric_limits<uint64_t>::max() - dm)  { if (err) *err="expected overflow (dm)"; return false; }
    expected += dm;
    if (expected > std::numeric_limits<uint64_t>::max() - p9)  { if (err) *err="expected overflow (p9)"; return false; }
    expected += p9;
    if (expected > std::numeric_limits<uint64_t>::max() - p13) { if (err) *err="expected overflow (p13)"; return false; }
    expected += p13;

    if (expected > file_bytes) {
        if (err) *err = "file truncated vs header";
        return false;
    }
    return true;
}


bool write_header_v2(std::ofstream& out, const HeaderV2& h) {
    out.write(reinterpret_cast<const char*>(h.magic), 4);
    out.write(reinterpret_cast<const char*>(&h.version), sizeof(h.version));
    out.write(reinterpret_cast<const char*>(&h.n_docs), sizeof(h.n_docs));
    out.write(reinterpret_cast<const char*>(&h.n_post9), sizeof(h.n_post9));
    out.write(reinterpret_cast<const char*>(&h.n_post13), sizeof(h.n_post13));
    return (bool)out;
}

std::string utc_now_compact() {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const std::time_t t = system_clock::to_time_t(now);
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d_%H%M%S");
    return oss.str();
}

bool atomic_replace_file_best_effort(const std::filesystem::path& tmp,
                                     const std::filesystem::path& fin) {
    try {
        std::error_code ec;
        std::filesystem::create_directories(fin.parent_path(), ec);

        std::filesystem::rename(tmp, fin, ec);
        if (!ec) return true;

        std::filesystem::remove(fin, ec);
        ec.clear();
        std::filesystem::rename(tmp, fin, ec);
        if (!ec) return true;

        std::cerr << "[l5] atomic_replace failed: " << ec.message()
                  << " tmp=" << tmp << " fin=" << fin << "\n";
        return false;
    } catch (const std::exception& e) {
        std::cerr << "[l5] atomic_replace exception: " << e.what()
                  << " tmp=" << tmp << " fin=" << fin << "\n";
        return false;
    }
}

} // namespace l5
