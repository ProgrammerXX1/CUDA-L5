// Back_L5/cpp/src/format.cpp
#include "l5/format.h"

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
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
