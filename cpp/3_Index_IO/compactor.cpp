#include "l5/compactor.h"

#include "l5/errors.h"
#include "l5/format.h"
#include "l5/manifest.h"
#include "l5/merge.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#if !defined(_WIN32)
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace l5 {

namespace {

static bool is_safe_segment_name(std::string_view s) {
    if (s.empty()) return false;
    if (s == "." || s == "..") return false;
    if (s.find('/')  != std::string_view::npos) return false;
    if (s.find('\\') != std::string_view::npos) return false;
    for (unsigned char c : s) {
        if (!(std::isalnum(c) || c == '_' || c == '-' || c == '.')) return false;
    }
    return true;
}

#if !defined(_WIN32)
class CompactionLock {
    int fd_ = -1;
public:
    explicit CompactionLock(const fs::path& root) {
        const auto p = (root / ".level5_compactor.lock").string();
        fd_ = ::open(p.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
        if (fd_ < 0) throw L5Exception("cannot open compactor lock: " + p);
        if (::flock(fd_, LOCK_EX) != 0) throw L5Exception("flock LOCK_EX failed: " + p);
    }
    ~CompactionLock() { if (fd_ >= 0) { ::flock(fd_, LOCK_UN); ::close(fd_); } }
    CompactionLock(const CompactionLock&) = delete;
    CompactionLock& operator=(const CompactionLock&) = delete;
};
#else
class CompactionLock { public: explicit CompactionLock(const fs::path&) {} };
#endif

static std::string rand_hex8() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;
    uint32_t v = dis(gen);

    static const char* hex = "0123456789abcdef";
    std::string s;
    s.resize(8);
    for (int i = 0; i < 8; ++i) {
        s[7 - i] = hex[v & 0xF];
        v >>= 4;
    }
    return s;
}

static std::string gen_seg_name_compact() {
    return std::string("seg_") + utc_now_compact() + "_c" + rand_hex8();
}

} // namespace

CompactResult compact_once(const fs::path& src_root,
                           const fs::path& dst_root,
                           const CompactOptions& opt) {
    CompactResult rr;

    if (opt.fanout < 2) return rr;
    CompactionLock comp_lk(src_root); 

    Manifest m;
    std::string merr;
    if (!load_manifest_strict(src_root, m, &merr)) {
        throw L5Exception("compact: manifest corrupted: " + merr);
    }
    if (m.segments.size() < (size_t)opt.fanout) return rr;

    const unsigned N = opt.fanout;

    // oldest N: assume manifest order is append order
    std::vector<std::string> to_remove;
    to_remove.reserve(N);

    std::vector<fs::path> src_seg_dirs;
    src_seg_dirs.reserve(N);

    uint64_t in_docs = 0;
    uint64_t in_k9 = 0;

    for (unsigned i = 0; i < N; ++i) {
        const auto& e = m.segments[i];
        if (!is_safe_segment_name(e.segment_name)) {
            throw L5Exception("compact: unsafe segment_name in manifest: " + e.segment_name);
        }
        to_remove.push_back(e.segment_name);
        src_seg_dirs.push_back(src_root / e.segment_name);
        in_docs += e.stats.docs;
        in_k9 += e.stats.k9;
    }

    const fs::path out_root = dst_root.empty() ? src_root : dst_root;

    std::error_code ec;
    fs::create_directories(out_root, ec);

    const std::string new_seg = gen_seg_name_compact();
    const fs::path new_seg_dir = out_root / new_seg;

    // 1) build merged segment
    SegmentEntry out_e = merge_segments_sorted(out_root, new_seg, src_seg_dirs);
    // cheap sanity: merged stats should match sum of inputs
    if (out_e.stats.docs != in_docs || out_e.stats.k9 != in_k9) {
        std::error_code ec_rm;
        fs::remove_all(new_seg_dir, ec_rm);
        throw L5Exception("compact: merged stats mismatch (possible corrupted inputs)");
    }
    // 2) update manifests atomically
    if (fs::equivalent(src_root, out_root, ec) && !ec) {
        std::string uerr;
        if (!replace_segments_in_manifest(out_root, to_remove, out_e, &uerr)) {
            std::error_code ec_rm;
            fs::remove_all(new_seg_dir, ec_rm);
            throw L5Exception("compact: manifest replace failed (same root): " + uerr);
        }
    } else {
        // dst: append new
        if (!append_segment_to_manifest(out_root, out_e)) {
            std::error_code ec_rm;
            fs::remove_all(new_seg_dir, ec_rm);
            throw L5Exception("compact: append to dst manifest failed");
        }
        // src: remove old
        if (!remove_segments_from_manifest(src_root, to_remove)) {
            // rollback best-effort: remove newly appended segment from dst + delete files
            (void)remove_segments_from_manifest(out_root, std::vector<std::string>{new_seg});
            std::error_code ec_rm;
            fs::remove_all(new_seg_dir, ec_rm);
            throw L5Exception("compact: remove_segments_from_manifest failed");
        }
    }

    // 3) delete old segment dirs (best-effort)
    for (const auto& p : src_seg_dirs) {
        std::error_code ec2;
        fs::remove_all(p, ec2);
    }

    rr.did_compact = true;
    rr.merged_segments = N;
    rr.new_segment_name = new_seg;
    rr.out_docs = out_e.stats.docs;
    rr.out_k9 = out_e.stats.k9;
    return rr;
}

} // namespace l5
