#include "l5/compactor.h"

#include "l5/errors.h"
#include "l5/format.h"
#include "l5/manifest.h"
#include "l5/merge.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <random>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace l5 {

namespace {

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
        to_remove.push_back(e.segment_name);
        src_seg_dirs.push_back(src_root / e.segment_name);
        in_docs += e.stats.docs;
        in_k9 += e.stats.k9;
    }

    const fs::path out_root = dst_root.empty() ? src_root : dst_root;

    std::error_code ec;
    fs::create_directories(out_root, ec);

    const std::string new_seg = gen_seg_name_compact();

    // 1) build merged segment
    SegmentEntry out_e = merge_segments_sorted(out_root, new_seg, src_seg_dirs);

    // 2) update manifests atomically
    if (fs::equivalent(src_root, out_root, ec) && !ec) {
        Manifest mm;
        std::string merr2;
        if (!load_manifest_strict(src_root, mm, &merr2)) {
            throw L5Exception("compact: manifest corrupted (same root): " + merr2);
        }

        // filter out old
        Manifest keep;
        keep.segments.reserve(mm.segments.size());
        for (auto& e : mm.segments) {
            bool del = false;
            for (const auto& s : to_remove) {
                if (e.segment_name == s) { del = true; break; }
            }
            if (!del) keep.segments.push_back(std::move(e));
        }
        keep.segments.push_back(out_e);
        if (!save_manifest(src_root, keep)) throw L5Exception("compact: save_manifest failed (same root)");
    } else {
        // src: remove old
        if (!remove_segments_from_manifest(src_root, to_remove)) {
            throw L5Exception("compact: remove_segments_from_manifest failed");
        }

        // dst: append new
        if (!append_segment_to_manifest(out_root, out_e)) {
            throw L5Exception("compact: append to dst manifest failed");
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
