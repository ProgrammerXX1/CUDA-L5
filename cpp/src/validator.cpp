// Back_L5/cpp/src/validator.cpp
#include "l5/validator.h"
#include "l5/reader.h"
#include "l5/manifest.h"
#include "l5/format.h"
#include "l5/docinfo.h"

#include <filesystem>
#include <sstream>

namespace l5 {

static bool is_sorted_postings(const std::vector<Posting9>& p) {
    for (size_t i = 1; i < p.size(); ++i) {
        const auto& a = p[i - 1];
        const auto& b = p[i];
        if (a.h > b.h) return false;
        if (a.h == b.h && a.did > b.did) return false;
        if (a.h == b.h && a.did == b.did && a.pos > b.pos) return false;
    }
    return true;
}

ValidationResult validate_segment(const std::filesystem::path& seg_dir, bool check_sorted) {
    ValidationResult vr;
    SegmentData seg;
    std::string err;

    if (!load_segment_bin(seg_dir, seg, &err)) {
        vr.errors.push_back(err);
        vr.ok = false;
        return vr;
    }

    std::vector<DocInfo> docinfo;
    if (!load_docids_json(seg_dir, docinfo, &err)) {
        vr.errors.push_back(err);
    }

    if (docinfo.size() != seg.header.n_docs) {
        std::ostringstream oss;
        oss << "docids size mismatch: docinfo=" << docinfo.size()
            << " header.n_docs=" << seg.header.n_docs;
        vr.errors.push_back(oss.str());
    }

    if (check_sorted && !is_sorted_postings(seg.postings9)) {
        vr.errors.push_back("postings9 is not sorted by (h,did,pos)");
    }

    // did bounds and pos bounds
    for (const auto& p : seg.postings9) {
        if (p.did >= seg.header.n_docs) {
            vr.errors.push_back("posting did out of range");
            break;
        }
        const auto tok_len = seg.docmeta[p.did].tok_len;
        if (tok_len < (uint32_t)K_SHINGLE) {
            vr.errors.push_back("doc tok_len < K (invalid docmeta)");
            break;
        }
        const uint32_t max_pos = tok_len - (uint32_t)K_SHINGLE;
        if (p.pos > max_pos) {
            vr.errors.push_back("posting pos out of range");
            break;
        }
    }

    vr.ok = vr.errors.empty();
    return vr;
}

ValidationResult validate_out_root(const std::filesystem::path& out_root) {
    ValidationResult vr;
    auto m = load_manifest(out_root);
    if (m.segments.empty()) {
        vr.errors.push_back("manifest has no segments (or missing)");
        vr.ok = false;
        return vr;
    }

    for (const auto& s : m.segments) {
        const auto seg_dir = out_root / s.segment_name;
        auto r = validate_segment(seg_dir, true);
        if (!r.ok) {
            for (auto& e : r.errors) {
                vr.errors.push_back(s.segment_name + ": " + e);
            }
        }
    }
    vr.ok = vr.errors.empty();
    return vr;
}

} // namespace l5
