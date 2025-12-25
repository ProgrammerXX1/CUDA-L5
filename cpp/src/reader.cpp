#include "l5/reader.h"

#include <fstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace l5 {

bool load_segment_bin(const std::filesystem::path& seg_dir, SegmentData& out, std::string* err) {
    out = SegmentData{};
    out.seg_dir = seg_dir;

    const auto bin = seg_dir / "index_native.bin";
    std::ifstream in(bin, std::ios::binary);
    if (!in) {
        if (err) *err = "cannot open " + bin.string();
        return false;
    }

    HeaderV2 h{};
    if (!read_header_v2(in, h)) {
        if (err) *err = "invalid header or version in " + bin.string();
        return false;
    }
    out.header = h;

    out.docmeta.resize(h.n_docs);
    for (uint32_t i = 0; i < h.n_docs; ++i) {
        DocMeta dm{};
        in.read(reinterpret_cast<char*>(&dm.tok_len), sizeof(dm.tok_len));
        in.read(reinterpret_cast<char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
        in.read(reinterpret_cast<char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
        if (!in) {
            if (err) *err = "failed reading docmeta";
            return false;
        }
        out.docmeta[i] = dm;
    }

    out.postings9.resize((size_t)h.n_post9);
    for (uint64_t i = 0; i < h.n_post9; ++i) {
        Posting9 p{};
        in.read(reinterpret_cast<char*>(&p.h), sizeof(p.h));
        in.read(reinterpret_cast<char*>(&p.did), sizeof(p.did));
        in.read(reinterpret_cast<char*>(&p.pos), sizeof(p.pos));
        if (!in) {
            if (err) *err = "failed reading postings9";
            return false;
        }
        out.postings9[(size_t)i] = p;
    }

    return true;
}

bool load_docids_json(const std::filesystem::path& seg_dir,
                      std::vector<std::string>& docids,
                      std::string* err) {
    docids.clear();
    const auto p = seg_dir / "index_native_docids.json";
    std::ifstream in(p);
    if (!in) {
        if (err) *err = "cannot open " + p.string();
        return false;
    }
    json j;
    try {
        in >> j;
    } catch (...) {
        if (err) *err = "failed parsing " + p.string();
        return false;
    }
    if (!j.is_array()) {
        if (err) *err = "docids json is not array";
        return false;
    }
    docids.reserve(j.size());
    for (auto& v : j) {
        if (!v.is_string()) continue;
        docids.push_back(v.get<std::string>());
    }
    return true;
}

} // namespace l5
