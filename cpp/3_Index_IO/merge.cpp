// Back_L5/cpp/src/merge.cpp
#include "l5/merge.h"

#include "l5/errors.h"
#include "l5/format.h"
#include "l5/reader.h"
#include "l5/docinfo.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <queue>
#include <string_view>
#include <system_error>
#include <vector>

namespace fs = std::filesystem;

namespace l5 {

namespace {

static inline void json_write_hex4(std::ostream& os, unsigned char c) {
    static const char* hex = "0123456789abcdef";
    os << "\\u00" << hex[(c >> 4) & 0xF] << hex[c & 0xF];
}

static void json_write_string(std::ostream& os, std::string_view s) {
    os.put('"');
    for (unsigned char c : s) {
        switch (c) {
            case '\\': os << "\\\\"; break;
            case '"':  os << "\\\""; break;
            case '\b': os << "\\b";  break;
            case '\f': os << "\\f";  break;
            case '\n': os << "\\n";  break;
            case '\r': os << "\\r";  break;
            case '\t': os << "\\t";  break;
            default:
                if (c < 0x20) json_write_hex4(os, c);
                else os.put((char)c);
        }
    }
    os.put('"');
}

static constexpr std::streamoff DOCMETA_BYTES = (std::streamoff)DOCMETA_V2_BYTES;

struct SegInfo {
    fs::path seg_dir;
    HeaderV2 h{};
    uint32_t did_offset{0};
};

struct PostingStream {
    std::ifstream in;
    uint64_t remaining{0};
    uint32_t did_offset{0};
    Posting9 cur{};
    bool has{false};

    PostingStream() = default;

    PostingStream(const fs::path& seg_dir, uint32_t did_off) {
        const fs::path bin = seg_dir / "index_native.bin";

        std::error_code ec;
        const uint64_t file_bytes = fs::file_size(bin, ec);
        if (ec) throw L5Exception("file_size failed " + bin.string() + " err=" + ec.message());

        in.open(bin, std::ios::binary);
        if (!in) throw L5Exception("cannot open " + bin.string());

        HeaderV2 h{};
        if (!read_header_v2(in, h)) throw L5Exception("invalid header " + bin.string());

        std::string herr;
        if (!header_v2_sane(h, file_bytes, &herr)) {
            throw L5Exception("insane header " + bin.string() + ": " + herr);
        }

        // after read_header_v2() stream is at docmeta start; skip docmeta relative to current pos
        const uint64_t skip_u64 = (uint64_t)h.n_docs * (uint64_t)DOCMETA_V2_BYTES;
        if (skip_u64 > (uint64_t)std::numeric_limits<std::streamoff>::max()) {
            throw L5Exception("seek overflow (docmeta skip) " + bin.string());
        }
        in.seekg((std::streamoff)skip_u64, std::ios::cur);
        if (!in) throw L5Exception("seek failed " + bin.string());

        remaining = h.n_post9;
        did_offset = did_off;
        next();
    }

    void next() {
        if (remaining == 0) { has = false; return; }

        Posting9 p{};
        in.read(reinterpret_cast<char*>(&p.h), sizeof(p.h));
        in.read(reinterpret_cast<char*>(&p.did), sizeof(p.did));
        in.read(reinterpret_cast<char*>(&p.pos), sizeof(p.pos));
        if (!in) throw L5Exception("read posting failed");

        p.did += did_offset;

        cur = p;
        --remaining;
        has = true;
    }
};

static inline bool post_less(const Posting9& a, const Posting9& b) {
    if (a.h != b.h) return a.h < b.h;
    if (a.did != b.did) return a.did < b.did;
    return a.pos < b.pos;
}

struct HeapItem {
    Posting9 p;
    size_t idx;
};

struct HeapCmp {
    bool operator()(const HeapItem& a, const HeapItem& b) const {
        // min-heap emulation
        return post_less(b.p, a.p);
    }
};

static void copy_docmeta_bytes(const fs::path& seg_dir, uint32_t n_docs, std::ofstream& out) {
    const fs::path bin = seg_dir / "index_native.bin";

    std::error_code ec;
    const uint64_t file_bytes = fs::file_size(bin, ec);
    if (ec) throw L5Exception("file_size failed " + bin.string() + " err=" + ec.message());

    std::ifstream in(bin, std::ios::binary);
    if (!in) throw L5Exception("cannot open " + bin.string());

    HeaderV2 h{};
    if (!read_header_v2(in, h)) throw L5Exception("invalid header " + bin.string());

    std::string herr;
    if (!header_v2_sane(h, file_bytes, &herr)) {
        throw L5Exception("insane header " + bin.string() + ": " + herr);
    }

    if (h.n_docs != n_docs) throw L5Exception("docmeta copy: n_docs mismatch");

    // After read_header_v2(), stream is at docmeta start.
    const uint64_t bytes = (uint64_t)n_docs * (uint64_t)DOCMETA_V2_BYTES;

    std::vector<char> buf(1u << 20); // 1 MiB
    uint64_t left = bytes;

    while (left > 0) {
        const size_t take = (size_t)std::min<uint64_t>(left, (uint64_t)buf.size());
        in.read(buf.data(), (std::streamsize)take);
        if (in.gcount() != (std::streamsize)take) throw L5Exception("docmeta read truncated");
        out.write(buf.data(), (std::streamsize)take);
        if (!out) throw L5Exception("docmeta write failed");
        left -= (uint64_t)take;
    }
}

static void write_docids_merged(const fs::path& out_path,
                                const std::string& new_meta_path,
                                const std::vector<fs::path>& src_seg_dirs) {
    std::ofstream dj(out_path, std::ios::binary);
    if (!dj) throw L5Exception("cannot open docids tmp: " + out_path.string());

    dj.put('[');
    bool first = true;

    for (const auto& seg : src_seg_dirs) {
        std::vector<DocInfo> docs;
        std::string err;
        if (!load_docids_json(seg, docs, &err)) {
            throw L5Exception("load_docids_json failed: " + err);
        }

        for (auto& di : docs) {
            if (!first) dj.put(',');
            first = false;

            dj.put('{');

            dj << "\"doc_id\":";
            json_write_string(dj, di.doc_id);

            dj << ",\"organization_id\":";
            json_write_string(dj, di.organization_id);

            dj << ",\"external_id\":";
            json_write_string(dj, di.external_id.empty() ? di.doc_id : di.external_id);

            dj << ",\"source_path\":";
            json_write_string(dj, di.source_path);

            dj << ",\"source_name\":";
            json_write_string(dj, di.source_name);

            dj << ",\"meta_path\":";
            json_write_string(dj, new_meta_path);

            dj << ",\"preview_text\":";
            json_write_string(dj, di.preview_text);

            dj.put('}');
        }
    }

    dj.put(']');
    dj.flush();
    if (!dj) throw L5Exception("docids write failed");
}

} // namespace

SegmentEntry merge_segments_sorted(const fs::path& dst_root,
                                  const std::string& new_segment_name,
                                  const std::vector<fs::path>& src_seg_dirs) {
    if (src_seg_dirs.empty()) throw L5Exception("merge: empty inputs");

    std::error_code ec;
    fs::create_directories(dst_root, ec);

    const fs::path seg_dir = dst_root / new_segment_name;
    if (fs::exists(seg_dir)) throw L5Exception("merge: segment exists: " + seg_dir.string());

    fs::create_directories(seg_dir, ec);
    if (ec) throw L5Exception("merge: cannot create seg dir: " + ec.message());

    // final files
    const fs::path bin_fin  = seg_dir / "index_native.bin";
    const fs::path doc_fin  = seg_dir / "index_native_docids.json";
    const fs::path meta_fin = seg_dir / "index_native_meta.json";

    // temp files
    const fs::path bin_tmp  = seg_dir / "index_native.bin.tmp";
    const fs::path doc_tmp  = seg_dir / "index_native_docids.json.tmp";
    const fs::path meta_tmp = seg_dir / "index_native_meta.json.tmp";

    // read headers, compute did offsets and totals
    std::vector<SegInfo> infos;
    infos.reserve(src_seg_dirs.size());

    uint32_t did_off = 0;
    uint64_t total_post9 = 0;

    for (const auto& sdir : src_seg_dirs) {
        const fs::path bin = sdir / "index_native.bin";

        std::error_code ec_sz;
        const uint64_t file_bytes = fs::file_size(bin, ec_sz);
        if (ec_sz) throw L5Exception("merge: file_size failed " + bin.string() + " err=" + ec_sz.message());

        std::ifstream in(bin, std::ios::binary);
        if (!in) throw L5Exception("merge: cannot open " + bin.string());

        HeaderV2 h{};
        if (!read_header_v2(in, h)) throw L5Exception("merge: invalid header " + bin.string());

        std::string herr;
        if (!header_v2_sane(h, file_bytes, &herr)) {
            throw L5Exception("merge: insane header " + bin.string() + ": " + herr);
        }

        // did_off is uint32 -> MUST NOT overflow
        const uint64_t did_next = (uint64_t)did_off + (uint64_t)h.n_docs;
        if (did_next > (uint64_t)std::numeric_limits<uint32_t>::max()) {
            throw L5Exception("merge: total_docs overflow uint32");
        }

        // total_post9 is uint64 -> protect overflow
        if (total_post9 > std::numeric_limits<uint64_t>::max() - h.n_post9) {
            throw L5Exception("merge: total_post9 overflow uint64");
        }

        SegInfo si;
        si.seg_dir = sdir;
        si.h = h;
        si.did_offset = did_off;

        did_off = (uint32_t)did_next;
        total_post9 += h.n_post9;

        infos.push_back(std::move(si));
    }

    const uint32_t total_docs = did_off;
    if (total_docs == 0) throw L5Exception("merge: total_docs=0");

    const std::string built_at = utc_now_compact();

    // write docids.tmp (meta_path rewritten)
    const std::string meta_path = new_segment_name + "/";
    write_docids_merged(doc_tmp, meta_path, src_seg_dirs);

    // write meta.tmp
    {
        std::ofstream m(meta_tmp, std::ios::binary);
        if (!m) throw L5Exception("cannot open meta tmp: " + meta_tmp.string());

        m.put('{');
        m << "\"segment_name\":";
        json_write_string(m, new_segment_name);
        m << ",\"built_at_utc\":";
        json_write_string(m, built_at);
        m << ",\"stats\":{";
        m << "\"docs\":" << total_docs << ",\"k9\":" << total_post9 << ",\"k13\":0";
        m << "}";
        m << ",\"strict_text_is_normalized\":0";
        m.put('}');
        m.flush();
        if (!m) throw L5Exception("meta write failed");
    }

    // write bin.tmp: header + docmeta concat + postings k-way merge
    {
        std::ofstream out(bin_tmp, std::ios::binary);
        if (!out) throw L5Exception("cannot open " + bin_tmp.string());

        HeaderV2 h{};
        h.magic[0] = 'P'; h.magic[1] = 'L'; h.magic[2] = 'A'; h.magic[3] = 'G';
        h.version = 2;
        h.n_docs = total_docs;
        h.n_post9 = total_post9;
        h.n_post13 = 0;

        if (!write_header_v2(out, h)) throw L5Exception("merge: write header failed");

        // concat docmeta bytes in the same order as src segments
        for (const auto& si : infos) {
            copy_docmeta_bytes(si.seg_dir, si.h.n_docs, out);
        }

        // postings streams
        std::vector<std::unique_ptr<PostingStream>> streams;
        streams.reserve(infos.size());
        for (const auto& si : infos) {
            streams.emplace_back(std::make_unique<PostingStream>(si.seg_dir, si.did_offset));
        }

        std::priority_queue<HeapItem, std::vector<HeapItem>, HeapCmp> pq;
        for (size_t i = 0; i < streams.size(); ++i) {
            if (streams[i]->has) pq.push(HeapItem{streams[i]->cur, i});
        }

        std::vector<Posting9> buf;
        buf.reserve(1u << 16);

        while (!pq.empty()) {
            auto it = pq.top();
            pq.pop();

            buf.push_back(it.p);
            if (buf.size() >= (1u << 16)) {
                for (const auto& p : buf) {
                    out.write(reinterpret_cast<const char*>(&p.h), sizeof(p.h));
                    out.write(reinterpret_cast<const char*>(&p.did), sizeof(p.did));
                    out.write(reinterpret_cast<const char*>(&p.pos), sizeof(p.pos));
                }
                if (!out) throw L5Exception("merge: postings write failed");
                buf.clear();
            }

            auto& st = streams[it.idx];
            st->next();
            if (st->has) pq.push(HeapItem{st->cur, it.idx});
        }

        if (!buf.empty()) {
            for (const auto& p : buf) {
                out.write(reinterpret_cast<const char*>(&p.h), sizeof(p.h));
                out.write(reinterpret_cast<const char*>(&p.did), sizeof(p.did));
                out.write(reinterpret_cast<const char*>(&p.pos), sizeof(p.pos));
            }
            if (!out) throw L5Exception("merge: postings write failed");
        }

        out.flush();
        if (!out) throw L5Exception("merge: bin write failed");
    }

    // atomic replace final files
    if (!atomic_replace_file_best_effort(bin_tmp, bin_fin))  throw L5Exception("merge: atomic replace failed (bin)");
    if (!atomic_replace_file_best_effort(doc_tmp, doc_fin))  throw L5Exception("merge: atomic replace failed (docids)");
    if (!atomic_replace_file_best_effort(meta_tmp, meta_fin)) throw L5Exception("merge: atomic replace failed (meta)");

    SegmentEntry e;
    e.segment_name = new_segment_name;
    e.path = new_segment_name + "/";
    e.built_at_utc = built_at;
    e.stats.docs = total_docs;
    e.stats.k9 = total_post9;
    e.stats.k13 = 0;

    return e;
}

} // namespace l5
