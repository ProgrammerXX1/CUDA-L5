#include "l5/builder.h"
#include "l5/format.h"
#include "l5/manifest.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

#include <simdjson.h>
#include <nlohmann/json.hpp>

#include "l5/errors.h"
#include "text_common.h" // from your common dir

using json = nlohmann::json;
namespace fs = std::filesystem;

namespace l5 {

namespace {

struct ThreadResult {
    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;
    std::vector<Posting9> postings9;
};

static bool env_bool(const char* key, bool defv) {
    const char* s = std::getenv(key);
    if (!s || !*s) return defv;
    if (std::strcmp(s, "1") == 0) return true;
    if (std::strcmp(s, "0") == 0) return false;
    if (std::strcmp(s, "true") == 0 || std::strcmp(s, "TRUE") == 0) return true;
    if (std::strcmp(s, "false") == 0 || std::strcmp(s, "FALSE") == 0) return false;
    return defv;
}

static bool has_key(const simdjson::dom::element& e, const char* key) {
    simdjson::dom::element tmp;
    return !e.at_key(key).get(tmp);
}

static bool get_bool_safe(const simdjson::dom::element& e, const char* key, bool defv) {
    simdjson::dom::element v;
    if (e.at_key(key).get(v)) return defv;
    bool b = defv;
    if (!v.get(b)) return b;
    return defv;
}

static bool get_text_is_normalized(const simdjson::dom::element& doc) {
    const bool strict = env_bool("PLAGIO_STRICT_TEXT_IS_NORMALIZED", false);

    const bool has_new = has_key(doc, "text_is_normalized");
    const bool has_old = has_key(doc, "normalized");

    if (has_new) return get_bool_safe(doc, "text_is_normalized", true);
    if (has_old) return get_bool_safe(doc, "normalized", true);

    if (strict) return false;
    return true;
}

static bool write_text_file_tmp(const fs::path& tmp, const std::string& content) {
    std::ofstream out(tmp, std::ios::binary);
    if (!out) return false;
    out.write(content.data(), (std::streamsize)content.size());
    out.flush();
    return (bool)out;
}

static bool write_json_tmp(const fs::path& tmp, const json& j) {
    return write_text_file_tmp(tmp, j.dump());
}

static void process_range(const std::vector<std::string>& lines,
                          size_t start,
                          size_t end,
                          ThreadResult& out,
                          const BuildOptions& opt) {
    out.docs.clear();
    out.doc_ids.clear();
    out.postings9.clear();

    out.docs.reserve(end - start);
    out.doc_ids.reserve(end - start);
    out.postings9.reserve((end - start) * 256);

    simdjson::dom::parser parser;
    std::vector<TokenSpan> spans;
    spans.reserve(256);

    for (size_t i = start; i < end; ++i) {
        const std::string& line = lines[i];
        if (line.empty()) continue;

        simdjson::dom::element doc;
        if (parser.parse(line).get(doc)) continue;

        std::string_view did_sv;
        if (doc["doc_id"].get(did_sv) || did_sv.empty()) continue;

        std::string_view text_sv;
        if (doc["text"].get(text_sv) || text_sv.empty()) continue;

        const bool text_is_norm = get_text_is_normalized(doc);

        std::string did{did_sv};
        std::string text{text_sv};

        std::string norm;
        if (text_is_norm) norm = std::move(text);
        else norm = normalize_for_shingles_simple(text);

        spans.clear();
        tokenize_spans(norm, spans);
        if (spans.empty()) continue;

        if (opt.max_tokens_per_doc > 0 && spans.size() > (size_t)opt.max_tokens_per_doc) {
            spans.resize((size_t)opt.max_tokens_per_doc);
        }
        if (spans.size() < (size_t)K_SHINGLE) continue;

        const int n = (int)spans.size();
        const int cnt = n - K_SHINGLE + 1;
        if (cnt <= 0) continue;

        auto [hi, lo] = simhash128_spans(norm, spans);

        DocMeta dm{};
        dm.tok_len = (uint32_t)spans.size();
        dm.simhash_hi = hi;
        dm.simhash_lo = lo;

        const uint32_t local_doc_id = (uint32_t)out.docs.size();
        out.docs.push_back(dm);
        out.doc_ids.push_back(std::move(did));

        const int step = (opt.shingle_stride > 0 ? opt.shingle_stride : 1);
        uint32_t produced = 0;
        const uint32_t max_sh =
            (opt.max_shingles_per_doc > 0) ? opt.max_shingles_per_doc : (uint32_t)cnt;

        for (int pos = 0; pos < cnt && produced < max_sh; pos += step) {
            uint64_t h = hash_shingle_tokens_spans(norm, spans, pos, K_SHINGLE);
            out.postings9.push_back(Posting9{h, local_doc_id, (uint32_t)pos});
            ++produced;
        }
    }
}

} // namespace

BuildStats build_segment_jsonl(const fs::path& corpus_jsonl,
                              const fs::path& out_root,
                              const BuildOptions& opt) {
    BuildStats st;

    // segment name
    std::string segment_name = opt.segment_name;
    if (segment_name.empty()) {
        segment_name = std::string("seg_") + utc_now_compact();
    }

    // prepare dirs
    std::error_code ec;
    fs::create_directories(out_root, ec);
    if (ec) throw L5Exception("cannot create out_root: " + out_root.string() + " err=" + ec.message());

    const fs::path seg_dir = out_root / segment_name;
    if (fs::exists(seg_dir)) throw L5Exception("segment already exists: " + seg_dir.string());

    fs::create_directories(seg_dir, ec);
    if (ec) throw L5Exception("cannot create segment dir: " + seg_dir.string() + " err=" + ec.message());

    // read JSONL lines
    std::ifstream in(corpus_jsonl);
    if (!in) throw L5Exception("cannot open corpus: " + corpus_jsonl.string());

    std::vector<std::string> lines;
    lines.reserve(4096);
    {
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) lines.push_back(line);
        }
    }
    if (lines.empty()) throw L5Exception("corpus is empty");

    const size_t total_lines = lines.size();

    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 4;
    unsigned num_threads = std::min<unsigned>(hw, opt.max_threads > 0 ? opt.max_threads : 16u);
    if (num_threads > total_lines) num_threads = (unsigned)total_lines;
    if (num_threads == 0) num_threads = 1;

    std::vector<ThreadResult> results(num_threads);
    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    const size_t chunk_size = (total_lines + num_threads - 1) / num_threads;
    size_t cur_start = 0;

    for (unsigned t = 0; t < num_threads; ++t) {
        const size_t start = cur_start;
        const size_t end = std::min<size_t>(start + chunk_size, total_lines);
        cur_start = end;
        if (start >= end) break;

        workers.emplace_back([&, start, end, t]() {
            process_range(lines, start, end, results[t], opt);
        });
    }

    const unsigned used_threads = (unsigned)workers.size();
    for (auto& th : workers) th.join();

    uint64_t total_docs = 0, total_posts9 = 0;
    for (unsigned t = 0; t < used_threads; ++t) {
        total_docs += results[t].docs.size();
        total_posts9 += results[t].postings9.size();
    }
    if (total_docs == 0) throw L5Exception("no valid docs");

    // offsets
    std::vector<uint32_t> doc_id_offsets(used_threads, 0);
    {
        uint32_t acc = 0;
        for (unsigned t = 0; t < used_threads; ++t) {
            doc_id_offsets[t] = acc;
            acc += (uint32_t)results[t].docs.size();
        }
    }

    // merge docs/docids
    std::vector<DocMeta> docs;
    std::vector<std::string> doc_ids;
    std::vector<Posting9> postings9;

    docs.reserve((size_t)total_docs);
    doc_ids.reserve((size_t)total_docs);
    postings9.reserve((size_t)total_posts9);

    for (unsigned t = 0; t < used_threads; ++t) {
        auto& r = results[t];
        for (size_t i = 0; i < r.docs.size(); ++i) {
            docs.push_back(r.docs[i]);
            doc_ids.push_back(std::move(r.doc_ids[i]));
        }
    }

    for (unsigned t = 0; t < used_threads; ++t) {
        const uint32_t base = doc_id_offsets[t];
        auto& r = results[t];
        for (const auto& p : r.postings9) {
            postings9.push_back(Posting9{p.h, base + p.did, p.pos});
        }
    }

    std::sort(postings9.begin(), postings9.end(), [](const Posting9& a, const Posting9& b) {
        if (a.h != b.h) return a.h < b.h;
        if (a.did != b.did) return a.did < b.did;
        return a.pos < b.pos;
    });

    const uint32_t N_docs = (uint32_t)docs.size();
    const uint64_t N_post9 = (uint64_t)postings9.size();
    const uint64_t N_post13 = 0;

    const fs::path bin_fin  = seg_dir / "index_native.bin";
    const fs::path doc_fin  = seg_dir / "index_native_docids.json";
    const fs::path meta_fin = seg_dir / "index_native_meta.json";

    const fs::path bin_tmp  = seg_dir / "index_native.bin.tmp";
    const fs::path doc_tmp  = seg_dir / "index_native_docids.json.tmp";
    const fs::path meta_tmp = seg_dir / "index_native_meta.json.tmp";

    // 1) bin tmp
    {
        std::ofstream bout(bin_tmp, std::ios::binary);
        if (!bout) throw L5Exception("cannot open " + bin_tmp.string());

        HeaderV2 h{};
        h.magic[0] = 'P'; h.magic[1] = 'L'; h.magic[2] = 'A'; h.magic[3] = 'G';
        h.version = 2;
        h.n_docs = N_docs;
        h.n_post9 = N_post9;
        h.n_post13 = N_post13;

        if (!write_header_v2(bout, h)) throw L5Exception("write header failed");

        for (const auto& dm : docs) {
            bout.write(reinterpret_cast<const char*>(&dm.tok_len), sizeof(dm.tok_len));
            bout.write(reinterpret_cast<const char*>(&dm.simhash_hi), sizeof(dm.simhash_hi));
            bout.write(reinterpret_cast<const char*>(&dm.simhash_lo), sizeof(dm.simhash_lo));
        }

        for (const auto& p : postings9) {
            bout.write(reinterpret_cast<const char*>(&p.h), sizeof(p.h));
            bout.write(reinterpret_cast<const char*>(&p.did), sizeof(p.did));
            bout.write(reinterpret_cast<const char*>(&p.pos), sizeof(p.pos));
        }
        bout.flush();
        if (!bout) throw L5Exception("write failed " + bin_tmp.string());
    }

    // 2) docids tmp
    {
        json j = json::array();
        j = doc_ids;
        if (!write_json_tmp(doc_tmp, j)) throw L5Exception("write failed " + doc_tmp.string());
    }

    // 3) meta tmp
    {
        json j_meta;
        j_meta["segment_name"] = segment_name;
        j_meta["built_at_utc"] = utc_now_compact();
        j_meta["stats"] = {{"docs", N_docs}, {"k9", N_post9}, {"k13", 0}};
        j_meta["strict_text_is_normalized"] = (env_bool("PLAGIO_STRICT_TEXT_IS_NORMALIZED", false) ? 1 : 0);
        if (!write_json_tmp(meta_tmp, j_meta)) throw L5Exception("write failed " + meta_tmp.string());
    }

    if (!atomic_replace_file_best_effort(bin_tmp, bin_fin)) throw L5Exception("atomic replace failed (bin)");
    if (!atomic_replace_file_best_effort(doc_tmp, doc_fin)) throw L5Exception("atomic replace failed (docids)");
    if (!atomic_replace_file_best_effort(meta_tmp, meta_fin)) throw L5Exception("atomic replace failed (meta)");

    // manifest append
    SegmentEntry e;
    e.segment_name = segment_name;
    e.path = segment_name + "/";
    e.built_at_utc = utc_now_compact();
    e.stats.docs = N_docs;
    e.stats.k9 = N_post9;
    e.stats.k13 = 0;

    if (!append_segment_to_manifest(out_root, e)) throw L5Exception("manifest append failed");

    st.segment_name = segment_name;
    st.seg_dir = seg_dir;
    st.docs = N_docs;
    st.post9 = N_post9;
    st.threads = used_threads;
    st.strict_text_is_normalized = env_bool("PLAGIO_STRICT_TEXT_IS_NORMALIZED", false) ? 1 : 0;
    st.built_at_utc = e.built_at_utc;

    return st;
}

} // namespace l5
