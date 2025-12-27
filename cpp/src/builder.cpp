// Back_L5/cpp/src/builder.cpp
#include "l5/builder.h"
#include "l5/format.h"
#include "l5/manifest.h"
#include "l5/docinfo.h"
#include "l5/errors.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <simdjson.h>

#include "text_common.h"

namespace fs = std::filesystem;

namespace l5 {

namespace {

// --------------------
// bounded queue (streaming pipeline)
// --------------------
template <class T>
class BoundedQueue {
public:
    explicit BoundedQueue(size_t cap) : cap_(cap) {}

    bool push(T&& v) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_not_full_.wait(lk, [&] { return closed_ || q_.size() < cap_; });
        if (closed_) return false;
        q_.push_back(std::move(v));
        cv_not_empty_.notify_one();
        return true;
    }

    bool pop(T& out) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_not_empty_.wait(lk, [&] { return closed_ || !q_.empty(); });
        if (q_.empty()) return false; // closed and empty
        out = std::move(q_.front());
        q_.pop_front();
        cv_not_full_.notify_one();
        return true;
    }

    void close() {
        std::lock_guard<std::mutex> lk(mu_);
        closed_ = true;
        cv_not_empty_.notify_all();
        cv_not_full_.notify_all();
    }

private:
    size_t cap_{0};
    std::mutex mu_;
    std::condition_variable cv_not_empty_;
    std::condition_variable cv_not_full_;
    std::deque<T> q_;
    bool closed_{false};
};

// --------------------
// helpers / limits / parsing
// --------------------

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

static bool get_text_is_normalized(const simdjson::dom::element& doc, bool strict) {
    const bool has_new = has_key(doc, "text_is_normalized");
    const bool has_old = has_key(doc, "normalized");

    if (has_new) return get_bool_safe(doc, "text_is_normalized", true);
    if (has_old) return get_bool_safe(doc, "normalized", true);

    if (strict) return false;
    return true;
}

static std::string_view get_sv_or_empty(const simdjson::dom::element& doc, const char* key) {
    std::string_view sv{};
    auto err = doc.at_key(key).get(sv);
    if (err) return std::string_view{};
    return sv;
}

// UTF-8 safe prefix boundary
static size_t utf8_safe_prefix_len(std::string_view s, size_t max_bytes) {
    const size_t n = std::min(max_bytes, s.size());
    size_t i = 0;
    size_t last_good = 0;

    while (i < n) {
        const unsigned char c = (unsigned char)s[i];

        size_t len = 1;
        if (c < 0x80) len = 1;
        else if (c >= 0xC2 && c <= 0xDF) len = 2;
        else if (c >= 0xE0 && c <= 0xEF) len = 3;
        else if (c >= 0xF0 && c <= 0xF4) len = 4;
        else break;

        if (i + len > n) break;

        bool ok = true;
        for (size_t j = 1; j < len; ++j) {
            const unsigned char cc = (unsigned char)s[i + j];
            if ((cc & 0xC0) != 0x80) { ok = false; break; }
        }
        if (!ok) break;

        i += len;
        last_good = i;
    }
    return last_good;
}

static inline std::string_view clip_text_view(std::string_view s, uint32_t max_bytes, bool text_is_norm) {
    if (max_bytes == 0) return s;
    if (s.size() <= (size_t)max_bytes) return s;

    if (text_is_norm) {
        const size_t cut = utf8_safe_prefix_len(s, (size_t)max_bytes);
        return s.substr(0, cut);
    }
    // не нормализован: можно резать по байтам, normalize_for_shingles_simple_to переживёт
    return s.substr(0, (size_t)max_bytes);
}

// --------------------
// JSON streaming writer (docids.json)
// --------------------

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

// --------------------
// Posting record for intermediate + sort
// --------------------
struct P9 {
    uint64_t h;
    uint32_t did;
    uint32_t pos;
};
static_assert(sizeof(P9) == 16, "P9 must be 16 bytes");

// comparator by (h,did,pos)
static inline bool p9_less(const P9& a, const P9& b) {
    if (a.h != b.h) return a.h < b.h;
    if (a.did != b.did) return a.did < b.did;
    return a.pos < b.pos;
}

// O(N) radix sort for vector<P9> by (h,did,pos), LSD stable counting sort by bytes.
static void radix_sort_p9(std::vector<P9>& a, std::vector<P9>& tmp) {
    if (a.size() <= 1) return;
    tmp.resize(a.size());

    auto pass_byte = [&](auto get_byte) {
        std::array<size_t, 256> cnt{};
        for (const auto& x : a) ++cnt[(size_t)get_byte(x)];
        std::array<size_t, 256> off{};
        size_t sum = 0;
        for (size_t i = 0; i < 256; ++i) {
            off[i] = sum;
            sum += cnt[i];
        }
        for (const auto& x : a) {
            const unsigned char b = (unsigned char)get_byte(x);
            tmp[off[(size_t)b]++] = x;
        }
        a.swap(tmp);
    };

    // pos (32)
    for (int sh = 0; sh < 32; sh += 8) {
        pass_byte([&](const P9& x) -> unsigned char { return (unsigned char)((x.pos >> sh) & 0xFF); });
    }
    // did (32)
    for (int sh = 0; sh < 32; sh += 8) {
        pass_byte([&](const P9& x) -> unsigned char { return (unsigned char)((x.did >> sh) & 0xFF); });
    }
    // h (64)
    for (int sh = 0; sh < 64; sh += 8) {
        pass_byte([&](const P9& x) -> unsigned char { return (unsigned char)((x.h >> sh) & 0xFF); });
    }
}

// read chunk of P9 from stream (binary 16B records)
static size_t read_p9_chunk(std::ifstream& in, std::vector<P9>& buf, size_t max_recs) {
    buf.resize(max_recs);
    in.read(reinterpret_cast<char*>(buf.data()), (std::streamsize)(max_recs * sizeof(P9)));
    const std::streamsize got = in.gcount();
    const size_t recs = (size_t)(got / (std::streamsize)sizeof(P9));
    buf.resize(recs);
    return recs;
}

// write vector<P9> raw
static void write_p9_vec(std::ofstream& out, const std::vector<P9>& v) {
    if (v.empty()) return;
    out.write(reinterpret_cast<const char*>(v.data()), (std::streamsize)(v.size() * sizeof(P9)));
}

// merge runs -> file or stream
struct RunReader {
    std::ifstream in;
    P9 cur{};
    bool has{false};

    explicit RunReader(const fs::path& p) {
        in.open(p, std::ios::binary);
        if (!in) throw L5Exception("cannot open run: " + p.string());
        next();
    }
    void next() {
        in.read(reinterpret_cast<char*>(&cur), (std::streamsize)sizeof(P9));
        has = (in.gcount() == (std::streamsize)sizeof(P9));
    }
};

struct HeapItem {
    P9 p;
    size_t ridx;
};

struct HeapCmp {
    bool operator()(const HeapItem& a, const HeapItem& b) const {
        // min-heap via priority_queue (which is max-heap by default)
        return p9_less(b.p, a.p);
    }
};

static void merge_runs_to_stream(const std::vector<fs::path>& runs, std::ofstream& out) {
    std::vector<std::unique_ptr<RunReader>> rr;
    rr.reserve(runs.size());

    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapCmp> pq;

    for (size_t i = 0; i < runs.size(); ++i) {
        rr.emplace_back(std::make_unique<RunReader>(runs[i]));
        if (rr.back()->has) pq.push(HeapItem{rr.back()->cur, i});
    }

    std::vector<P9> outbuf;
    outbuf.reserve(1u << 16);

    while (!pq.empty()) {
        auto it = pq.top();
        pq.pop();

        outbuf.push_back(it.p);
        if (outbuf.size() >= (1u << 16)) {
            write_p9_vec(out, outbuf);
            outbuf.clear();
        }

        auto& r = rr[it.ridx];
        r->next();
        if (r->has) pq.push(HeapItem{r->cur, it.ridx});
    }

    if (!outbuf.empty()) write_p9_vec(out, outbuf);
}

static void merge_runs_to_file(const std::vector<fs::path>& runs, const fs::path& out_path) {
    std::ofstream out(out_path, std::ios::binary);
    if (!out) throw L5Exception("cannot open merge out: " + out_path.string());
    merge_runs_to_stream(runs, out);
    out.flush();
    if (!out) throw L5Exception("merge write failed: " + out_path.string());
}

// --------------------
// build pipeline structures
// --------------------
struct DocResult {
    uint32_t did{0};
    DocMeta meta{};

    std::string doc_id;
    std::string organization_id;
    std::string external_id;
    std::string source_path;
    std::string source_name;
    std::string preview_text;
};

struct SegCleanupOnFail {
    fs::path p;
    bool keep{false};
    ~SegCleanupOnFail() {
        if (keep) return;
        std::error_code ec;
        fs::remove_all(p, ec);
    }
};

// acquire did with max_docs cap (lock-free)
static bool acquire_did(std::atomic<uint32_t>& next_did, uint32_t max_docs, uint32_t& out_did) {
    if (max_docs == 0) {
        out_did = next_did.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    uint32_t cur = next_did.load(std::memory_order_relaxed);
    while (true) {
        if (cur >= max_docs) return false;
        if (next_did.compare_exchange_weak(cur, cur + 1, std::memory_order_relaxed)) {
            out_did = cur;
            return true;
        }
        // cur updated by compare_exchange_weak
    }
}

// partition postings by top byte of hash
static void partition_postings_to_buckets(const std::vector<fs::path>& inputs,
                                         const fs::path& bucket_dir) {
    fs::create_directories(bucket_dir);

    constexpr size_t BUCKETS = 256;
    constexpr size_t BUF_CAP = 4096;
    constexpr size_t BLOCK_RECS = 1u << 16; // 65536 (1MB)

    std::array<std::ofstream, BUCKETS> outs;
    for (size_t b = 0; b < BUCKETS; ++b) {
        char name[32];
        std::snprintf(name, sizeof(name), "b_%02X.bin", (unsigned)b);
        const fs::path p = bucket_dir / name;
        outs[b].open(p, std::ios::binary);
        if (!outs[b]) throw L5Exception("cannot open bucket: " + p.string());
    }

    std::array<std::vector<P9>, BUCKETS> buf;
    for (size_t b = 0; b < BUCKETS; ++b) buf[b].reserve(BUF_CAP);

    std::vector<P9> block;
    block.reserve(BLOCK_RECS);

    for (const auto& in_path : inputs) {
        std::ifstream in(in_path, std::ios::binary);
        if (!in) continue;

        while (true) {
            const size_t got = read_p9_chunk(in, block, BLOCK_RECS);
            if (got == 0) break;

            for (size_t i = 0; i < got; ++i) {
                const P9& p = block[i];
                const unsigned b = (unsigned)((p.h >> 56) & 0xFF);
                auto& v = buf[b];
                v.push_back(p);
                if (v.size() >= BUF_CAP) {
                    write_p9_vec(outs[b], v);
                    v.clear();
                }
            }
        }

        in.close();
        std::error_code ec;
        fs::remove(in_path, ec); // cleanup worker file
    }

    for (size_t b = 0; b < BUCKETS; ++b) {
        if (!buf[b].empty()) {
            write_p9_vec(outs[b], buf[b]);
            buf[b].clear();
        }
        outs[b].flush();
        if (!outs[b]) throw L5Exception("bucket write failed");
        outs[b].close();
    }
}

// sort one bucket -> append to index stream (bounded RAM)
static void sort_bucket_append_to_index(const fs::path& bucket_path,
                                        std::ofstream& index_out,
                                        const fs::path& tmp_dir,
                                        uint64_t ram_limit_bytes,
                                        unsigned bucket_id) {
    std::error_code ec;
    const uint64_t bytes = fs::exists(bucket_path, ec) ? (uint64_t)fs::file_size(bucket_path, ec) : 0;
    if (ec || bytes == 0) return;

    const uint64_t total_recs = bytes / (uint64_t)sizeof(P9);
    if (total_recs == 0) return;

    // memory budget for vector + tmp (radix sort uses 2 arrays)
    const uint64_t max_recs = std::max<uint64_t>(1ull, ram_limit_bytes / (uint64_t)(2 * sizeof(P9)));
    const size_t chunk_recs = (size_t)std::min<uint64_t>(total_recs, max_recs);

    // fits in-memory => radix sort O(N), write
    if (total_recs <= (uint64_t)chunk_recs) {
        std::vector<P9> a((size_t)total_recs);
        std::vector<P9> tmp;

        std::ifstream in(bucket_path, std::ios::binary);
        if (!in) throw L5Exception("cannot open bucket for read: " + bucket_path.string());
        in.read(reinterpret_cast<char*>(a.data()), (std::streamsize)(a.size() * sizeof(P9)));
        if (in.gcount() != (std::streamsize)(a.size() * sizeof(P9))) {
            throw L5Exception("bucket read truncated: " + bucket_path.string());
        }

        radix_sort_p9(a, tmp);
        write_p9_vec(index_out, a);

        std::error_code ec2;
        fs::remove(bucket_path, ec2);
        return;
    }

    // external sort: runs + k-way merge (bounded RAM)
    fs::create_directories(tmp_dir);

    std::vector<fs::path> runs;
    runs.reserve((size_t)((total_recs + chunk_recs - 1) / chunk_recs));

    std::ifstream in(bucket_path, std::ios::binary);
    if (!in) throw L5Exception("cannot open bucket for read: " + bucket_path.string());

    std::vector<P9> a;
    std::vector<P9> tmp;
    a.reserve(chunk_recs);

    size_t run_idx = 0;
    while (true) {
        const size_t got = read_p9_chunk(in, a, chunk_recs);
        if (got == 0) break;

        radix_sort_p9(a, tmp);

        char rn[64];
        std::snprintf(rn, sizeof(rn), "b_%02X_run_%06zu.bin", bucket_id, run_idx++);
        fs::path run_path = tmp_dir / rn;

        std::ofstream ro(run_path, std::ios::binary);
        if (!ro) throw L5Exception("cannot open run for write: " + run_path.string());
        write_p9_vec(ro, a);
        ro.flush();
        if (!ro) throw L5Exception("run write failed: " + run_path.string());

        runs.push_back(run_path);
    }

    in.close();
    {
        std::error_code ec2;
        fs::remove(bucket_path, ec2);
    }

    if (runs.empty()) return;

    // reduce number of runs to keep file-descriptors bounded
    constexpr size_t FANIN = 64;
    int stage = 0;

    while (runs.size() > FANIN) {
        std::vector<fs::path> new_runs;
        new_runs.reserve((runs.size() + FANIN - 1) / FANIN);

        for (size_t i = 0; i < runs.size(); i += FANIN) {
            const size_t j = std::min(runs.size(), i + FANIN);
            std::vector<fs::path> group;
            group.reserve(j - i);
            for (size_t k = i; k < j; ++k) group.push_back(runs[k]);

            char mn[64];
            std::snprintf(mn, sizeof(mn), "b_%02X_merge_%02d_%06zu.bin", bucket_id, stage, new_runs.size());
            fs::path merged_path = tmp_dir / mn;

            merge_runs_to_file(group, merged_path);

            // cleanup old group runs
            for (const auto& p : group) {
                std::error_code ec3;
                fs::remove(p, ec3);
            }

            new_runs.push_back(merged_path);
        }

        runs.swap(new_runs);
        ++stage;
    }

    // final merge directly into index stream
    merge_runs_to_stream(runs, index_out);

    // cleanup remaining runs
    for (const auto& p : runs) {
        std::error_code ec3;
        fs::remove(p, ec3);
    }
}

} // namespace

BuildStats build_segment_jsonl(const fs::path& corpus_jsonl,
                              const fs::path& out_root,
                              const BuildOptions& opt_in) {
    BuildOptions opt = opt_in;

    BuildStats st;

    std::string segment_name = opt.segment_name;
    if (segment_name.empty()) segment_name = std::string("seg_") + utc_now_compact();

    const bool strict = opt.strict_text_is_normalized || env_bool("PLAGIO_STRICT_TEXT_IS_NORMALIZED", false);
    const std::string built_at = utc_now_compact();

    std::error_code ec;
    fs::create_directories(out_root, ec);
    if (ec) throw L5Exception("cannot create out_root: " + out_root.string() + " err=" + ec.message());

    const fs::path seg_dir = out_root / segment_name;
    if (fs::exists(seg_dir)) throw L5Exception("segment already exists: " + seg_dir.string());

    fs::create_directories(seg_dir, ec);
    if (ec) throw L5Exception("cannot create segment dir: " + seg_dir.string() + " err=" + ec.message());

    SegCleanupOnFail cleanup{seg_dir};

    // derive threads / inflight bounds
    unsigned hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 4;
    unsigned max_thr = (opt.max_threads > 0 ? opt.max_threads : 16u);
    unsigned num_threads = std::min<unsigned>(hw, max_thr);
    if (num_threads == 0) num_threads = 1;

    if (opt.inflight_docs == 0) {
        opt.inflight_docs = std::max<uint32_t>(32u, (uint32_t)(num_threads * 4u));
    }

    // temp paths
    const fs::path bin_fin  = seg_dir / "index_native.bin";
    const fs::path doc_fin  = seg_dir / "index_native_docids.json";
    const fs::path meta_fin = seg_dir / "index_native_meta.json";

    const fs::path bin_tmp  = seg_dir / "index_native.bin.tmp";
    const fs::path doc_tmp  = seg_dir / "index_native_docids.json.tmp";
    const fs::path meta_tmp = seg_dir / "index_native_meta.json.tmp";

    const fs::path docmeta_tmp = seg_dir / "index_native_docmeta.bin.tmp";

    const fs::path tmp_dir = seg_dir / "_tmp_build";
    fs::create_directories(tmp_dir, ec);

    // postings worker files
    std::vector<fs::path> postings_files;
    postings_files.reserve(num_threads);
    for (unsigned t = 0; t < num_threads; ++t) {
        char nm[64];
        std::snprintf(nm, sizeof(nm), "postings_unsorted_%02u.bin", t);
        postings_files.push_back(tmp_dir / nm);
    }

    // bounded queues
    BoundedQueue<std::string> q_lines(opt.inflight_docs);
    BoundedQueue<DocResult>   q_docs(opt.inflight_docs);

    std::atomic<uint32_t> next_did{0};
    std::atomic<bool> stop{false};

    std::vector<std::atomic<uint64_t>> postings_written(num_threads);
    for (auto& x : postings_written) x.store(0);

    // writer thread: docmeta + docids.json streaming in did order
    std::atomic<uint32_t> docs_written{0};

    std::thread writer([&](){
        std::ofstream dm(docmeta_tmp, std::ios::binary);
        if (!dm) throw L5Exception("cannot open docmeta tmp: " + docmeta_tmp.string());

        std::ofstream dj(doc_tmp, std::ios::binary);
        if (!dj) throw L5Exception("cannot open docids tmp: " + doc_tmp.string());

        dj.put('[');
        bool first = true;

        uint32_t expect = 0;
        std::unordered_map<uint32_t, DocResult> pending;
        pending.reserve((size_t)opt.inflight_docs * 2);

        DocResult r;
        while (q_docs.pop(r)) {
            pending.emplace(r.did, std::move(r));

            while (true) {
                auto it = pending.find(expect);
                if (it == pending.end()) break;

                DocResult cur = std::move(it->second);
                pending.erase(it);

                // docmeta: write by fields (padding-safe)
                dm.write(reinterpret_cast<const char*>(&cur.meta.tok_len), sizeof(cur.meta.tok_len));
                dm.write(reinterpret_cast<const char*>(&cur.meta.simhash_hi), sizeof(cur.meta.simhash_hi));
                dm.write(reinterpret_cast<const char*>(&cur.meta.simhash_lo), sizeof(cur.meta.simhash_lo));

                // docids JSON object (stream)
                if (!first) dj.put(',');
                first = false;

                dj.put('{');

                dj << "\"doc_id\":";
                json_write_string(dj, cur.doc_id);

                dj << ",\"organization_id\":";
                json_write_string(dj, cur.organization_id);

                dj << ",\"external_id\":";
                json_write_string(dj, cur.external_id.empty() ? cur.doc_id : cur.external_id);

                dj << ",\"source_path\":";
                json_write_string(dj, cur.source_path);

                dj << ",\"source_name\":";
                json_write_string(dj, cur.source_name);

                dj << ",\"meta_path\":";
                std::string mp = segment_name + "/";
                json_write_string(dj, mp);

                dj << ",\"preview_text\":";
                json_write_string(dj, cur.preview_text);

                dj.put('}');

                ++expect;
            }
        }

        // close JSON array
        dj.put(']');
        dj.flush();
        dm.flush();

        if (!dj) throw L5Exception("docids write failed");
        if (!dm) throw L5Exception("docmeta write failed");

        docs_written.store(expect, std::memory_order_relaxed);
    });

    // worker threads
    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    for (unsigned t = 0; t < num_threads; ++t) {
        workers.emplace_back([&, t](){
            simdjson::dom::parser parser;

            std::vector<TokenSpan> spans;
            spans.reserve(512);

            std::vector<uint64_t> token_hashes;
            token_hashes.reserve(512);

            std::string norm;
            norm.reserve(8 * 1024);

            std::ofstream post_out(postings_files[t], std::ios::binary);
            if (!post_out) throw L5Exception("cannot open postings tmp: " + postings_files[t].string());

            std::string line;
            while (q_lines.pop(line)) {
                if (stop.load(std::memory_order_relaxed)) {
                    continue;
                }
                if (line.empty()) continue;

                simdjson::dom::element doc;
                if (parser.parse(line).get(doc)) continue;

                std::string_view did_sv;
                if (doc["doc_id"].get(did_sv) || did_sv.empty()) continue;

                std::string_view text_sv;
                if (doc["text"].get(text_sv) || text_sv.empty()) continue;

                const bool text_is_norm = get_text_is_normalized(doc, strict);

                // apply text byte cap (degrade: truncate)
                text_sv = clip_text_view(text_sv, opt.max_text_bytes_per_doc, text_is_norm);

                // optional fields
                std::string_view ext_sv      = get_sv_or_empty(doc, "external_id");
                std::string_view org_sv      = get_sv_or_empty(doc, "organization_id");
                std::string_view src_path_sv = get_sv_or_empty(doc, "source_path");
                std::string_view src_name_sv = get_sv_or_empty(doc, "source_name");

                // normalize
                if (text_is_norm) {
                    norm.assign(text_sv.data(), text_sv.size());
                } else {
                    normalize_for_shingles_simple_to(text_sv, norm);
                }

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

                uint32_t did = 0;
                if (!acquire_did(next_did, opt.max_docs_in_segment, did)) {
                    stop.store(true, std::memory_order_relaxed);
                    continue;
                }

                // hashes + simhash
                hash_tokens_bytes_spans(norm, spans, token_hashes);
                auto [hi, lo] = simhash128_token_hashes(token_hashes);

                DocResult r;
                r.did = did;
                r.meta.tok_len = (uint32_t)spans.size();
                r.meta.simhash_hi = hi;
                r.meta.simhash_lo = lo;

                r.doc_id = std::string(did_sv);
                r.external_id = ext_sv.empty() ? r.doc_id : std::string(ext_sv);
                r.organization_id = org_sv.empty() ? std::string() : std::string(org_sv);
                r.source_path = src_path_sv.empty() ? std::string() : std::string(src_path_sv);
                r.source_name = src_name_sv.empty() ? std::string() : std::string(src_name_sv);

                // preview_text (<=240 bytes, UTF-8 safe)
                {
                    constexpr size_t PREV = 240;
                    if (norm.size() <= PREV) {
                        r.preview_text = norm;
                    } else {
                        const size_t cut = utf8_safe_prefix_len(norm, PREV);
                        r.preview_text.assign(norm.data(), cut);
                    }
                }

                // postings (streaming, per-thread file)
                const int step = (opt.shingle_stride > 0 ? opt.shingle_stride : 1);
                uint32_t produced = 0;
                const uint32_t max_sh =
                    (opt.max_shingles_per_doc > 0) ? opt.max_shingles_per_doc : (uint32_t)cnt;

                uint64_t local_posts = 0;

                for (int pos = 0; pos < cnt && produced < max_sh; pos += step) {
                    const uint64_t h = hash_shingle_token_hashes(token_hashes, pos, K_SHINGLE);
                    P9 p{h, did, (uint32_t)pos};
                    post_out.write(reinterpret_cast<const char*>(&p), (std::streamsize)sizeof(P9));
                    ++produced;
                    ++local_posts;
                }

                postings_written[t].fetch_add(local_posts, std::memory_order_relaxed);

                // send docmeta/docinfo to writer
                if (!q_docs.push(std::move(r))) {
                    // if writer closed unexpectedly -> stop
                    stop.store(true, std::memory_order_relaxed);
                }
            }

            post_out.flush();
            if (!post_out) throw L5Exception("postings write failed: " + postings_files[t].string());
        });
    }

    // reader thread (streaming JSONL)
    std::thread reader([&](){
        std::ifstream in(corpus_jsonl, std::ios::binary);
        if (!in) throw L5Exception("cannot open corpus: " + corpus_jsonl.string());

        std::string line;
        line.reserve(64 * 1024);

        // rough safety cap for line size (corpus produced by our service)
        const size_t max_line = (size_t)std::max<uint32_t>(opt.max_text_bytes_per_doc + 1024u * 1024u, 2u * 1024u * 1024u);

        while (std::getline(in, line)) {
            if (stop.load(std::memory_order_relaxed)) break;
            if (line.empty()) continue;

            if (line.size() > max_line) {
                // деградация: пропускаем слишком длинную строку (не держим в очереди)
                continue;
            }

            if (!q_lines.push(std::move(line))) break;
            line.clear();
        }

        q_lines.close();
    });

    // join pipeline
    reader.join();
    for (auto& th : workers) th.join();

    q_docs.close();
    writer.join();

    const uint32_t N_docs = docs_written.load(std::memory_order_relaxed);
    if (N_docs == 0) throw L5Exception("no valid docs");

    uint64_t N_post9 = 0;
    for (unsigned t = 0; t < num_threads; ++t) N_post9 += postings_written[t].load(std::memory_order_relaxed);

    // docmeta sanity: expected size = N_docs * 20 bytes
    {
        std::error_code ec2;
        const uint64_t dm_bytes = (uint64_t)fs::file_size(docmeta_tmp, ec2);
        if (!ec2) {
            const uint64_t expect = (uint64_t)N_docs * (uint64_t)(sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint64_t));
            if (dm_bytes != expect) {
                throw L5Exception("docmeta size mismatch: got=" + std::to_string(dm_bytes) +
                                  " expect=" + std::to_string(expect));
            }
        }
    }

    // -------------------------
    // Partition + sort postings (bounded RAM)
    // -------------------------
    const fs::path bucket_dir = tmp_dir / "buckets";
    partition_postings_to_buckets(postings_files, bucket_dir);

    // -------------------------
    // Write final index_native.bin.tmp
    // -------------------------
    {
        std::ofstream bout(bin_tmp, std::ios::binary);
        if (!bout) throw L5Exception("cannot open " + bin_tmp.string());

        HeaderV2 h{};
        h.magic[0] = 'P'; h.magic[1] = 'L'; h.magic[2] = 'A'; h.magic[3] = 'G';
        h.version = 2;
        h.n_docs = N_docs;
        h.n_post9 = N_post9;
        h.n_post13 = 0;

        if (!write_header_v2(bout, h)) throw L5Exception("write header failed");

        // append docmeta bytes as-is
        {
            std::ifstream dm(docmeta_tmp, std::ios::binary);
            if (!dm) throw L5Exception("cannot open docmeta tmp for read: " + docmeta_tmp.string());

            std::vector<char> buf(1u << 20);
            while (dm) {
                dm.read(buf.data(), (std::streamsize)buf.size());
                const std::streamsize got = dm.gcount();
                if (got > 0) bout.write(buf.data(), got);
            }
            if (!bout) throw L5Exception("failed writing docmeta to index");
        }

        // sort each bucket in order and append
        const fs::path sort_tmp_dir = tmp_dir / "sort_runs";
        fs::create_directories(sort_tmp_dir, ec);

        for (unsigned b = 0; b < 256; ++b) {
            char name[32];
            std::snprintf(name, sizeof(name), "b_%02X.bin", b);
            const fs::path bp = bucket_dir / name;
            sort_bucket_append_to_index(bp, bout, sort_tmp_dir, opt.ram_limit_bytes, b);
        }

        bout.flush();
        if (!bout) throw L5Exception("write failed " + bin_tmp.string());
    }

    // -------------------------
    // meta json tmp (compact)
    // -------------------------
    {
        std::ofstream m(meta_tmp, std::ios::binary);
        if (!m) throw L5Exception("cannot open meta tmp: " + meta_tmp.string());

        m.put('{');
        m << "\"segment_name\":";
        json_write_string(m, segment_name);
        m << ",\"built_at_utc\":";
        json_write_string(m, built_at);
        m << ",\"stats\":{";
        m << "\"docs\":" << N_docs << ",\"k9\":" << N_post9 << ",\"k13\":0";
        m << "}";
        m << ",\"strict_text_is_normalized\":" << (strict ? 1 : 0);
        m.put('}');
        m.flush();
        if (!m) throw L5Exception("meta write failed");
    }

    // atomic replace
    if (!atomic_replace_file_best_effort(bin_tmp, bin_fin)) throw L5Exception("atomic replace failed (bin)");
    if (!atomic_replace_file_best_effort(doc_tmp, doc_fin)) throw L5Exception("atomic replace failed (docids)");
    if (!atomic_replace_file_best_effort(meta_tmp, meta_fin)) throw L5Exception("atomic replace failed (meta)");

    SegmentEntry e;
    e.segment_name = segment_name;
    e.path = segment_name + "/";
    e.built_at_utc = built_at;
    e.stats.docs = N_docs;
    e.stats.k9 = N_post9;
    e.stats.k13 = 0;

    if (!append_segment_to_manifest(out_root, e)) throw L5Exception("manifest append failed");

    // cleanup temp dir (best effort)
    {
        std::error_code ec3;
        fs::remove_all(tmp_dir, ec3);
        fs::remove(docmeta_tmp, ec3);
    }

    st.segment_name = segment_name;
    st.seg_dir = seg_dir;
    st.docs = N_docs;
    st.post9 = N_post9;
    st.threads = num_threads;
    st.strict_text_is_normalized = strict ? 1 : 0;
    st.built_at_utc = built_at;

    cleanup.keep = true;
    return st;
}

} // namespace l5
