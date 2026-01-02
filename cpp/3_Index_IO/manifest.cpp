#include "l5/manifest.h"
#include "l5/format.h"
#include "l5/errors.h"

#include <fstream>
#include <unordered_set>
#include <iostream>
#include <random>
#include <string>
#include <system_error>
#include <filesystem>
#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstdlib>

#if !defined(_WIN32)
#include <sys/file.h>
#include <fcntl.h>
#include <unistd.h>
#endif

#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace l5 {

static std::string rand_hex8() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis;
    uint32_t v = dis(gen);
    static const char* hex = "0123456789abcdef";
    std::string s(8, '0');
    for (int i = 0; i < 8; ++i) { s[7 - i] = hex[v & 0xF]; v >>= 4; }
    return s;
}

#if !defined(_WIN32)
class ManifestLock {
    int fd_ = -1;
public:
    explicit ManifestLock(const std::filesystem::path& root) {
        const auto p = (root / ".level5_manifest.lock").string();
        fd_ = ::open(p.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0644);
        if (fd_ < 0) throw L5Exception("cannot open manifest lock: " + p);
        if (::flock(fd_, LOCK_EX) != 0) throw L5Exception("flock LOCK_EX failed: " + p);
    }
    ~ManifestLock() { if (fd_ >= 0) { ::flock(fd_, LOCK_UN); ::close(fd_); } }
    ManifestLock(const ManifestLock&) = delete;
    ManifestLock& operator=(const ManifestLock&) = delete;
};
#else
class ManifestLock {
public:
    explicit ManifestLock(const std::filesystem::path&) {}
};
#endif

static bool read_json_object_strict(const std::filesystem::path& p, json& out, std::string* err) {
    std::error_code ec;
    if (!std::filesystem::exists(p, ec)) {
        out = json::object(); // missing => ok
        return true;
    }
    if (ec) {
        if (err) *err = "exists() failed: " + ec.message();
        return false;
    }
    std::ifstream in(p);
    if (!in) {
        if (err) *err = "cannot open " + p.string();
        return false;
    }
    try {
        in >> out;
    } catch (const std::exception& e) {
        if (err) *err = std::string("json parse failed: ") + e.what();
        return false;
    } catch (...) {
        if (err) *err = "json parse failed: unknown";
        return false;
    }
    if (!out.is_object()) {
        if (err) *err = "json is not object";
        return false;
    }
    return true;
}

static std::filesystem::path unique_manifest_tmp(const std::filesystem::path& root) {
    // level5_manifest.json.tmp.<pid>.<rand>
    char buf[128];
#if !defined(_WIN32)
    const int pid = (int)::getpid();
#else
    const int pid = 0;
#endif
    std::snprintf(buf, sizeof(buf), "level5_manifest.json.tmp.%d.%s", pid, rand_hex8().c_str());
    return root / buf;
}

static bool write_text_file_tmp(const std::filesystem::path& tmp, const std::string& content) {
    std::ofstream out(tmp, std::ios::binary);
    if (!out) return false;
    out.write(content.data(), (std::streamsize)content.size());
    out.flush();
    return (bool)out;
}

bool load_manifest_strict(const std::filesystem::path& out_root, Manifest& out, std::string* err) {
    out = Manifest{};
    const auto p = out_root / "level5_manifest.json";
    json j;
    if (!read_json_object_strict(p, j, err)) return false;
    if (!j.contains("segments")) return true; // empty ok
    if (!j["segments"].is_array()) {
        if (err) *err = "manifest.segments is not array";
        return false;
    }
    for (auto& e : j["segments"]) {
        if (!e.is_object()) continue;
        SegmentEntry se;
        se.segment_name = e.value("segment_name", "");
        se.path = e.value("path", "");
        se.built_at_utc = e.value("built_at_utc", "");
        auto st = e.value("stats", json::object());
        se.stats.docs = st.value("docs", 0);
        se.stats.k9   = st.value("k9", 0);
        se.stats.k13  = st.value("k13", 0);
        if (!se.segment_name.empty() && !se.path.empty()) out.segments.push_back(std::move(se));
    }
    return true;
}

Manifest load_manifest(const std::filesystem::path& out_root) {
    Manifest m;
    std::string err;
    if (!load_manifest_strict(out_root, m, &err)) {
        std::cerr << "[l5] manifest read failed: " << err
                  << " root=" << out_root << "\n";
        return Manifest{};
    }
    return m;
}

bool save_manifest(const std::filesystem::path& out_root, const Manifest& m) {
    const auto manifest_fin = out_root / "level5_manifest.json";
    const auto manifest_tmp = unique_manifest_tmp(out_root);
    ManifestLock lk(out_root);

    std::error_code ec;
    std::filesystem::create_directories(out_root, ec);

    json j;
    j["segments"] = json::array();

    for (const auto& e : m.segments) {
        if (e.segment_name.empty() || e.path.empty()) continue;
        json entry;
        entry["segment_name"] = e.segment_name;
        entry["path"] = e.path;
        entry["built_at_utc"] = e.built_at_utc;
        entry["stats"] = {{"docs", e.stats.docs}, {"k9", e.stats.k9}, {"k13", e.stats.k13}};
        j["segments"].push_back(std::move(entry));
    }

    if (!write_text_file_tmp(manifest_tmp, j.dump())) return false;
    return atomic_replace_file_best_effort(manifest_tmp, manifest_fin);
}

bool append_segment_to_manifest(const std::filesystem::path& out_root, const SegmentEntry& e) {
    const auto manifest_fin = out_root / "level5_manifest.json";
    const auto manifest_tmp = unique_manifest_tmp(out_root);

    ManifestLock lk(out_root);

    json j;
    std::string err;
    if (!read_json_object_strict(manifest_fin, j, &err)) {
        // IMPORTANT: don't "repair" corrupted manifest by overwriting it with a new empty one.
        std::cerr << "[l5] append_segment_to_manifest: manifest corrupted: " << err
                  << " file=" << manifest_fin << "\n";
        return false;
    }
    if (!j.contains("segments")) j["segments"] = json::array();
    if (!j["segments"].is_array()) {
        std::cerr << "[l5] append_segment_to_manifest: segments is not array"
                  << " file=" << manifest_fin << "\n";
        return false;
    }

    json entry;
    entry["segment_name"] = e.segment_name;
    entry["path"] = e.path;
    entry["built_at_utc"] = e.built_at_utc;
    entry["stats"] = {{"docs", e.stats.docs}, {"k9", e.stats.k9}, {"k13", e.stats.k13}};
    j["segments"].push_back(std::move(entry));

    std::error_code ec;
    std::filesystem::create_directories(out_root, ec);

    if (!write_text_file_tmp(manifest_tmp, j.dump())) return false;
    return atomic_replace_file_best_effort(manifest_tmp, manifest_fin);
}

bool remove_segments_from_manifest(const std::filesystem::path& out_root,
                                   const std::vector<std::string>& segment_names) {
    if (segment_names.empty()) return true;

    Manifest m;
    std::string err;
    if (!load_manifest_strict(out_root, m, &err)) {
        std::cerr << "[l5] remove_segments_from_manifest: manifest corrupted: " << err
                  << " root=" << out_root << "\n";
        return false;
    }
    
    std::unordered_set<std::string> del;
    del.reserve(segment_names.size() * 2);
    for (const auto& s : segment_names) del.insert(s);

    Manifest out;
    out.segments.reserve(m.segments.size());
    for (auto& e : m.segments) {
        if (del.find(e.segment_name) != del.end()) continue;
        out.segments.push_back(std::move(e));
    }

    return save_manifest(out_root, out);
}

} // namespace l5
