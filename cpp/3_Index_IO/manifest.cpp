#include "l5/manifest.h"
#include "l5/format.h"
#include "l5/errors.h"

#include <fstream>
#include <unordered_set>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
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
        if (!is_safe_segment_name(se.segment_name)) continue;
        if (se.path.empty()) se.path = se.segment_name + "/";
        if (se.path != se.segment_name + "/") continue;
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
    std::error_code ec;
    std::filesystem::create_directories(out_root, ec);

    const auto manifest_fin = out_root / "level5_manifest.json";
    const auto manifest_tmp = unique_manifest_tmp(out_root);
    ManifestLock lk(out_root);

    json j;
    j["segments"] = json::array();

    for (const auto& e : m.segments) {
        if (!is_safe_segment_name(e.segment_name)) continue;
        json entry;
        entry["segment_name"] = e.segment_name;
        entry["path"] = e.segment_name + "/";
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
    if (!is_safe_segment_name(e.segment_name)) {
        std::cerr << "[l5] append_segment_to_manifest: unsafe segment_name\n"; return false;
    }

    json entry;
    entry["segment_name"] = e.segment_name;
    entry["path"] = e.segment_name + "/";
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

    const auto manifest_fin = out_root / "level5_manifest.json";
    const auto manifest_tmp = unique_manifest_tmp(out_root);
    ManifestLock lk(out_root);

    json j;
    std::string err;
    if (!read_json_object_strict(manifest_fin, j, &err)) {
        std::cerr << "[l5] remove_segments_from_manifest: manifest corrupted: " << err
                  << " file=" << manifest_fin << "\n";
        return false;
    }
    if (!j.contains("segments")) return true; // nothing to remove
    if (!j["segments"].is_array()) {
        std::cerr << "[l5] remove_segments_from_manifest: segments is not array"
                  << " file=" << manifest_fin << "\n";
        return false;
    }
    
    std::unordered_set<std::string> del;
    del.reserve(segment_names.size() * 2);
    for (const auto& s : segment_names) del.insert(s);

    json out_arr = json::array();
    for (auto& v : j["segments"]) {
        if (!v.is_object()) continue;
        const std::string name = v.value("segment_name", "");
        if (!is_safe_segment_name(name)) continue;
        if (del.find(name) != del.end()) continue;

        json entry;
        entry["segment_name"] = name;
        entry["path"] = name + "/";
        entry["built_at_utc"] = v.value("built_at_utc", "");
        auto st = v.value("stats", json::object());
        entry["stats"] = {{"docs", st.value("docs", 0)}, {"k9", st.value("k9", 0)}, {"k13", st.value("k13", 0)}};
        out_arr.push_back(std::move(entry));
    }
    j["segments"] = std::move(out_arr);

    std::error_code ec;
    std::filesystem::create_directories(out_root, ec);

    if (!write_text_file_tmp(manifest_tmp, j.dump())) return false;
    return atomic_replace_file_best_effort(manifest_tmp, manifest_fin);

}
bool replace_segments_in_manifest(const std::filesystem::path& out_root,
                                  const std::vector<std::string>& remove_segment_names,
                                  const SegmentEntry& add_entry,
                                  std::string* err) {
    if (!is_safe_segment_name(add_entry.segment_name)) {
        if (err) *err = "unsafe add_entry.segment_name";
        return false;
    }

    const auto manifest_fin = out_root / "level5_manifest.json";
    const auto manifest_tmp = unique_manifest_tmp(out_root);
    ManifestLock lk(out_root);

    json j;
    std::string jerr;
    if (!read_json_object_strict(manifest_fin, j, &jerr)) {
        if (err) *err = "manifest corrupted: " + jerr;
        return false;
    }
    if (!j.contains("segments")) j["segments"] = json::array();
    if (!j["segments"].is_array()) {
        if (err) *err = "manifest.segments is not array";
        return false;
    }

    std::unordered_set<std::string> del;
    del.reserve(remove_segment_names.size() * 2);
    for (const auto& s : remove_segment_names) del.insert(s);

    json out_arr = json::array();
    for (auto& v : j["segments"]) {
        if (!v.is_object()) continue;
        const std::string name = v.value("segment_name", "");
        if (!is_safe_segment_name(name)) continue;
        if (del.find(name) != del.end()) continue;

        json entry;
        entry["segment_name"] = name;
        entry["path"] = name + "/";
        entry["built_at_utc"] = v.value("built_at_utc", "");
        auto st = v.value("stats", json::object());
        entry["stats"] = {{"docs", st.value("docs", 0)}, {"k9", st.value("k9", 0)}, {"k13", st.value("k13", 0)}};
        out_arr.push_back(std::move(entry));
    }

    // append new segment at end
    json ne;
    ne["segment_name"] = add_entry.segment_name;
    ne["path"] = add_entry.segment_name + "/";
    ne["built_at_utc"] = add_entry.built_at_utc;
    ne["stats"] = {{"docs", add_entry.stats.docs}, {"k9", add_entry.stats.k9}, {"k13", add_entry.stats.k13}};
    out_arr.push_back(std::move(ne));

    j["segments"] = std::move(out_arr);

    std::error_code ec;
    std::filesystem::create_directories(out_root, ec);

    if (!write_text_file_tmp(manifest_tmp, j.dump())) { if (err) *err = "write tmp failed"; return false; }
    if (!atomic_replace_file_best_effort(manifest_tmp, manifest_fin)) { if (err) *err = "atomic replace failed"; return false; }
    return true;
}

} // namespace l5
