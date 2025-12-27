// Back_L5/cpp/src/manifest.cpp
#include "l5/manifest.h"
#include "l5/format.h"

#include <fstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

namespace l5 {

static json read_json_file_or_empty_object(const std::filesystem::path& p) {
    try {
        std::ifstream in(p);
        if (!in) return json::object();
        json j;
        in >> j;
        if (!j.is_object()) return json::object();
        return j;
    } catch (...) {
        return json::object();
    }
}

static bool write_text_file_tmp(const std::filesystem::path& tmp, const std::string& content) {
    std::ofstream out(tmp, std::ios::binary);
    if (!out) return false;
    out.write(content.data(), (std::streamsize)content.size());
    out.flush();
    return (bool)out;
}

Manifest load_manifest(const std::filesystem::path& out_root) {
    Manifest m;
    const auto p = out_root / "level5_manifest.json";
    json j = read_json_file_or_empty_object(p);
    if (!j.is_object()) return m;
    if (!j.contains("segments") || !j["segments"].is_array()) return m;

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
        if (!se.segment_name.empty() && !se.path.empty()) {
            m.segments.push_back(std::move(se));
        }
    }
    return m;
}

bool append_segment_to_manifest(const std::filesystem::path& out_root, const SegmentEntry& e) {
    const auto manifest_fin = out_root / "level5_manifest.json";
    const auto manifest_tmp = out_root / "level5_manifest.json.tmp";

    json j = read_json_file_or_empty_object(manifest_fin);
    if (!j.contains("segments") || !j["segments"].is_array()) {
        j["segments"] = json::array();
    }

    json entry;
    entry["segment_name"] = e.segment_name;
    entry["path"] = e.path;
    entry["built_at_utc"] = e.built_at_utc;
    entry["stats"] = {{"docs", e.stats.docs}, {"k9", e.stats.k9}, {"k13", e.stats.k13}};
    j["segments"].push_back(std::move(entry));

    if (!write_text_file_tmp(manifest_tmp, j.dump())) return false;
    return atomic_replace_file_best_effort(manifest_tmp, manifest_fin);
}

} // namespace l5
