// Back_L5/cpp/include/l5/docinfo.h
#pragma once
#include <string>

namespace l5 {

struct DocInfo {
    std::string doc_id;          // internal / display id
    std::string organization_id; // org scope
    std::string external_id;     // unique external id
    std::string source_path;     // stored path / upload path
    std::string source_name;     // original file name (e.g. "report.txt")
    std::string meta_path;       // segment path (provenance), e.g. "seg_xxx/"
    std::string preview_text;    // optional: small preview snippet
};

} // namespace l5
