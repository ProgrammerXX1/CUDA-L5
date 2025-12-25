#include <cassert>
#include <filesystem>
#include <ctime>

#include "l5/builder.h"
#include "l5/validator.h"

static std::filesystem::path mk_tmp_dir() {
    auto base = std::filesystem::temp_directory_path();
    auto p = base / ("l5_test_" + std::to_string((uint64_t)std::time(nullptr) + 1));
    std::filesystem::create_directories(p);
    return p;
}

static std::filesystem::path test_data_file(const char* name) {
#ifndef L5_TEST_DATA_DIR
    return std::filesystem::path("cpp/tests/data") / name; // fallback
#else
    return std::filesystem::path(L5_TEST_DATA_DIR) / name;
#endif
}

int main() {
    auto out_root = mk_tmp_dir();
    auto corpus = test_data_file("tiny.jsonl");

    l5::BuildOptions opt;
    opt.segment_name = "seg_test_validate";
    l5::build_segment_jsonl(corpus, out_root, opt);

    auto vr = l5::validate_out_root(out_root);
    assert(vr.ok);
    return 0;
}
