#include <cassert>
#include <filesystem>
#include <iostream>
#include <ctime>

#include "l5/builder.h"
#include "l5/validator.h"

static std::filesystem::path mk_tmp_dir() {
    auto base = std::filesystem::temp_directory_path();
    auto p = base / ("l5_test_" + std::to_string((uint64_t)std::time(nullptr)));
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
    opt.segment_name = "seg_test_build";

    auto st = l5::build_segment_jsonl(corpus, out_root, opt);

    assert(st.docs > 0);
    assert(std::filesystem::exists(out_root / st.segment_name / "index_native.bin"));
    assert(std::filesystem::exists(out_root / st.segment_name / "index_native_docids.json"));
    assert(std::filesystem::exists(out_root / st.segment_name / "index_native_meta.json"));
    assert(std::filesystem::exists(out_root / "level5_manifest.json"));

    auto vr = l5::validate_out_root(out_root);
    if (!vr.ok) {
        for (auto& e : vr.errors) std::cerr << e << "\n";
    }
    assert(vr.ok);

    std::cout << "OK\n";
    return 0;
}
