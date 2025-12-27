#include <filesystem>
#include <iostream>
#include <ctime>

#include "l5/builder.h"
#include "l5/search_multi.h"

static std::filesystem::path mk_tmp_dir() {
    auto base = std::filesystem::temp_directory_path();
    auto p = base / ("l5_test_" + std::to_string((uint64_t)std::time(nullptr) + 2));
    std::filesystem::create_directories(p);
    return p;
}

static std::filesystem::path test_data_file(const char* name) {
#ifndef L5_TEST_DATA_DIR
    return std::filesystem::path("cpp/tests/data") / name;
#else
    return std::filesystem::path(L5_TEST_DATA_DIR) / name;
#endif
}

int main() {
    auto out_root = mk_tmp_dir();
    auto corpus = test_data_file("tiny.jsonl");

    l5::BuildOptions opt;
    opt.segment_name = "seg_test_search";
    l5::build_segment_jsonl(corpus, out_root, opt);

    l5::SearchOptions sopt;
    sopt.topk = 5;
    sopt.min_hits = 1;
    sopt.span_min_len = 2;
    sopt.candidates_topn = 50;

    const std::string query =
        "Это длинный тестовый документ для шингловой системы и поиска. "
        "Он нужен чтобы построить много шинглов k девять и проверить совпадения. "
        "Мы специально пишем несколько предложений подряд чтобы создать устойчивые последовательности токенов.";

    auto r = l5::search_out_root(out_root, query, true, sopt);

    if (r.segments_scanned < 1) {
        std::cerr << "FAIL: segments_scanned < 1\n";
        return 2;
    }
    if (r.hits.empty()) {
        std::cerr << "FAIL: no hits for query: " << query << "\n";
        return 3;
    }

    std::cout << "Top hit: " << r.hits[0].doc_id
              << " C=" << r.hits[0].C
              << " spans=" << r.hits[0].match_spans.size() << "\n";
    return 0;
}
