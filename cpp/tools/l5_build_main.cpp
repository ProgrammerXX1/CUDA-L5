#include <iostream>
#include <string>
#include <filesystem>

#include <nlohmann/json.hpp>
#include "l5/builder.h"

static std::string arg_value(int& i, int argc, char** argv) {
    if (i + 1 >= argc) return "";
    return argv[++i];
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: l5_build <corpus_jsonl> <out_root_dir> [--segment-name NAME]\n";
        return 1;
    }

    std::filesystem::path corpus = argv[1];
    std::filesystem::path out_root = argv[2];

    l5::BuildOptions opt;
    for (int i = 3; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--segment-name") opt.segment_name = arg_value(i, argc, argv);
    }

    try {
        auto st = l5::build_segment_jsonl(corpus, out_root, opt);
        nlohmann::json j;
        j["segment_name"] = st.segment_name;
        j["seg_dir"] = st.seg_dir.string();
        j["docs"] = st.docs;
        j["post9"] = st.post9;
        j["threads"] = st.threads;
        j["strict_text_is_normalized"] = st.strict_text_is_normalized;
        j["built_at_utc"] = st.built_at_utc;
        std::cout << j.dump() << "\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "l5_build failed: " << e.what() << "\n";
        return 2;
    }
}
