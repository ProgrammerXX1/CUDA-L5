// Back_L5/cpp/tools/l5_search_main.cpp
#include <iostream>
#include <filesystem>
#include <string>

#include "l5/search_multi.h"
#include <nlohmann/json.hpp>

static std::string arg_value(int& i, int argc, char** argv) {
    if (i + 1 >= argc) return "";
    return argv[++i];
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: l5_search <out_root_dir> --query \"...\" [--topk N] [--normalized 0|1]\n";
        return 1;
    }

    std::filesystem::path out_root = argv[1];
    std::string query;
    bool normalized = false;
    l5::SearchOptions opt;

    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--query") query = arg_value(i, argc, argv);
        else if (a == "--topk") opt.topk = (uint32_t)std::stoul(arg_value(i, argc, argv));
        else if (a == "--min-hits") opt.min_hits = (uint32_t)std::stoul(arg_value(i, argc, argv));
        else if (a == "--normalized") normalized = (arg_value(i, argc, argv) == "1");
    }

    if (query.empty()) {
        std::cerr << "Missing --query\n";
        return 2;
    }

    auto r = l5::search_out_root(out_root, query, normalized, opt);
    std::cout << l5::to_json(r).dump() << "\n";
    return 0;
}
