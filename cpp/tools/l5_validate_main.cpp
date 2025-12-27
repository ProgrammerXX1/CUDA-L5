// Back_L5/cpp/tools/l5_validate_main.cpp
#include <iostream>
#include <filesystem>
#include <string>

#include <nlohmann/json.hpp>
#include "l5/validator.h"

static std::string arg_value(int& i, int argc, char** argv) {
    if (i + 1 >= argc) return "";
    return argv[++i];
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: l5_validate <out_root_dir> [--segment NAME]\n";
        return 1;
    }

    std::filesystem::path out_root = argv[1];
    std::string segment;

    for (int i = 2; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--segment") segment = arg_value(i, argc, argv);
    }

    l5::ValidationResult vr;
    if (!segment.empty()) {
        vr = l5::validate_segment(out_root / segment, true);
    } else {
        vr = l5::validate_out_root(out_root);
    }

    nlohmann::json j;
    j["ok"] = vr.ok;
    j["errors"] = vr.errors;

    std::cout << j.dump() << "\n";
    return vr.ok ? 0 : 2;
}
