// Back_L5/cpp/include/l5/validator.h
#pragma once
#include <filesystem>
#include <string>
#include <vector>

namespace l5 {

struct ValidationResult {
    bool ok{false};
    std::vector<std::string> errors;
};

ValidationResult validate_segment(const std::filesystem::path& seg_dir, bool check_sorted = true);

ValidationResult validate_out_root(const std::filesystem::path& out_root);

} // namespace l5
