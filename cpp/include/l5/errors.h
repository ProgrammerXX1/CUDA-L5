#pragma once
#include <stdexcept>
#include <string>
#include <vector>

namespace l5 {

enum class ErrorCode {
    Ok = 0,
    IoError,
    ParseError,
    InvalidFormat,
    InvalidArgs,
    SegmentExists,
    NoValidDocs,
    ValidationFailed,
};

struct Error {
    ErrorCode code{ErrorCode::Ok};
    std::string message;
};

class L5Exception : public std::runtime_error {
public:
    explicit L5Exception(const std::string& msg) : std::runtime_error(msg) {}
};

} // namespace l5
