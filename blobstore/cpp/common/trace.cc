#include "trace.h"

namespace blobstore {

void Trace::Append(const std::string& name, seastar::lowres_clock::duration duration) noexcept {
    if (!trace_string_.empty()) {
        trace_string_ += ",";
    }
    auto dur = duration.count();
    if (dur < 0) {
        trace_string_ += name;
    } else {
        trace_string_ += fmt::format("{}:{}", name, dur);
    }
}

void Trace::Append(const std::string& name, seastar::lowres_clock::time_point start) noexcept {
    auto now = seastar::lowres_clock::now();
    Append(name, std::chrono::duration_cast<std::chrono::microseconds>(now - start));  // us
}

void Trace::Append(const std::string& name, std::chrono::steady_clock::time_point start) noexcept {
    auto now = std::chrono::steady_clock::now();
    Append(name, std::chrono::duration_cast<std::chrono::microseconds>(now - start));  // us
}

void Trace::Append(const std::string& name, const std::string& value) noexcept {
    if (!trace_string_.empty()) {
        trace_string_ += ",";
    }
    trace_string_ += name;
    trace_string_ += ":";
    trace_string_ += value;
}

}  // namespace blobstore
