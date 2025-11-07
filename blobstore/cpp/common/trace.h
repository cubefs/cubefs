#pragma once

#include <fmt/format.h>

#include <chrono>
#include <seastar/core/lowres_clock.hh>
#include <string>

#include "common/util.h"

namespace blobstore {

class Trace {
    std::string trace_id_;
    std::string trace_string_;

   public:
    static constexpr size_t kDefaultCapacity = 64;

    explicit Trace() : trace_id_(GenerateTraceid()) { trace_string_.reserve(kDefaultCapacity); }
    explicit Trace(const std::string& trace_id) : trace_id_(trace_id) {
        trace_string_.reserve(kDefaultCapacity);
    }

    Trace(const Trace&) = delete;
    Trace(Trace&&) = delete;
    Trace& operator=(const Trace&) = delete;
    Trace& operator=(Trace&&) = delete;

    inline const std::string& TraceID() const noexcept { return trace_id_; }
    inline const std::string& TraceString() const noexcept { return trace_string_; }

    void Append(const std::string& name, seastar::lowres_clock::duration duration =
                                             seastar::lowres_clock::duration(-1)) noexcept;
    void Append(const std::string& name, seastar::lowres_clock::time_point start) noexcept;
    void Append(const std::string& name, std::chrono::steady_clock::time_point start) noexcept;
    void Append(const std::string& name, const std::string& value) noexcept;

    friend std::ostream& operator<<(std::ostream& os, const Trace& trace) noexcept {
        return os << trace.TraceString();
    }
};

}  // namespace blobstore

#if FMT_VERSION >= 90000

template <>
struct fmt::formatter<blobstore::Trace> : fmt::ostream_formatter {};

#endif
