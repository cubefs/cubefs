#pragma once

#include <fmt/ostream.h>

#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <string>

namespace blobstore {

enum class ErrCode {
    OK = 200,
    // custom error
    ErrInvalid = 400,
    ErrNotFound = 404,
    ErrTimeout = 408,
    ErrConflict = 409,
    ErrEOF = 410,
    ErrTooLarge = 413,
    ErrEIO = 424,
    ErrClosed = 425,
    ErrUnsupported = 426,
    ErrDevice = 430,
    ErrUnknown = 500,

    // net rpc error
    ErrNetwork = 510,
    ErrNetworkPipe = 511,
    ErrNetworkReset = 512,
    ErrNetworkProtocol = 513,

    // blobnode 6xx
    ErrBlobnodeStoreInit = 601,
};

const char* GetReason(ErrCode code);

class StatusBase {
   protected:
    ErrCode code_;
    mutable seastar::sstring reason_;
    mutable std::string cached_string_;
    mutable bool string_cached_ = false;

   public:
    StatusBase() noexcept : code_(ErrCode::OK) {}
    explicit StatusBase(ErrCode code) noexcept : code_(code) {}
    StatusBase(ErrCode code, const seastar::sstring& reason) noexcept
        : code_(code), reason_(reason) {}
    StatusBase(const StatusBase& other) noexcept : code_(other.code_), reason_(other.reason_) {}
    StatusBase(StatusBase&& other) noexcept
        : code_(other.code_), reason_(std::move(other.reason_)) {}

    StatusBase& operator=(const StatusBase& other) noexcept {
        if (this != &other) {
            code_ = other.code_;
            reason_ = other.reason_;
            InvalidateCache();
        }
        return *this;
    }
    StatusBase& operator=(StatusBase&& other) noexcept {
        if (this != &other) {
            code_ = other.code_;
            reason_ = std::move(other.reason_);
            InvalidateCache();
        }
        return *this;
    }

    explicit operator bool() const noexcept { return OK(); }
    bool OK() const noexcept { return code_ == ErrCode::OK; }

    ErrCode Code() const noexcept { return code_; }
    const seastar::sstring& Reason() const noexcept {
        if (reason_.empty()) {
            reason_ = GetReason(code_);
            string_cached_ = false;
        }
        return reason_;
    }

    StatusBase& SetCode(ErrCode code) noexcept {
        code_ = code;
        InvalidateCache();
        return *this;
    }
    StatusBase& SetReason(const seastar::sstring& reason) noexcept {
        reason_ = reason;
        InvalidateCache();
        return *this;
    }

    const std::string& ToString() const {
        if (reason_.empty() || !string_cached_) {
            cached_string_ = FormatString();
            string_cached_ = true;
        }
        return cached_string_;
    }

    friend std::ostream& operator<<(std::ostream& os, const StatusBase& s) {
        return os << s.ToString();
    }

   protected:
    void InvalidateCache() noexcept { string_cached_ = false; }

   private:
    std::string FormatString() const {
        return fmt::format(R"({{"code": {}, "reason": "{}"}})", static_cast<int>(code_), Reason());
    }
};

template <typename... T>
class Status;

template <typename T>
class Status<T> : public StatusBase {
    T value_;

   public:
    using ValueType = T;
    Status() = default;

    explicit Status(ErrCode code) noexcept : StatusBase(code) {}

    Status(ErrCode code, const seastar::sstring& reason) noexcept : StatusBase(code, reason) {}

    Status(const Status& other) noexcept(std::is_nothrow_copy_constructible_v<T>)
        : StatusBase(other), value_(other.value_) {}

    Status(Status&& other) noexcept(std::is_nothrow_move_constructible_v<T>)
        : StatusBase(std::move(other)), value_(std::move(other.value_)) {}

    Status& operator=(const Status& other) noexcept(std::is_nothrow_copy_assignable_v<T>) {
        if (this != &other) {
            StatusBase::operator=(other);
            value_ = other.value_;
        }
        return *this;
    }

    Status& operator=(Status&& other) noexcept(std::is_nothrow_move_assignable_v<T>) {
        if (this != &other) {
            StatusBase::operator=(std::move(other));
            value_ = std::move(other.value_);
        }
        return *this;
    }

    T& Value() & { return value_; }
    const T& Value() const& { return value_; }
    T&& Value() && { return std::move(value_); }
    const T&& Value() const&& { return std::move(value_); }

    Status& SetValue(T val) noexcept(std::is_nothrow_move_assignable_v<T>) {
        value_ = std::move(val);
        return *this;
    }
};

template <>
class Status<> : public StatusBase {
   public:
    using ValueType = void;

    Status() = default;

    Status(ErrCode code) noexcept : StatusBase(code) {}

    Status(ErrCode code, const seastar::sstring& reason) noexcept : StatusBase(code, reason) {}

    Status(const Status& other) noexcept : StatusBase(other) {}

    Status(Status&& other) noexcept : StatusBase(std::move(other)) {}

    Status& operator=(const Status& other) noexcept {
        if (this != &other) {
            StatusBase::operator=(other);
        }
        return *this;
    }

    Status& operator=(Status&& other) noexcept {
        if (this != &other) {
            StatusBase::operator=(std::move(other));
        }
        return *this;
    }
};

template <typename... T>
using FutureStatus = seastar::future<::blobstore::Status<T...>>;

}  // namespace blobstore

#if FMT_VERSION >= 90000

template <typename T>
struct fmt::formatter<blobstore::Status<T>> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<blobstore::Status<>> : fmt::ostream_formatter {};

#endif
