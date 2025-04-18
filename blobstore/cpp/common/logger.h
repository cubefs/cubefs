#pragma once

#include <fmt/core.h>
#include <stdlib.h>

#include <boost/lockfree/spsc_queue.hpp>
#include <future>
#include <optional>
#include <seastar/core/posix.hh>
#include <thread>

#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"

namespace blobstore {

#ifndef SPDLOG_HEADER_ONLY
#define SPDLOG_HEADER_ONLY 1
#endif

#ifdef LOG_INIT
#undef LOG_INIT
#endif

#ifdef LOG_SET_LEVEL
#undef LOG_SET_LEVEL
#endif

#ifdef LOG_TRACE
#undef LOG_TRACE
#endif

#ifdef LOG_DEBUG
#undef LOG_DEBUG
#endif

#ifdef LOG_INFO
#undef LOG_INFO
#endif

#ifdef LOG_WARN
#undef LOG_WARN
#endif

#ifdef LOG_ERROR
#undef LOG_ERROR
#endif

#ifdef LOG_FATAL
#undef LOG_FATAL
#endif

class Logger;
class LoggerFactory;

void InitLoggerFactory(const std::string& filename, size_t max_file_size, size_t max_files,
                       unsigned cpu = -1);

LoggerFactory* GetLoggerFactory();

struct LogItem {
    spdlog::memory_buf_t buf;
    spdlog::details::log_msg log_msg;
    std::optional<std::promise<void>> flush_prom;  // only use flush
    Logger* logger;
};

class LoggerFactory {
    std::string name_;
    unsigned cpu_;
    int fd_;
    seastar::posix_thread thread_;
    std::mutex mux_;
    std::vector<Logger*> logger_vec_;
    std::atomic<bool> stopped_ = {false};
    std::shared_ptr<spdlog::logger> log_;
    static bool async_mode_;

    void Run(std::string name);

   public:
    explicit LoggerFactory(std::string name, std::string logfile, size_t max_file_size,
                           size_t max_files, unsigned cpu = -1);

    ~LoggerFactory();

    bool should_log(spdlog::level::level_enum msg_level);

    void set_level(spdlog::level::level_enum log_level);

    Logger* GetLocalLogger();

    static void EnableAsyncMode() { async_mode_ = true; }
    static void DisableAsyncMode() { async_mode_ = false; }
    static bool AsyncMode() { return async_mode_; }
};

class Logger {
    static constexpr size_t queue_length = 8192;
    using lf_queue = boost::lockfree::spsc_queue<LogItem*, boost::lockfree::capacity<queue_length>>;
    std::string name_;
    int fd_;
    lf_queue pending_;
    lf_queue completed_;

    bool closed_ = false;
    std::mutex mux_;
    std::condition_variable cv_;

    friend class LoggerFactory;

    LogItem* GetLogItem();

   public:
    explicit Logger(std::string name, int fd) : name_(name), fd_(fd) {}
    ~Logger();

    void Flush();

    template <typename... Args>
    void Log(spdlog::source_loc loc, spdlog::level::level_enum lvl, spdlog::string_view_t fmt,
             Args&&... args) {
        LogItem* item = GetLogItem();
        fmt::detail::vformat_to(item->buf, fmt, fmt::make_format_args(args...));
        item->log_msg = spdlog::details::log_msg(
            loc, name_, lvl, spdlog::string_view_t(item->buf.data(), item->buf.size()));

        if (LoggerFactory::AsyncMode()) {
            if (!pending_.push(item)) {
                delete item;
            }
            return;
        }

        while (!pending_.push(item)) {
            std::unique_lock<std::mutex> lock(mux_);
            cv_.wait(lock);
        }

        uint64_t val = 1;
        size_t n = ::write(fd_, &val, sizeof(val));
        (void)n;
        return;
    }

    void Close() { closed_ = true; }
};

#define LOG_INIT(filename, max_file_size, max_files, cpu) \
    blobstore::InitLoggerFactory(filename, max_file_size, max_files, cpu)

#define LOG_ENABLE_ASYNC_MODE() blobstore::LoggerFactory::EnableAsyncMode()

#define LOG_DISABLE_ASYNC_MODE() blobstore::LoggerFactory::DisableAsyncMode()

// str_level value: trace, debug, info, warning, error, critical
#define LOG_SET_LEVEL(str_level)                                      \
    do {                                                              \
        blobstore::LoggerFactory* ft = blobstore::GetLoggerFactory(); \
        if (ft) [[likely]] {                                          \
            ft->set_level(spdlog::level::from_str(str_level));        \
        } else {                                                      \
            spdlog::set_level(spdlog::level::from_str(str_level));    \
        }                                                             \
    } while (0)

#define LOG_COMMON(level, ...)                                                                     \
    do {                                                                                           \
        blobstore::LoggerFactory* ft = blobstore::GetLoggerFactory();                              \
        if (ft) [[likely]] {                                                                       \
            if (ft->should_log(level)) {                                                           \
                ft->GetLocalLogger()->Log(spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
                                          level, __VA_ARGS__);                                     \
            }                                                                                      \
        } else {                                                                                   \
            if (spdlog::should_log(level)) {                                                       \
                spdlog::default_logger_raw()->log(                                                 \
                    spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, level, __VA_ARGS__);  \
            }                                                                                      \
        }                                                                                          \
    } while (0)

#define LOG_TRACE(...) LOG_COMMON(spdlog::level::trace, __VA_ARGS__)
#define LOG_DEBUG(...) LOG_COMMON(spdlog::level::debug, __VA_ARGS__)
#define LOG_INFO(...) LOG_COMMON(spdlog::level::info, __VA_ARGS__)
#define LOG_WARN(...) LOG_COMMON(spdlog::level::warn, __VA_ARGS__)
#define LOG_ERROR(...) LOG_COMMON(spdlog::level::err, __VA_ARGS__)

#define LOG_FATAL(...)                                    \
    do {                                                  \
        LOG_COMMON(spdlog::level::critical, __VA_ARGS__); \
        LOG_FLUSH();                                      \
        abort();                                          \
    } while (0)

#define LOG_FATAL_THROW(...)                                \
    do {                                                    \
        LOG_COMMON(spdlog::level::critical, __VA_ARGS__);   \
        throw std::runtime_error(fmt::format(__VA_ARGS__)); \
    } while (0)

#define LOG_FLUSH()                                                   \
    do {                                                              \
        blobstore::LoggerFactory* ft = blobstore::GetLoggerFactory(); \
        if (ft) [[likely]] {                                          \
            ft->GetLocalLogger()->Flush();                            \
        }                                                             \
    } while (0)

}  // namespace blobstore
