#pragma once

#include <fmt/core.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>
#include <stdlib.h>

#include <boost/lockfree/spsc_queue.hpp>
#include <future>
#include <optional>
#include <seastar/core/posix.hh>
#include <thread>

namespace blobstore {

#ifndef SPDLOG_HEADER_ONLY
#define SPDLOG_HEADER_ONLY 1
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

void InitLoggerFactory(unsigned cpu = -1);

LoggerFactory* GetLoggerFactory();

enum class LogType { Normal = 0, Audit = 1, Max = 2 };

struct LogItem {
    LogType log_type;
    spdlog::memory_buf_t buf;
    spdlog::details::log_msg log_msg;
    std::optional<std::promise<void>> flush_pr;  // only use flush
    Logger* logger;
};

struct LogItemCmp {
    bool operator()(const LogItem* item1, const LogItem* item2) const {
        return item1->log_msg.time <= item2->log_msg.time;
    }
};

class LoggerFactory {
    std::string name_;
    unsigned cpu_;
    int fd_;
    seastar::posix_thread thread_;
    std::mutex mux_;
    std::vector<Logger*> logger_vec_;
    std::atomic<bool> stopped_ = {false};
    std::array<std::string, static_cast<size_t>(LogType::Max)> log_name_array_;
    std::array<std::string, static_cast<size_t>(LogType::Max)> log_pattern_array_;
    std::array<std::once_flag, static_cast<size_t>(LogType::Max)> log_flag_array_;
    std::array<std::shared_ptr<spdlog::logger>, static_cast<size_t>(LogType::Max)> log_array_;
    static bool discard_mode_;

    void InitLogThread();

    void RearrangeLogItems(std::set<LogItem*, LogItemCmp>& log_entries,
                           std::vector<int>& deleted_vec);

    void Flush();

    void FlushLogItems(const std::set<LogItem*, LogItemCmp>& log_entries);

    void DeleteLoggers(const std::vector<int>& deleted_vec);

    void Run();

   public:
    explicit LoggerFactory(std::string name, unsigned cpu = -1);

    ~LoggerFactory();

    void InitLogfile(LogType type, const std::string& filename, spdlog::level::level_enum level,
                     size_t max_file_size, size_t max_files);

    bool ShouldLog(LogType log_type, spdlog::level::level_enum level);

    void SetLevel(LogType log_type, spdlog::level::level_enum level);

    Logger* GetLocalLogger();

    static void EnableDiscardMode() { discard_mode_ = true; }
    static void DisableDiscardMode() { discard_mode_ = false; }
    static bool DiscardMode() { return discard_mode_; }
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

    void CompleteLogItem(LogItem* item);

   public:
    explicit Logger(std::string name, int fd) : name_(name), fd_(fd) {}
    ~Logger();

    void Flush();

    template <typename... Args>
    void Log(LogType log_type, spdlog::source_loc loc, spdlog::level::level_enum lvl,
             spdlog::string_view_t fmt, Args&&... args) {
        LogItem* item = GetLogItem();
        item->log_type = log_type;
        fmt::detail::vformat_to(item->buf, fmt, fmt::make_format_args(args...));
        item->log_msg = spdlog::details::log_msg(
            loc, name_, lvl, spdlog::string_view_t(item->buf.data(), item->buf.size()));

        if (LoggerFactory::DiscardMode()) {
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

#define InitLogFactory(cpu) blobstore::InitLoggerFactory(cpu);

#define INIT_LOGGER(type, filename, level, max_file_size, max_files)               \
    do {                                                                           \
        auto log_factory = blobstore::GetLoggerFactory();                          \
        if (!log_factory) {                                                        \
            blobstore::InitLoggerFactory();                                        \
            log_factory = blobstore::GetLoggerFactory();                           \
        }                                                                          \
        log_factory->InitLogfile(type, filename, level, max_file_size, max_files); \
    } while (0)

#define InitNormalLog(filename, str_level, max_file_size, max_files)                      \
    INIT_LOGGER(blobstore::LogType::Normal, filename, spdlog::level::from_str(str_level), \
                max_file_size, max_files)

#define InitAuditLog(filename, max_file_size, max_files) \
    INIT_LOGGER(blobstore::LogType::Audit, filename, spdlog::level::info, max_file_size, max_files)

#define DISABLE_AUDIT_LOG()                                              \
    do {                                                                 \
        blobstore::LoggerFactory* ft = blobstore::GetLoggerFactory();    \
        if (ft) [[likely]] {                                             \
            ft->SetLevel(blobstore::LogType::Audit, spdlog::level::off); \
        }                                                                \
    } while (0)

#define ENABLE_AUDIT_LOG()                                                \
    do {                                                                  \
        blobstore::LoggerFactory* ft = blobstore::GetLoggerFactory();     \
        if (ft) [[likely]] {                                              \
            ft->SetLevel(blobstore::LogType::Audit, spdlog::level::info); \
        }                                                                 \
    } while (0)

#define LOG_ENABLE_DISCARD_MODE() blobstore::LoggerFactory::EnableDiscardMode()

#define LOG_DISABLE_DISCARD_MODE() blobstore::LoggerFactory::DisableDiscardMode()

// str_level value: trace, debug, info, warning, error, critical
#define LOG_SET_LEVEL(str_level)                                                          \
    do {                                                                                  \
        blobstore::LoggerFactory* ft = blobstore::GetLoggerFactory();                     \
        if (ft) [[likely]] {                                                              \
            ft->SetLevel(blobstore::LogType::Normal, spdlog::level::from_str(str_level)); \
        } else {                                                                          \
            spdlog::set_level(spdlog::level::from_str(str_level));                        \
        }                                                                                 \
    } while (0)

#define LOG_COMMON(level, ...)                                                                     \
    do {                                                                                           \
        blobstore::LoggerFactory* ft = blobstore::GetLoggerFactory();                              \
        if (ft) [[likely]] {                                                                       \
            if (ft->ShouldLog(blobstore::LogType::Normal, level)) {                                \
                ft->GetLocalLogger()->Log(blobstore::LogType::Normal,                              \
                                          spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
                                          level, __VA_ARGS__);                                     \
            }                                                                                      \
        } else {                                                                                   \
            if (spdlog::should_log(level)) {                                                       \
                spdlog::default_logger_raw()->log(                                                 \
                    spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, level, __VA_ARGS__);  \
            }                                                                                      \
        }                                                                                          \
    } while (0)

#define AUDIT_LOG(...)                                                                         \
    do {                                                                                       \
        blobstore::LoggerFactory* ft = blobstore::GetLoggerFactory();                          \
        if (ft && ft->ShouldLog(blobstore::LogType::Audit, spdlog::level::info)) [[likely]] {  \
            ft->GetLocalLogger()->Log(LogType::Audit,                                          \
                                      spdlog::source_loc{__FILE__, __LINE__, SPDLOG_FUNCTION}, \
                                      spdlog::level::info, __VA_ARGS__);                       \
        }                                                                                      \
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
