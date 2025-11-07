#include "logger.h"

namespace blobstore {

static std::once_flag log_flag;
static LoggerFactory* log_factory = nullptr;
bool LoggerFactory::discard_mode_ = true;

struct LoggerWrap {
    Logger* logger = nullptr;

    LoggerWrap() {}

    ~LoggerWrap() {
        if (logger) {
            logger->Close();
        }
    }
};

static thread_local LoggerWrap l_logger_wrap;

void InitLoggerFactory(unsigned cpu) {
    std::call_once(log_flag, [cpu]() {
        LoggerFactory* factory = new LoggerFactory("log-thread", cpu);
        log_factory = factory;
    });
}

LoggerFactory* GetLoggerFactory() { return log_factory; }

Logger::~Logger() {
    pending_.consume_all([](LogItem* item) { delete item; });
    completed_.consume_all([](LogItem* item) { delete item; });
}

LogItem* Logger::GetLogItem() {
    LogItem* item = nullptr;
    for (;;) {
        bool ok = completed_.consume_one([&item](LogItem* it) { item = it; });
        if (!ok) {
            item = new LogItem;
        } else if (item->buf.capacity() > 1024) {
            delete item;
            item = nullptr;
            continue;
        }
        break;
    }
    item->buf.clear();
    item->logger = this;
    return item;
}

void Logger::CompleteLogItem(LogItem* item) {
    if (!completed_.push(item)) {
        delete item;
    }
}

void Logger::Flush() {
    LogItem* item = new LogItem;
    item->logger = this;
    item->log_msg.time = std::chrono::time_point<std::chrono::system_clock>::max();
    item->flush_pr = std::move(std::promise<void>());
    auto fu = item->flush_pr.value().get_future();
    while (!pending_.push(item)) {
        std::unique_lock<std::mutex> lock(mux_);
        cv_.wait(lock);
    }
    uint64_t val = 1;
    size_t n = ::write(fd_, &val, sizeof(val));
    (void)n;
    fu.wait();
    delete item;
    completed_.consume_all([](LogItem* item) { delete item; });
}

LoggerFactory::LoggerFactory(std::string name, unsigned cpu)
    : name_(name),
      cpu_(cpu),
      fd_(eventfd(0, EFD_CLOEXEC)),
      log_name_array_({"normal_log", "audit_log"}),
      log_pattern_array_({"[%Y-%m-%d %H:%M:%S.%f %t][%l][%s:%#] %v", "[%Y-%m-%d %H:%M:%S.%f] %v"}),
      thread_([this] { Run(); }) {}

void LoggerFactory::InitLogfile(LogType type, const std::string& filename,
                                spdlog::level::level_enum level, size_t max_file_size,
                                size_t max_files) {
    std::call_once(log_flag_array_[static_cast<int>(type)], [this, type, filename, level,
                                                             max_file_size, max_files] {
        log_array_[static_cast<int>(type)] = spdlog::rotating_logger_st(
            log_name_array_[static_cast<int>(type)], filename, max_file_size, max_files);
        log_array_[static_cast<int>(type)]->set_level(level);
        log_array_[static_cast<int>(type)]->set_pattern(log_pattern_array_[static_cast<int>(type)]);
    });
}

LoggerFactory::~LoggerFactory() {
    stopped_.store(true, std::memory_order_relaxed);
    thread_.join();
}

void LoggerFactory::InitLogThread() {
    pthread_setname_np(pthread_self(), name_.c_str());
    sigset_t mask;
    sigfillset(&mask);
    ::pthread_sigmask(SIG_BLOCK, &mask, NULL);

    if (cpu_ != static_cast<unsigned>(-1)) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu_, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    }
}

void LoggerFactory::RearrangeLogItems(std::set<LogItem*, LogItemCmp>& log_entries,
                                      std::vector<int>& deleted_vec) {
    mux_.lock();
    std::vector<Logger*> loggers = logger_vec_;
    mux_.unlock();
    for (int i = 0; i < static_cast<int>(loggers.size()); i++) {
        loggers[i]->pending_.consume_all([&](LogItem* item) { log_entries.insert(item); });
        loggers[i]->mux_.lock();
        loggers[i]->cv_.notify_all();
        loggers[i]->mux_.unlock();
        if (loggers[i]->closed_) {
            deleted_vec.push_back(i);
        }
    }
}

void LoggerFactory::Flush() {
    for (int i = 0; i < static_cast<int>(LogType::Max); i++) {
        if (log_array_[i]) {
            log_array_[i]->flush();
        }
    }
}

void LoggerFactory::FlushLogItems(const std::set<LogItem*, LogItemCmp>& log_entries) {
    std::array<bool, static_cast<size_t>(LogType::Max)> flush_array = {};

    for (auto item : log_entries) {
        if (item->flush_pr) {
            Flush();
            item->flush_pr.value().set_value();
            // this is a flush item, so we cann't push it into completed queue
            continue;
        }
        auto log = log_array_[static_cast<int>(item->log_type)];
        if (!log) {
            item->logger->CompleteLogItem(item);
            continue;
        }
        flush_array[static_cast<int>(item->log_type)] = true;

        auto sinks = log->sinks();
        for (int i = 0; i < static_cast<int>(sinks.size()); i++) {
            try {
                // 如果磁盘故障, 这里写日志可能会抛出异常, 需要忽略掉
                sinks[i]->log(item->log_msg);
            } catch (...) {
            }
        }
        item->logger->CompleteLogItem(item);
    }
    for (int i = 0; i < static_cast<int>(LogType::Max); i++) {
        if (!flush_array[i]) {
            continue;
        }
        log_array_[i]->flush();
    }
}

void LoggerFactory::DeleteLoggers(const std::vector<int>& deleted_vec) {
    if (deleted_vec.size() == 0) return;
    std::unique_lock<std::mutex> lock(mux_);
    for (int i = static_cast<int>(deleted_vec.size()) - 1; i >= 0; i--) {
        int index = deleted_vec[i];
        delete logger_vec_[index];
        if (index != static_cast<int>(logger_vec_.size()) - 1) {
            logger_vec_[index] = logger_vec_.back();
        }
        logger_vec_.pop_back();
    }
}

void LoggerFactory::Run() {
    InitLogThread();
    while (!stopped_.load(std::memory_order_relaxed)) {
        std::set<LogItem*, LogItemCmp> log_entries;
        std::vector<int> deleted_vec;
        RearrangeLogItems(log_entries, deleted_vec);
        if (log_entries.empty() && deleted_vec.empty()) {
            uint64_t count;
            auto r = ::read(fd_, &count, sizeof(count));
            assert(r == sizeof(count));
            (void)r;
            continue;
        }

        FlushLogItems(log_entries);
        DeleteLoggers(deleted_vec);
    }
}

bool LoggerFactory::ShouldLog(LogType log_type, spdlog::level::level_enum level) {
    if (log_array_[static_cast<int>(log_type)]) [[likely]] {
        return log_array_[static_cast<int>(log_type)]->should_log(level);
    }
    return false;
}

void LoggerFactory::SetLevel(LogType log_type, spdlog::level::level_enum level) {
    if (log_array_[static_cast<int>(log_type)]) [[likely]] {
        return log_array_[static_cast<int>(log_type)]->set_level(level);
    }
}

Logger* LoggerFactory::GetLocalLogger() {
    if (!l_logger_wrap.logger) {
        l_logger_wrap.logger = new Logger(name_, fd_);
        std::unique_lock<std::mutex> lock(mux_);
        logger_vec_.push_back(l_logger_wrap.logger);
    }
    return l_logger_wrap.logger;
}

}  // namespace blobstore
