#include "logger.h"

namespace blobstore {

static std::once_flag log_flag;
static LoggerFactory* log_factory = nullptr;
bool LoggerFactory::async_mode_ = false;

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

struct LogItemCmp {
    bool operator()(const LogItem* item1, const LogItem* item2) const {
        return item1->log_msg.time <= item2->log_msg.time;
    }
};

void InitLoggerFactory(const std::string& filename, size_t max_file_size, size_t max_files,
                       unsigned cpu) {
    std::call_once(log_flag, [filename, max_file_size, max_files, cpu]() {
        LoggerFactory* factory =
            new LoggerFactory("log-thread", filename, max_file_size, max_files, cpu);
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

void Logger::Flush() {
    LogItem* item = new LogItem;
    item->logger = this;
    item->log_msg.time = std::chrono::time_point<std::chrono::system_clock>::max();
    item->flush_prom = std::move(std::promise<void>());
    auto fu = item->flush_prom.value().get_future();
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

LoggerFactory::LoggerFactory(std::string name, std::string logfile, size_t max_file_size,
                             size_t max_files, unsigned cpu)
    : name_(name),
      cpu_(cpu),
      log_(spdlog::rotating_logger_mt(name, logfile, max_file_size, max_files)),
      fd_(eventfd(0, EFD_CLOEXEC)),
      thread_([name, this] { Run(name); }) {
    log_->set_pattern("[%Y-%m-%d %H:%M:%S.%f][%l][%s:%#] %v");
}

LoggerFactory::~LoggerFactory() {
    stopped_.store(true, std::memory_order_relaxed);
    thread_.join();
}

void LoggerFactory::Run(std::string name) {
    pthread_setname_np(pthread_self(), name.c_str());
    sigset_t mask;
    sigfillset(&mask);
    auto r = ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
    seastar::throw_pthread_error(r);

    if (cpu_ != -1) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(cpu_, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    }

    while (!stopped_.load(std::memory_order_relaxed)) {
        std::set<LogItem*, LogItemCmp> log_entries;
        std::vector<int> deleted_vec;
        mux_.lock();
        std::vector<Logger*> loggers = logger_vec_;
        mux_.unlock();
        for (int i = 0; i < loggers.size(); i++) {
            loggers[i]->pending_.consume_all([&](LogItem* item) { log_entries.insert(item); });
            loggers[i]->mux_.lock();
            loggers[i]->cv_.notify_all();
            loggers[i]->mux_.unlock();
            if (loggers[i]->closed_) {
                deleted_vec.push_back(i);
            }
        }

        if (log_entries.empty() && deleted_vec.empty()) {
            uint64_t count;
            auto r = ::read(fd_, &count, sizeof(count));
            assert(r == sizeof(count));
            continue;
        }

        for (auto item : log_entries) {
            if (item->flush_prom) {
                log_->flush();
                item->flush_prom.value().set_value();
                // if this is a flush item, we cann't push it into completed
                // queue
                continue;
            }
            auto sinks = log_->sinks();
            for (int i = 0; i < sinks.size(); i++) {
                try {
                    sinks[i]->log(item->log_msg);
                } catch (...) {
                }
            }
            if (!item->logger->completed_.push(item)) {
                delete item;
            }
        }
        log_->flush();

        if (deleted_vec.size() > 0) [[unlikely]] {
            std::unique_lock<std::mutex> lock(mux_);
            for (int i = deleted_vec.size() - 1; i >= 0; i--) {
                int index = deleted_vec[i];
                delete logger_vec_[index];
                if (index != logger_vec_.size() - 1) {
                    logger_vec_[index] = logger_vec_.back();
                }
                logger_vec_.pop_back();
            }
        }
    }
}

bool LoggerFactory::should_log(spdlog::level::level_enum msg_level) {
    return log_->should_log(msg_level);
}

void LoggerFactory::set_level(spdlog::level::level_enum log_level) {
    return log_->set_level(log_level);
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
