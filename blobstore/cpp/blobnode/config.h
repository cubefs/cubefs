#pragma once
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace blobstore {
namespace blobnode {

struct LogConfig {
    std::string file;
    std::string level;
    size_t max_size;
    uint32_t max_files;
};

struct StoreConfig {
    std::string mountpoint;
};

struct Config {
    bool poll_mode = false;
    std::set<unsigned> cpuset;
    std::optional<std::string> memory;
    std::optional<std::string> hugedir;
    uint16_t listen_port = 0;
    std::optional<unsigned> log_cpu;

    std::optional<LogConfig> log;    // normal log
    std::optional<LogConfig> audit;  // audit log
    std::vector<StoreConfig> store_vec;
};

Config ParseConfigFile(const std::string& path);

}  // namespace blobnode
}  // namespace blobstore
