#include "config.h"

#include <boost/program_options.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <seastar/util/conversions.hh>

namespace blobstore {
namespace blobnode {

static StoreConfig ParseStoreConfig(const boost::property_tree::ptree& pt) {
    StoreConfig c;
    c.mountpoint = pt.get<std::string>("mountpoint");
    return c;
}

static LogConfig ParseLogConfig(const boost::property_tree::ptree& pt) {
    LogConfig c;
    c.file = pt.get<std::string>("file");
    c.level = pt.get<std::string>("level", "info");
    c.max_size = seastar::parse_memory_size(pt.get<std::string>("max_size", "1GB"));
    c.max_files = pt.get<uint32_t>("max_files", 10);
    return c;
}

Config ParseConfigFile(const std::string& path) {
    Config cfg;
    boost::property_tree::ptree pt;
    boost::property_tree::read_json(path, pt);

    cfg.poll_mode = pt.get<bool>("poll_mode", false);
    if (auto v = pt.get_optional<std::string>("memory")) {
        cfg.memory = v.value();
    }
    if (auto v = pt.get_optional<std::string>("hugedir")) {
        cfg.hugedir = v.value();
    }
    if (auto cpu_set = pt.get_child_optional("cpuset")) {
        for (auto& it : cpu_set.value()) {
            cfg.cpuset.insert(it.second.get_value<unsigned>());
        }
    }
    cfg.listen_port = pt.get<uint16_t>("listen_port", 8000);  // TODO default port is 8000
    if (auto v = pt.get_optional<unsigned>("log_cpu")) {
        cfg.log_cpu = v.value();
    }
    if (auto child = pt.get_child_optional("log")) {
        cfg.log = ParseLogConfig(child.value());
    }
    if (auto child = pt.get_child_optional("audit")) {
        cfg.audit = ParseLogConfig(child.value());
    }
    if (auto child = pt.get_child_optional("stores")) {
        for (auto& it : child.value()) {
            auto store_cfg = ParseStoreConfig(it.second);
            cfg.store_vec.emplace_back(std::move(store_cfg));
        }
    }
    return cfg;
}

}  // namespace blobnode
}  // namespace blobstore
