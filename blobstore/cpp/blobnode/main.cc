#include <pthread.h>

#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include "common/logger.h"
#include "common/net/rpc_server.h"
#include "config.h"
#include "service.h"
#include "store_handler.h"

namespace bpo = boost::program_options;

namespace blobstore {
namespace blobnode {

static seastar::future<> StartServer(Config cfg, StoreHandlerFactory *store_factory) {
    auto fn = [listen_port = cfg.listen_port, store_factory]() -> seastar::future<> {
        std::unique_ptr<Service> service = std::make_unique<Service>(store_factory);
        std::unique_ptr<net::TcpRpcServer> rpc_server =
            std::make_unique<net::TcpRpcServer>(net::Option(), "0.0.0.0", listen_port);

        /*
        rpc_server->RegisterHandler(
            RoutePathIndex::Ping,
            [s = service.get()](net::RpcServerContext *ctx) -> seastar::future<Status<>> {
                return s->HandlePing(ctx);
            });
            */
        co_await rpc_server->Start();
        co_await rpc_server->Close();
        co_return;
    };

    co_await seastar::smp::invoke_on_all(std::ref(fn));
    co_return;
}

static int Startup(int argc, char **argv) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("config,c", bpo::value<std::string>(), "config file");

    bpo::variables_map vm;
    try {
        bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
        bpo::notify(vm);
    } catch (std::exception &e) {
        std::cout << "parse command line error: " << e.what() << std::endl;
        return -1;
    }

    if (vm.count("help") || !vm.count("config")) {
        std::cout << desc << std::endl;
        return 0;
    }

    Config cfg;
    try {
        cfg = ParseConfigFile(vm["config"].as<std::string>());
    } catch (std::exception &e) {
        LOG_ERROR("parse config file error: {}", e.what());
        return -1;
    }

    if (cfg.log_cpu) {
        InitLogFactory(cfg.log_cpu.value());
    }

    if (cfg.log) {
        InitNormalLog(cfg.log.value().file, cfg.log.value().level, cfg.log.value().max_size,
                      cfg.log.value().max_files);
    }

    if (cfg.audit) {
        InitAuditLog(cfg.audit.value().file, cfg.audit.value().max_size,
                     cfg.audit.value().max_files);
    }

    seastar::app_template::seastar_options opts;
    opts.auto_handle_sigint_sigterm = false;
    opts.reactor_opts.abort_on_seastar_bad_alloc.set_value();
    if (cfg.poll_mode) opts.reactor_opts.poll_mode.set_value();
    if (!cfg.cpuset.empty()) {
        opts.smp_opts.smp.set_value(cfg.cpuset.size());
        opts.smp_opts.cpuset.set_value(cfg.cpuset);
    }
    if (cfg.memory) opts.smp_opts.memory.set_value(cfg.memory.value());
    if (cfg.hugedir) opts.smp_opts.hugepages.set_value(cfg.hugedir.value());

    seastar::app_template app(std::move(opts));
    char *args[1] = {argv[0]};
    return app.run(1, args, [cfg]() -> seastar::future<> {
        return seastar::async([cfg]() {
            auto res = StoreHandlerFactory::Create(cfg.store_vec).get();
            if (!res) {
                return;
            }
            auto store_factory = std::move(res.Value());
            StartServer(cfg, store_factory.get()).get();
            store_factory->Close().get();
        });
    });
}

}  // namespace blobnode
}  // namespace blobstore

int main(int argc, char **argv) { return blobstore::blobnode::Startup(argc, argv); }
