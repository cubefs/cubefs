#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include "common/logger.h"
#include "common/net/rpc.h"
#include "common/net/rpc_server.h"
#include "common/net/tcp_connection.h"
#include "common/net/tcp_session.h"
#include "common/status.h"

namespace bpo = boost::program_options;

class SimpleService : public blobstore::net::RpcService {
   public:
    seastar::future<blobstore::Status<>> HandleMessage(
        blobstore::net::RpcServerContext* ctx) override {
        blobstore::Status<> s;
        blobstore::net::RpcRequestHeader& req_header = ctx->GetRpcRequestHeader();
        int64_t n = req_header.ContentLength();
        while (n > 0) {
            auto res = co_await ctx->ReadBody();
            if (!res) {
                s.SetCode(res.Code()).SetReason(res.Reason());
                co_return s;
            }

            auto b = std::move(res.Value());
            n -= b.size();
        }
        LOG_INFO("recv a message from remote: {}, traceid={}, content_len={}", ctx->RemoteAddress(),
                 req_header.Traceid(), req_header.ContentLength());
        blobstore::net::RpcResponseHeader resp_header;
        s = co_await ctx->WriteHeader(std::move(resp_header));
        co_return s;
    }

    seastar::future<> Close() override { return seastar::make_ready_future<>(); }
};

seastar::future<> test_server(uint16_t port) {
    std::unique_ptr<SimpleService> service = std::make_unique<SimpleService>();
    std::unique_ptr<blobstore::net::TcpRpcServer> rpc_server =
        std::make_unique<blobstore::net::TcpRpcServer>(blobstore::net::Option(), "0.0.0.0", port,
                                                       service.get());
    co_await rpc_server->Start();
    co_await rpc_server->Close();
    co_return;
}

int main(int argc, char** argv) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("port", bpo::value<uint16_t>(), "Server port");
    // desc.add_options()("cpu", bpo::value<unsigned>()->default_value(2),
    //                  "bind cpu");

    bpo::variables_map vm;
    try {
        bpo::store(bpo::command_line_parser(argc, argv).options(desc).run(), vm);
        bpo::notify(vm);
    } catch (std::exception& e) {
        std::cout << "parse command line error: " << e.what() << std::endl;
        return -1;
    }

    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 0;
    }

    seastar::app_template::seastar_options opts;
    // opts.smp_opts.smp.set_value(1);
    // opts.smp_opts.cpuset.set_value({vm["cpu"].as<unsigned>()});
    opts.auto_handle_sigint_sigterm = false;
    opts.reactor_opts.abort_on_seastar_bad_alloc.set_value();
    seastar::app_template app(std::move(opts));
    char* args[1] = {argv[0]};
    return app.run(1, args, [vm = std::move(vm)]() mutable -> seastar::future<> {
        return seastar::async([vm = std::move(vm)]() mutable {
            if (!vm.count("port")) {
                return;
            }
            uint16_t port = vm["port"].as<uint16_t>();
            seastar::smp::invoke_on_all([port] { return test_server(port); }).get();

            return;
        });
    });
}
