#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include "common/logger.h"
#include "common/net/rpc_client.h"

namespace bpo = boost::program_options;

seastar::future<> test_client(std::string host, uint16_t port) {
    auto rpc_client = co_await blobstore::net::RpcClient::MakeRpcClient();
    if (!rpc_client) {
        LOG_ERROR("make rpc client error");
        co_return;
    }

    auto test_fn = [rpc_client_ptr = rpc_client.get(), host, port]() -> seastar::future<> {
        blobstore::Buffer content(4096);
        for (;;) {
            auto res = co_await rpc_client_ptr->MakeRpcClientContext(host, port);
            if (!res) {
                LOG_ERROR("make rpc client context error: {}, host: {} port: {}", res, host, port);
                break;
            }
            auto ctx = std::move(res.Value());
            blobstore::net::RpcRequestHeader req_header;
            std::string trace_id = blobstore::GenerateTraceid();
            req_header.SetTraceid(trace_id);
            req_header.SetContentLength(content.size());
            auto s = co_await ctx->WriteHeader(std::move(req_header));
            if (!s) {
                LOG_ERROR("write header error: {}", s);
                // 发送失败退出
                break;
            }
            s = co_await ctx->WriteBody(content.get(), content.size());
            if (!s) {
                LOG_ERROR("write body error: {}", s);
                // 发送失败退出
                break;
            }

            auto read_res = co_await ctx->ReadHeader();
            if (!read_res) {
                LOG_ERROR("read header error: {}", read_res);
                break;
            }
            auto resp_header = std::move(read_res.Value());
            LOG_INFO("recv response header: ver={} magic={} code={}", resp_header.Version(),
                     resp_header.Magic(), resp_header.Code());
            co_await seastar::sleep(std::chrono::seconds(1));
        }
        co_return;
    };
    co_await seastar::smp::invoke_on_all(std::ref(test_fn));
    co_await rpc_client->Close();
    co_return;
}

int main(int argc, char** argv) {
    boost::program_options::options_description desc;
    desc.add_options()("help,h", "show help message");
    desc.add_options()("host", bpo::value<std::string>(), "Server host");
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
    opts.auto_handle_sigint_sigterm = false;
    opts.reactor_opts.abort_on_seastar_bad_alloc.set_value();
    seastar::app_template app(std::move(opts));
    char* args[1] = {argv[0]};
    return app.run(1, args, [vm = std::move(vm)]() mutable -> seastar::future<> {
        return seastar::async([vm = std::move(vm)]() mutable {
            if (!vm.count("port") || !vm.count("host")) {
                return;
            }
            std::string host = vm["host"].as<std::string>();
            uint16_t port = vm["port"].as<uint16_t>();
            test_client(host, port).get();
        });
    });
}
