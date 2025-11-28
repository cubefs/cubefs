#include <fmt/format.h>

#include <chrono>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>

#include "common/logger.h"
#include "common/net/rpc.h"
#include "common/net/rpc_server.h"
#include "common/status.h"
#include "demo.h"

namespace bpo = boost::program_options;

namespace {  // valid scope in local file
using ::blobstore::ErrCode;
using ::blobstore::FutureStatus;
using ::blobstore::Status;
using ::blobstore::net::RpcRequestHeader;
using ::blobstore::net::RpcResponseHeader;
using ::blobstore::net::RpcServerContext;
}  // namespace

FutureStatus<> Middleware1(RpcServerContext* ctx) {
    RpcRequestHeader& req_header = ctx->GetRpcRequestHeader();
    auto& trace = ctx->Trace();
    trace.Append("mid1", seastar::lowres_clock::duration(1111));
    LOG_INFO("{} middleware-1: processing request [{}]{}", trace.TraceID(),
             req_header.RemotePathIndex(), req_header.RemotePath());
    Status<> s;
    co_return s;
}

FutureStatus<> Middle(RpcServerContext* ctx) { throw std::runtime_error("should not be here"); }

class SimpleService {
   public:
    FutureStatus<> Middleware2(RpcServerContext* ctx) {
        Status<> s;
        RpcRequestHeader& req_header = ctx->GetRpcRequestHeader();
        if (req_header.RemotePathIndex() == +RoutePathIndex::Middle) {
            s.SetCode(ErrCode::ErrConflict).SetReason("demo: CustomStop");
            co_return s;
        }
        auto& trace = ctx->Trace();
        trace.Append("middle2");
        trace.Append("mid2", seastar::lowres_clock::duration(22222));
        LOG_INFO("{} middleware-2: processing request [{}]{}", trace.TraceID(),
                 req_header.RemotePathIndex(), req_header.RemotePath());
        co_return s;
    }

    FutureStatus<> HandlePing(RpcServerContext* ctx) {
        Status<> s;
        auto& trace = ctx->Trace();
        auto start = seastar::lowres_clock::now();
        LOG_INFO("{} handlePing: remote: {}", trace.TraceID(), ctx->RemoteAddress());

        RpcResponseHeader resp_header;
        resp_header.SetCode(ErrCode::OK);
        s = co_await ctx->WriteHeader(std::move(resp_header));

        trace.Append("ping", start);
        LOG_INFO("TRACE[{}]: {}", trace.TraceID(), trace);
        co_return s;
    }

    FutureStatus<> HandleKick(RpcServerContext* ctx) {
        Status<> s;
        RpcRequestHeader& req_header = ctx->GetRpcRequestHeader();
        auto& trace = ctx->Trace();

        auto start = std::chrono::steady_clock::now();

        blobstore::Trace kick_trace = blobstore::Trace(trace.TraceID());
        kick_trace.Append("kick-simple", "start");

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
        kick_trace.Append("kick", start);
        LOG_INFO("{} handleKick: recv message from remote: {}, content_len={}", trace.TraceID(),
                 ctx->RemoteAddress(), req_header.ContentLength());

        RpcResponseHeader resp_header;
        s = co_await ctx->WriteHeader(std::move(resp_header));

        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - start);
        trace.Append("kick", fmt::format("{}ns", duration.count()));
        LOG_INFO("TRACE[{}]: {}", trace.TraceID(), trace);
        LOG_INFO("KICK-[{}]: {}", kick_trace.TraceID(), kick_trace);
        co_return s;
    }

    FutureStatus<> HandleError(RpcServerContext* ctx) {
        RpcResponseHeader resp_header;
        resp_header.SetCode(567);
        resp_header.SetReason(std::string("demo: CustomError"));
        co_return co_await ctx->WriteHeader(std::move(resp_header));
    }
};

seastar::future<> test_server(uint16_t port) {
    std::unique_ptr<SimpleService> service = std::make_unique<SimpleService>();
    std::unique_ptr<blobstore::net::TcpRpcServer> rpc_server =
        std::make_unique<blobstore::net::TcpRpcServer>(blobstore::net::Option(), "0.0.0.0", port);

    rpc_server->AddMiddleware(Middleware1);
    rpc_server->AddMiddleware([s = service.get()](RpcServerContext* ctx) -> FutureStatus<> {
        return s->Middleware2(ctx);
    });
    rpc_server->RegisterHandler(+RoutePathIndex::Middle, Middle);
    rpc_server->RegisterHandler(+RoutePathIndex::Ping,
                                [s = service.get()](RpcServerContext* ctx) -> FutureStatus<> {
                                    return s->HandlePing(ctx);
                                });
    rpc_server->RegisterHandler(+RoutePathIndex::Kick,
                                [s = service.get()](RpcServerContext* ctx) -> FutureStatus<> {
                                    return s->HandleKick(ctx);
                                });
    rpc_server->RegisterHandler(+RoutePathIndex::Error,
                                [s = service.get()](RpcServerContext* ctx) -> FutureStatus<> {
                                    return s->HandleError(ctx);
                                });

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
    opts.smp_opts.smp.set_value(2);
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
