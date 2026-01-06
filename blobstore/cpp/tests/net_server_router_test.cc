#include <boost/test/unit_test.hpp>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <string>
#include <vector>

#include "common/net/rpc.h"
#include "common/net/rpc_server.h"
#include "common/status.h"
#include "common/util.h"
#include "net_mock_stream.h"

namespace {

using blobstore::Buffer;
using blobstore::ErrCode;
using blobstore::GenerateTraceid;
using blobstore::Status;
using blobstore::net::DeserializeRpcHeader;
using blobstore::net::MockStream;
using blobstore::net::RpcRequestHeader;
using blobstore::net::RpcResponseHeader;
using blobstore::net::RpcServer;
using blobstore::net::RpcServerContext;

class TestRpcServer : public RpcServer {
   public:
    seastar::future<> Start() override { return seastar::make_ready_future<>(); }
    seastar::future<> Close() override { return seastar::make_ready_future<>(); }

    using RpcServer::HandleContext;
};

std::unique_ptr<RpcServerContext> CreateTestContext(MockStream* stream, int32_t path_index,
                                                    const std::string& path = "/test") {
    RpcRequestHeader req_header;
    req_header.SetRemotePathIndex(path_index);
    req_header.SetRemotePath(path);
    req_header.SetTraceid(GenerateTraceid());
    return std::make_unique<RpcServerContext>(stream, std::move(req_header));
}

}  // namespace

// Test: Register handler and call it successfully
SEASTAR_TEST_CASE(test_register_and_call_handler) {
    TestRpcServer server;
    MockStream stream;
    bool handler_called = false;

    server.RegisterHandler(1,
                           [&handler_called](RpcServerContext* ctx) -> seastar::future<Status<>> {
                               handler_called = true;
                               RpcResponseHeader resp_header;
                               resp_header.SetStatus(ErrCode::OK);
                               resp_header.SetReason(std::string("success"));
                               co_return co_await ctx->WriteHeader(std::move(resp_header));
                           });

    auto ctx = CreateTestContext(&stream, 1);
    auto result = co_await server.HandleContext(ctx.get());

    BOOST_REQUIRE(result.OK());
    BOOST_REQUIRE(handler_called);
    BOOST_REQUIRE_EQUAL(stream.written_bodies_.size(), 1u);
}

// Test: Unregistered route returns 404
SEASTAR_TEST_CASE(test_unregistered_route_returns_404) {
    TestRpcServer server;
    MockStream stream;

    auto ctx = CreateTestContext(&stream, 999);
    auto result = co_await server.HandleContext(ctx.get());

    BOOST_REQUIRE(result.OK());
    BOOST_REQUIRE_EQUAL(stream.written_bodies_.size(), 1u);

    RpcResponseHeader resp_header;
    Buffer& buf = stream.written_bodies_[0];
    size_t body_offset = 0;
    BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));
    BOOST_REQUIRE_EQUAL(resp_header.Status(), static_cast<int>(ErrCode::ErrNotFound));
    BOOST_REQUIRE(!resp_header.Reason().empty());
}

// Test: Middleware executes before handler
SEASTAR_TEST_CASE(test_middleware_executes_before_handler) {
    TestRpcServer server;
    MockStream stream;
    std::vector<int> execution_order;

    server.AddMiddleware([&execution_order](RpcServerContext* ctx) -> seastar::future<Status<>> {
        execution_order.push_back(1);  // Middleware executes first
        co_return Status<>(ErrCode::OK);
    });
    server.RegisterHandler(2,
                           [&execution_order](RpcServerContext* ctx) -> seastar::future<Status<>> {
                               execution_order.push_back(2);  // Handler executes second
                               RpcResponseHeader resp_header;
                               resp_header.SetStatus(ErrCode::OK);
                               co_return co_await ctx->WriteHeader(std::move(resp_header));
                           });

    auto ctx = CreateTestContext(&stream, 2);
    auto result = co_await server.HandleContext(ctx.get());

    BOOST_REQUIRE(result.OK());
    BOOST_REQUIRE_EQUAL(execution_order.size(), 2u);
    BOOST_REQUIRE_EQUAL(execution_order[0], 1);  // Middleware first
    BOOST_REQUIRE_EQUAL(execution_order[1], 2);  // Handler second
}

// Test: Multiple middlewares execute in order
SEASTAR_TEST_CASE(test_multiple_middlewares_execute_in_order) {
    TestRpcServer server;
    MockStream stream;
    std::vector<int> execution_order;

    server.AddMiddleware([&execution_order](RpcServerContext* ctx) -> seastar::future<Status<>> {
        execution_order.push_back(1);
        co_return Status<>(ErrCode::OK);
    });
    server.AddMiddleware([&execution_order](RpcServerContext* ctx) -> seastar::future<Status<>> {
        execution_order.push_back(2);
        co_return Status<>(ErrCode::OK);
    });
    server.AddMiddleware([&execution_order](RpcServerContext* ctx) -> seastar::future<Status<>> {
        execution_order.push_back(3);
        co_return Status<>(ErrCode::OK);
    });
    server.RegisterHandler(3,
                           [&execution_order](RpcServerContext* ctx) -> seastar::future<Status<>> {
                               execution_order.push_back(4);
                               RpcResponseHeader resp_header;
                               resp_header.SetStatus(ErrCode::OK);
                               co_return co_await ctx->WriteHeader(std::move(resp_header));
                           });

    auto ctx = CreateTestContext(&stream, 3);
    auto result = co_await server.HandleContext(ctx.get());

    BOOST_REQUIRE(result.OK());
    BOOST_REQUIRE_EQUAL(execution_order.size(), 4u);
    BOOST_REQUIRE_EQUAL(execution_order[0], 1);
    BOOST_REQUIRE_EQUAL(execution_order[1], 2);
    BOOST_REQUIRE_EQUAL(execution_order[2], 3);
    BOOST_REQUIRE_EQUAL(execution_order[3], 4);
}

// Test: Middleware error stops execution
SEASTAR_TEST_CASE(test_middleware_error_stops_execution) {
    TestRpcServer server;
    MockStream stream;
    bool handler_called = false;
    bool middleware2_called = false;

    server.AddMiddleware([](RpcServerContext* ctx) -> seastar::future<Status<>> {
        co_return Status<>(ErrCode::OK);
    });
    server.AddMiddleware([&middleware2_called](RpcServerContext* ctx) -> seastar::future<Status<>> {
        middleware2_called = true;
        co_return Status<>(ErrCode::ErrInvalid, "middleware error");
    });
    server.RegisterHandler(4,
                           [&handler_called](RpcServerContext* ctx) -> seastar::future<Status<>> {
                               handler_called = true;
                               RpcResponseHeader resp_header;
                               resp_header.SetStatus(ErrCode::OK);
                               co_return co_await ctx->WriteHeader(std::move(resp_header));
                           });

    auto ctx = CreateTestContext(&stream, 4);
    auto result = co_await server.HandleContext(ctx.get());

    BOOST_REQUIRE(result.OK());
    BOOST_REQUIRE(middleware2_called);
    BOOST_REQUIRE(!handler_called);  // Handler should not be called
    BOOST_REQUIRE_EQUAL(stream.written_bodies_.size(), 1u);

    RpcResponseHeader resp_header;
    Buffer& buf = stream.written_bodies_[0];
    size_t body_offset = 0;
    BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));
    BOOST_REQUIRE_EQUAL(resp_header.Status(), static_cast<int>(ErrCode::ErrInvalid));
    BOOST_REQUIRE(!resp_header.Reason().empty());
}

// Test: Handler can be replaced
SEASTAR_TEST_CASE(test_handler_can_be_replaced) {
    TestRpcServer server;
    MockStream stream;
    int call_count = 0;

    // Register first handler
    server.RegisterHandler(5, [&call_count](RpcServerContext* ctx) -> seastar::future<Status<>> {
        call_count = 1;
        RpcResponseHeader resp_header;
        resp_header.SetStatus(ErrCode::OK);
        co_return co_await ctx->WriteHeader(std::move(resp_header));
    });
    // Replace with new handler
    server.RegisterHandler(5, [&call_count](RpcServerContext* ctx) -> seastar::future<Status<>> {
        call_count = 2;
        RpcResponseHeader resp_header;
        resp_header.SetStatus(ErrCode::OK);
        co_return co_await ctx->WriteHeader(std::move(resp_header));
    });

    auto ctx = CreateTestContext(&stream, 5);
    auto result = co_await server.HandleContext(ctx.get());

    BOOST_REQUIRE(result.OK());
    BOOST_REQUIRE_EQUAL(call_count, 2);
}

// Test: Multiple handlers for different routes
SEASTAR_TEST_CASE(test_multiple_handlers_different_routes) {
    TestRpcServer server;
    int handler1_called = 0;
    int handler2_called = 0;

    // Register handler for route 10
    server.RegisterHandler(10,
                           [&handler1_called](RpcServerContext* ctx) -> seastar::future<Status<>> {
                               handler1_called++;
                               RpcResponseHeader resp_header;
                               resp_header.SetStatus(ErrCode::OK);
                               co_return co_await ctx->WriteHeader(std::move(resp_header));
                           });
    // Register handler for route 20
    server.RegisterHandler(20,
                           [&handler2_called](RpcServerContext* ctx) -> seastar::future<Status<>> {
                               handler2_called++;
                               RpcResponseHeader resp_header;
                               resp_header.SetStatus(ErrCode::OK);
                               co_return co_await ctx->WriteHeader(std::move(resp_header));
                           });

    // Call route 10
    MockStream stream1;
    auto ctx1 = CreateTestContext(&stream1, 10);
    auto result1 = co_await server.HandleContext(ctx1.get());
    BOOST_REQUIRE(result1.OK());
    BOOST_REQUIRE_EQUAL(handler1_called, 1);
    BOOST_REQUIRE_EQUAL(handler2_called, 0);
    BOOST_REQUIRE_EQUAL(stream1.written_bodies_.size(), 1u);

    // Call route 20
    MockStream stream2;
    auto ctx2 = CreateTestContext(&stream2, 20);
    auto result2 = co_await server.HandleContext(ctx2.get());
    BOOST_REQUIRE(result2.OK());
    BOOST_REQUIRE_EQUAL(handler1_called, 1);
    BOOST_REQUIRE_EQUAL(handler2_called, 1);
    BOOST_REQUIRE_EQUAL(stream2.written_bodies_.size(), 1u);
}

// Test: Handler can access request header
SEASTAR_TEST_CASE(test_handler_accesses_request_header) {
    TestRpcServer server;
    MockStream stream;
    std::string received_path;
    std::string received_trace_id;

    server.RegisterHandler(
        6,
        [&received_path, &received_trace_id](RpcServerContext* ctx) -> seastar::future<Status<>> {
            RpcRequestHeader& req_header = ctx->GetRpcRequestHeader();
            received_path = req_header.RemotePath();
            received_trace_id = req_header.Traceid();
            RpcResponseHeader resp_header;
            resp_header.SetStatus(ErrCode::OK);
            co_return co_await ctx->WriteHeader(std::move(resp_header));
        });

    std::string test_path = "/api/test";
    std::string test_trace_id = "test-trace-123";
    RpcRequestHeader req_header;
    req_header.SetRemotePathIndex(6);
    req_header.SetRemotePath(test_path);
    req_header.SetTraceid(test_trace_id);
    auto ctx = std::make_unique<RpcServerContext>(&stream, std::move(req_header));

    auto result = co_await server.HandleContext(ctx.get());

    BOOST_REQUIRE(result.OK());
    BOOST_REQUIRE_EQUAL(received_path, test_path);
    BOOST_REQUIRE_EQUAL(received_trace_id, test_trace_id);
}
