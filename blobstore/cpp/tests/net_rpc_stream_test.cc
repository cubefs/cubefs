#include <boost/test/unit_test.hpp>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/test_case.hh>
#include <string>
#include <vector>

#include "common/net/rpc.h"
#include "common/net/rpc_stream.h"
#include "common/proto/rpc.pb.h"
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
using blobstore::net::RpcServerContext;
using blobstore::net::RpcServerStream;
using blobstore::net::SerializeRpcHeader;
using blobstore::proto::StreamCmd;

// Helper function to create a RpcServerContext with MockStream
std::unique_ptr<RpcServerContext> CreateTestServerContext(MockStream* stream,
                                                          const std::string& remote_path = "/test",
                                                          int32_t path_index = 1,
                                                          StreamCmd stream_cmd = StreamCmd::PSH) {
    RpcRequestHeader req_header;
    req_header.SetRemotePath(remote_path);
    req_header.SetRemotePathIndex(path_index);
    req_header.SetTraceid(GenerateTraceid());
    req_header.SetStreamCmd(stream_cmd);
    req_header.SetContentLength(0);
    return std::make_unique<RpcServerContext>(stream, std::move(req_header));
}

}  // namespace

// ======================== 基础功能测试 ========================

// Test: RpcServerStream creation
SEASTAR_TEST_CASE(test_rpc_server_stream_creation) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    BOOST_REQUIRE(server_stream != nullptr);

    co_return;
}

// Test: RpcServerStream send header
SEASTAR_TEST_CASE(test_rpc_server_stream_send_header) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Send header with OK status
    auto status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(status.OK());

    // Verify a frame was written
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 1u);

    // Parse the response header
    RpcResponseHeader resp_header;
    size_t body_offset = 0;
    Buffer& buf = mock_stream.written_bodies_[0];
    BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));
    BOOST_REQUIRE_EQUAL(resp_header.Status(), 200);
    BOOST_REQUIRE_EQUAL(resp_header.Reason(), "OK");

    co_return;
}

// Test: RpcServerStream send header with custom headers
SEASTAR_TEST_CASE(test_rpc_server_stream_send_header_with_custom_headers) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Set custom headers
    server_stream->SetHeader("X-Custom-Header", "custom-value");
    server_stream->SetHeader("X-Request-ID", "12345");

    auto status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(status.OK());
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 1u);

    // Parse and verify custom headers
    RpcResponseHeader resp_header;
    size_t body_offset = 0;
    Buffer& buf = mock_stream.written_bodies_[0];
    BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));

    auto header = resp_header.Header();
    BOOST_REQUIRE_EQUAL(header.Get("X-Custom-Header"), "custom-value");
    BOOST_REQUIRE_EQUAL(header.Get("X-Request-ID"), "12345");

    co_return;
}

// Test: RpcServerStream send header only once
SEASTAR_TEST_CASE(test_rpc_server_stream_send_header_only_once) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Send header first time
    auto status1 = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(status1.OK());
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 1u);

    // Try to send header again - should be no-op
    auto status2 = co_await server_stream->SendHeader(500, "Error");
    BOOST_REQUIRE(status2.OK());
    // Should still have only one frame (header not sent again)
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 1u);

    co_return;
}

// Test: RpcServerStream send raw data (new Send API)
SEASTAR_TEST_CASE(test_rpc_server_stream_send_raw_data) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Send header first
    auto header_status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(header_status.OK());

    // Send raw data using Send() method
    std::string test_data = "Hello, World!";
    Buffer data_buf(test_data.size());
    std::memcpy(data_buf.get_write(), test_data.data(), test_data.size());

    auto send_status = co_await server_stream->Send(std::move(data_buf));
    BOOST_REQUIRE(send_status.OK());

    // Should have 3 frames: initial header + data response header + body
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 3u);

    // Verify the data frame (status should be 0 for normal stream message)
    RpcResponseHeader data_resp_header;
    size_t body_offset = 0;
    Buffer& data_frame = mock_stream.written_bodies_[1];
    BOOST_REQUIRE(DeserializeRpcHeader(data_frame, data_resp_header, body_offset));
    BOOST_REQUIRE_EQUAL(data_resp_header.ContentLength(), test_data.size());
    BOOST_REQUIRE_EQUAL(data_resp_header.Status(), 0);  // 0 indicates normal stream message

    // Verify body content
    Buffer& body_frame = mock_stream.written_bodies_[2];
    std::string received_data(body_frame.get(), body_frame.size());
    BOOST_REQUIRE_EQUAL(received_data, test_data);

    co_return;
}

// Test: RpcServerStream send multiple messages
SEASTAR_TEST_CASE(test_rpc_server_stream_send_multiple_messages) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Send header
    auto header_status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(header_status.OK());

    // Send multiple messages
    std::vector<std::string> messages = {"message 1", "message 2", "message 3"};

    for (const auto& msg : messages) {
        Buffer buf(msg.size());
        std::memcpy(buf.get_write(), msg.data(), msg.size());
        auto status = co_await server_stream->Send(std::move(buf));
        BOOST_REQUIRE(status.OK());
    }

    // Should have: 1 initial header + 3 * (data header + body) = 7 frames
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 7u);

    co_return;
}

// Test: Send data without sending header first
SEASTAR_TEST_CASE(test_rpc_server_stream_send_data_without_initial_header) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Send data without explicitly calling SendHeader first
    std::string test_data = "data without header";
    Buffer data_buf(test_data.size());
    std::memcpy(data_buf.get_write(), test_data.data(), test_data.size());

    auto send_status = co_await server_stream->Send(std::move(data_buf));
    BOOST_REQUIRE(send_status.OK());

    // Should have 2 frames: data response header + body (no initial header sent)
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 2u);

    // Verify the data was sent with status 0
    RpcResponseHeader resp_header;
    size_t body_offset = 0;
    Buffer& header_frame = mock_stream.written_bodies_[0];
    BOOST_REQUIRE(DeserializeRpcHeader(header_frame, resp_header, body_offset));
    BOOST_REQUIRE_EQUAL(resp_header.Status(), 0);
    BOOST_REQUIRE_EQUAL(resp_header.ContentLength(), test_data.size());

    co_return;
}

// Test: Send empty data
SEASTAR_TEST_CASE(test_rpc_server_stream_send_empty_data) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Send header
    auto header_status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(header_status.OK());

    // Send empty data
    Buffer empty_buf(0);
    auto send_status = co_await server_stream->Send(std::move(empty_buf));
    BOOST_REQUIRE(send_status.OK());

    // Should have 3 frames: initial header + data header + empty body
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 3u);

    // Verify empty content length
    RpcResponseHeader data_resp_header;
    size_t body_offset = 0;
    Buffer& data_frame = mock_stream.written_bodies_[1];
    BOOST_REQUIRE(DeserializeRpcHeader(data_frame, data_resp_header, body_offset));
    BOOST_REQUIRE_EQUAL(data_resp_header.ContentLength(), 0);
    BOOST_REQUIRE_EQUAL(data_resp_header.Status(), 0);

    co_return;
}

// Test: Send large data
SEASTAR_TEST_CASE(test_rpc_server_stream_large_data_transfer) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    auto header_status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(header_status.OK());

    // Send large data (1MB)
    const size_t large_size = 1024 * 1024;
    Buffer large_buf(large_size);
    char* ptr = large_buf.get_write();
    for (size_t i = 0; i < large_size; i++) {
        ptr[i] = static_cast<char>(i % 256);
    }

    auto send_status = co_await server_stream->Send(std::move(large_buf));
    BOOST_REQUIRE(send_status.OK());

    // Should have 3 frames
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 3u);

    // Verify content length
    RpcResponseHeader data_resp_header;
    size_t body_offset = 0;
    Buffer& data_frame = mock_stream.written_bodies_[1];
    BOOST_REQUIRE(DeserializeRpcHeader(data_frame, data_resp_header, body_offset));
    BOOST_REQUIRE_EQUAL(data_resp_header.ContentLength(), large_size);

    co_return;
}

// ======================== 错误处理测试 ========================

// Test: Send header with error status codes
SEASTAR_TEST_CASE(test_rpc_server_stream_error_status_codes) {
    // Test 400 Bad Request
    {
        MockStream mock_stream;
        auto ctx = CreateTestServerContext(&mock_stream);
        auto server_stream = ctx->CreateServerStream();

        auto status = co_await server_stream->SendHeader(400, "Bad Request");
        BOOST_REQUIRE(status.OK());

        RpcResponseHeader resp_header;
        size_t body_offset = 0;
        Buffer& buf = mock_stream.written_bodies_[0];
        BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));
        BOOST_REQUIRE_EQUAL(resp_header.Status(), 400);
        BOOST_REQUIRE_EQUAL(resp_header.Reason(), "Bad Request");
    }

    // Test 500 Internal Server Error
    {
        MockStream mock_stream;
        auto ctx = CreateTestServerContext(&mock_stream);
        auto server_stream = ctx->CreateServerStream();

        auto status = co_await server_stream->SendHeader(500, "Internal Server Error");
        BOOST_REQUIRE(status.OK());

        RpcResponseHeader resp_header;
        size_t body_offset = 0;
        Buffer& buf = mock_stream.written_bodies_[0];
        BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));
        BOOST_REQUIRE_EQUAL(resp_header.Status(), 500);
    }

    co_return;
}

// Test: Send data after error status
SEASTAR_TEST_CASE(test_rpc_server_stream_send_data_after_error_status) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Send error status
    auto header_status = co_await server_stream->SendHeader(500, "Internal Error");
    BOOST_REQUIRE(header_status.OK());

    // Try to send data even after error status (should still work)
    std::string test_data = "error details";
    Buffer data_buf(test_data.size());
    std::memcpy(data_buf.get_write(), test_data.data(), test_data.size());

    auto send_status = co_await server_stream->Send(std::move(data_buf));
    BOOST_REQUIRE(send_status.OK());

    // Should have 3 frames: error header + data header + body
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 3u);

    co_return;
}

// ======================== Header管理测试 ========================

// Test: Set header multiple times (first value is kept)
SEASTAR_TEST_CASE(test_rpc_server_stream_set_header_multiple_times) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Set header multiple times - first value is kept (insert behavior)
    server_stream->SetHeader("X-Version", "1.0");
    server_stream->SetHeader("X-Version", "2.0");  // This won't overwrite
    server_stream->SetHeader("X-Custom", "value1");
    server_stream->SetHeader("X-Custom", "value2");  // This won't overwrite

    auto status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(status.OK());

    // Parse and verify the first values were kept
    RpcResponseHeader resp_header;
    size_t body_offset = 0;
    Buffer& buf = mock_stream.written_bodies_[0];
    BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));

    auto header = resp_header.Header();
    BOOST_REQUIRE_EQUAL(header.Get("X-Version"), "1.0");
    BOOST_REQUIRE_EQUAL(header.Get("X-Custom"), "value1");

    co_return;
}

// Test: Set header after send should not work
SEASTAR_TEST_CASE(test_rpc_server_stream_set_header_after_send) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Set header before send
    server_stream->SetHeader("X-Before", "before-value");

    // Send header
    auto status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(status.OK());

    // Try to set header after send - should be ignored
    server_stream->SetHeader("X-After", "after-value");

    // Verify only the "before" header exists
    RpcResponseHeader resp_header;
    size_t body_offset = 0;
    Buffer& buf = mock_stream.written_bodies_[0];
    BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));

    auto header = resp_header.Header();
    BOOST_REQUIRE_EQUAL(header.Get("X-Before"), "before-value");
    BOOST_REQUIRE_EQUAL(header.Get("X-After"), "");  // Should be empty

    co_return;
}

// Test: Send large header metadata
SEASTAR_TEST_CASE(test_rpc_server_stream_large_header_metadata) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Set many custom headers
    for (int i = 0; i < 100; i++) {
        std::string key = "X-Header-" + std::to_string(i);
        std::string value = "value-" + std::to_string(i);
        server_stream->SetHeader(key, value);
    }

    auto status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(status.OK());

    // Verify some of the headers
    RpcResponseHeader resp_header;
    size_t body_offset = 0;
    Buffer& buf = mock_stream.written_bodies_[0];
    BOOST_REQUIRE(DeserializeRpcHeader(buf, resp_header, body_offset));

    auto header = resp_header.Header();
    BOOST_REQUIRE_EQUAL(header.Get("X-Header-0"), "value-0");
    BOOST_REQUIRE_EQUAL(header.Get("X-Header-50"), "value-50");
    BOOST_REQUIRE_EQUAL(header.Get("X-Header-99"), "value-99");

    co_return;
}

// ======================== 边界和压力测试 ========================

// Test: Send data with special characters
SEASTAR_TEST_CASE(test_rpc_server_stream_send_data_with_special_chars) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    auto header_status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(header_status.OK());

    // Send data with special characters including null bytes
    std::string test_data = "Hello\0World\n\r\t";
    test_data += std::string({'\x01', '\x02', '\xFF'});

    Buffer data_buf(test_data.size());
    std::memcpy(data_buf.get_write(), test_data.data(), test_data.size());

    auto send_status = co_await server_stream->Send(std::move(data_buf));
    BOOST_REQUIRE(send_status.OK());

    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 3u);

    Buffer& body_frame = mock_stream.written_bodies_[2];
    BOOST_REQUIRE_EQUAL(body_frame.size(), test_data.size());

    co_return;
}

// Test: Rapid sequential sends
SEASTAR_TEST_CASE(test_rpc_server_stream_rapid_sequential_sends) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    auto header_status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(header_status.OK());

    // Send many small messages rapidly
    const int num_messages = 50;
    for (int i = 0; i < num_messages; i++) {
        std::string data = "msg-" + std::to_string(i);
        Buffer buf(data.size());
        std::memcpy(buf.get_write(), data.data(), data.size());

        auto status = co_await server_stream->Send(std::move(buf));
        BOOST_REQUIRE(status.OK());
    }

    // Should have: 1 initial header + 50 * (data header + body) = 101 frames
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 1u + num_messages * 2);

    co_return;
}

// Test: Boundary size data (common buffer sizes)
SEASTAR_TEST_CASE(test_rpc_server_stream_boundary_size_data) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    auto header_status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(header_status.OK());

    // Test common boundary sizes: 1KB, 4KB, 8KB, 16KB
    std::vector<size_t> sizes = {1024, 4096, 8192, 16384};

    for (size_t size : sizes) {
        Buffer buf(size);
        char* ptr = buf.get_write();
        for (size_t i = 0; i < size; i++) {
            ptr[i] = static_cast<char>(i % 256);
        }

        auto status = co_await server_stream->Send(std::move(buf));
        BOOST_REQUIRE(status.OK());
    }

    // Should have: 1 initial header + 4 sizes * 2 frames = 9 frames
    BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 1u + sizes.size() * 2);

    co_return;
}

// Test: Multiple stream instances
SEASTAR_TEST_CASE(test_rpc_server_stream_multiple_instances) {
    MockStream mock_stream1, mock_stream2;
    auto ctx1 = CreateTestServerContext(&mock_stream1, "/stream1", 1);
    auto ctx2 = CreateTestServerContext(&mock_stream2, "/stream2", 2);

    auto stream1 = ctx1->CreateServerStream();
    auto stream2 = ctx2->CreateServerStream();

    // Send data on both streams independently
    stream1->SetHeader("X-Stream", "1");
    stream2->SetHeader("X-Stream", "2");

    auto status1 = co_await stream1->SendHeader(200, "OK");
    auto status2 = co_await stream2->SendHeader(200, "OK");

    BOOST_REQUIRE(status1.OK());
    BOOST_REQUIRE(status2.OK());

    // Verify independent writes
    BOOST_REQUIRE_EQUAL(mock_stream1.written_bodies_.size(), 1u);
    BOOST_REQUIRE_EQUAL(mock_stream2.written_bodies_.size(), 1u);

    // Send data on both streams
    std::string data1 = "stream1 data";
    std::string data2 = "stream2 data";

    Buffer buf1(data1.size());
    Buffer buf2(data2.size());
    std::memcpy(buf1.get_write(), data1.data(), data1.size());
    std::memcpy(buf2.get_write(), data2.data(), data2.size());

    auto send1 = co_await stream1->Send(std::move(buf1));
    auto send2 = co_await stream2->Send(std::move(buf2));

    BOOST_REQUIRE(send1.OK());
    BOOST_REQUIRE(send2.OK());

    BOOST_REQUIRE_EQUAL(mock_stream1.written_bodies_.size(), 3u);
    BOOST_REQUIRE_EQUAL(mock_stream2.written_bodies_.size(), 3u);

    co_return;
}

// ======================== Stream命令测试 ========================

// Test: Context with different stream commands
SEASTAR_TEST_CASE(test_rpc_server_stream_with_different_stream_commands) {
    // Test with PSH command
    {
        MockStream mock_stream;
        auto ctx = CreateTestServerContext(&mock_stream, "/test", 1, StreamCmd::PSH);
        auto server_stream = ctx->CreateServerStream();

        auto status = co_await server_stream->SendHeader(200, "OK");
        BOOST_REQUIRE(status.OK());
        BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 1u);
    }

    // Test with FIN command
    {
        MockStream mock_stream;
        auto ctx = CreateTestServerContext(&mock_stream, "/test", 1, StreamCmd::FIN);
        auto server_stream = ctx->CreateServerStream();

        auto status = co_await server_stream->SendHeader(200, "OK");
        BOOST_REQUIRE(status.OK());
        BOOST_REQUIRE_EQUAL(mock_stream.written_bodies_.size(), 1u);
    }

    co_return;
}

// Test: Verify stream message status code (0 for normal data)
SEASTAR_TEST_CASE(test_rpc_server_stream_message_status_code) {
    MockStream mock_stream;
    auto ctx = CreateTestServerContext(&mock_stream);
    auto server_stream = ctx->CreateServerStream();

    // Send initial header
    auto header_status = co_await server_stream->SendHeader(200, "OK");
    BOOST_REQUIRE(header_status.OK());

    // Send multiple data messages
    for (int i = 0; i < 3; i++) {
        std::string data = "data-" + std::to_string(i);
        Buffer buf(data.size());
        std::memcpy(buf.get_write(), data.data(), data.size());

        auto status = co_await server_stream->Send(std::move(buf));
        BOOST_REQUIRE(status.OK());
    }

    // Verify all data messages have status code 0
    for (size_t i = 1; i < mock_stream.written_bodies_.size(); i += 2) {
        RpcResponseHeader resp_header;
        size_t body_offset = 0;
        Buffer& frame = mock_stream.written_bodies_[i];
        BOOST_REQUIRE(DeserializeRpcHeader(frame, resp_header, body_offset));
        BOOST_REQUIRE_EQUAL(resp_header.Status(), 0);  // Normal stream message
    }

    co_return;
}
