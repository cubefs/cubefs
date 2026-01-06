#pragma once

#include <cstdint>
#include <cstring>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <vector>

#include "common/net/session.h"
#include "common/status.h"
#include "common/util.h"

namespace blobstore {
namespace net {

// Mock Stream for testing
class MockStream : public Stream {
   private:
    uint32_t id_ = 1;
    uint64_t sess_id_ = 1;

   public:
    uint32_t ID() const override { return id_; }
    uint64_t SessID() const override { return sess_id_; }
    uint32_t MaxFrameSize() const override { return 128 * 1024; }

    seastar::future<Status<Buffer>> ReadFrame(std::chrono::milliseconds timeout) override {
        return seastar::make_ready_future<Status<Buffer>>(Status<Buffer>(ErrCode::OK));
    }

    seastar::future<Status<>> WriteFrame(const char* b, size_t n) override {
        Buffer buf(n);
        std::memcpy(buf.get_write(), b, n);
        written_bodies_.push_back(std::move(buf));
        return seastar::make_ready_future<Status<>>(Status<>(ErrCode::OK));
    }

    seastar::future<Status<>> WriteFrame(std::vector<iovec> iov) override {
        size_t total_size = 0;
        for (const auto& v : iov) {
            total_size += v.iov_len;
        }
        Buffer buf(total_size);
        char* ptr = buf.get_write();
        for (const auto& v : iov) {
            std::memcpy(ptr, v.iov_base, v.iov_len);
            ptr += v.iov_len;
        }
        written_bodies_.push_back(std::move(buf));
        return seastar::make_ready_future<Status<>>(Status<>(ErrCode::OK));
    }

    seastar::future<Status<>> WriteFrame(seastar::temporary_buffer<char> b) override {
        Buffer buf(b.size());
        std::memcpy(buf.get_write(), b.get(), b.size());
        written_bodies_.push_back(std::move(buf));
        return seastar::make_ready_future<Status<>>(Status<>(ErrCode::OK));
    }

    seastar::future<Status<>> WriteFrame(std::vector<Buffer> buffers) override {
        size_t total_size = 0;
        for (const auto& buf : buffers) {
            total_size += buf.size();
        }
        Buffer combined(total_size);
        char* ptr = combined.get_write();
        for (const auto& buf : buffers) {
            std::memcpy(ptr, buf.get(), buf.size());
            ptr += buf.size();
        }
        written_bodies_.push_back(std::move(combined));
        return seastar::make_ready_future<Status<>>(Status<>(ErrCode::OK));
    }

    seastar::socket_address LocalAddress() const override {
        return seastar::socket_address(seastar::ipv4_addr("127.0.0.1", 8080));
    }

    seastar::socket_address RemoteAddress() const override {
        return seastar::socket_address(seastar::ipv4_addr("127.0.0.1", 0));
    }

    bool Valid() const override { return true; }

    seastar::future<> Close() override { return seastar::make_ready_future<>(); }

    std::vector<Buffer> written_bodies_;
};

}  // namespace net
}  // namespace blobstore
