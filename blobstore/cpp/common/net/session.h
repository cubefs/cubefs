#pragma once

#include <sys/uio.h>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/weak_ptr.hh>
#include <string>
#include <vector>

#include "common/status.h"
#include "common/util.h"

namespace blobstore {
namespace net {

static const seastar::sstring kErrorPipeSession = "net: broken pipe session has closed";
static const seastar::sstring kErrorPipeClient = "net: broken pipe client has closed";

struct Option {
    bool keep_alive_enable = true;
    std::chrono::seconds keep_alive_interval = std::chrono::seconds(10);
    std::chrono::milliseconds write_timeout = std::chrono::milliseconds(200);
    uint32_t max_frame_size = 128 << 10;  // 128K
};

class BufferAllocator {
   public:
    virtual ~BufferAllocator() = default;
    virtual Buffer Allocate(size_t len) = 0;
};

class Stream {
   public:
    virtual ~Stream() {}
    virtual uint32_t ID() const = 0;
    virtual uint64_t SessID() const = 0;
    virtual uint32_t MaxFrameSize() const = 0;
    virtual seastar::future<Status<Buffer>> ReadFrame(
        std::chrono::milliseconds timeout = std::chrono::milliseconds::zero()) = 0;

    virtual seastar::future<Status<>> WriteFrame(const char *b, size_t n) = 0;
    virtual seastar::future<Status<>> WriteFrame(std::vector<iovec> iov) = 0;
    virtual seastar::future<Status<>> WriteFrame(seastar::temporary_buffer<char> b) = 0;
    virtual seastar::future<Status<>> WriteFrame(std::vector<Buffer> buffers) = 0;

    virtual seastar::socket_address LocalAddress() const = 0;

    virtual seastar::socket_address RemoteAddress() const = 0;

    virtual bool Valid() const = 0;

    virtual seastar::future<> Close() = 0;
};

using StreamPtr = seastar::shared_ptr<Stream>;

class Session {
   public:
    virtual ~Session() {}
    virtual uint64_t ID() const = 0;
    virtual bool Valid() const = 0;
    virtual seastar::future<Status<StreamPtr>> OpenStream() = 0;
    virtual seastar::future<Status<StreamPtr>> AcceptStream() = 0;
    virtual size_t Streams() const = 0;
    virtual seastar::socket_address LocalAddress() const = 0;
    virtual seastar::socket_address RemoteAddress() const = 0;
    virtual seastar::future<> Close() = 0;
};

using SessionPtr = seastar::shared_ptr<Session>;

}  // namespace net
}  // namespace blobstore
