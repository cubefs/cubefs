#pragma once

#include <sys/uio.h>

#include <queue>
#include <seastar/core/condition-variable.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/weak_ptr.hh>

#include "common/net/session.h"
#include "common/status.h"

namespace blobstore {
namespace net {

class TcpStream;
class TcpSession;

using TcpStreamPtr = seastar::shared_ptr<TcpStream>;

class TcpStream : public Stream {
    unsigned shard_;
    uint32_t id_;
    uint32_t sess_id_;
    uint8_t ver_;
    uint32_t frame_size_;
    uint32_t remote_wnd_;
    seastar::weak_ptr<TcpSession> sess_;
    seastar::socket_address local_addr_;
    seastar::socket_address remote_addr_;
    std::queue<seastar::temporary_buffer<char>> buffers_;
    size_t buffer_size_ = 0;
    seastar::condition_variable r_cv_;
    bool has_fin_ = false;
    seastar::gate gate_;

    uint32_t recv_bytes_ = 0;
    uint32_t sent_bytes_ = 0;
    uint32_t incr_ = 0;
    uint32_t remote_consumed_ = 0;
    seastar::condition_variable wnd_cv_;

    friend class TcpSession;

   private:
    seastar::future<> WaitSess();

    seastar::future<Status<>> WaitRead(std::chrono::milliseconds timeout);

    void PushData(seastar::temporary_buffer<char> data);

    void Fin();

    seastar::future<Status<>> SendWindowUpdate(uint32_t consumed);

    seastar::future<> SessionClose();

    void Update(uint32_t consumed, uint32_t window);

    seastar::future<Status<Buffer>> ReadFrameInternal(std::chrono::milliseconds timeout);

   public:
    explicit TcpStream(uint32_t id, uint8_t ver, uint32_t frame_size, uint32_t wnd_size,
                       seastar::weak_ptr<TcpSession> sess);

    virtual ~TcpStream() {}

    static StreamPtr MakeStream(uint32_t id, uint8_t ver, uint32_t frame_size, uint32_t wnd_size,
                                seastar::weak_ptr<TcpSession> sess);

    inline uint32_t ID() const { return id_; }

    inline uint64_t SessID() const { return sess_id_; }

    bool Valid() const;

    uint32_t MaxFrameSize() const { return frame_size_; }

    seastar::future<Status<Buffer>> ReadFrame(
        std::chrono::milliseconds timeout = std::chrono::milliseconds::zero());

    seastar::future<Status<>> WriteFrame(const char* b, size_t n);
    seastar::future<Status<>> WriteFrame(std::vector<iovec> iov);
    seastar::future<Status<>> WriteFrame(Buffer b);
    seastar::future<Status<>> WriteFrame(std::vector<Buffer> buffers);

    inline seastar::socket_address LocalAddress() const { return local_addr_; }
    inline seastar::socket_address RemoteAddress() const { return remote_addr_; }

    seastar::future<> Close();
};

}  // namespace net
}  // namespace blobstore
