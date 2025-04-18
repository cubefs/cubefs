#include "status.h"

#include <unordered_map>

namespace blobstore {

static thread_local char err_buf[128];

static std::unordered_map<ErrCode, const char*> codeMaps = {
    {ErrCode::OK, "OK"},
    {ErrCode::ErrEOF, "end of file"},
    {ErrCode::ErrTooLarge, "content too large"},
    {ErrCode::ErrUnknown, "unknown server error"},
};

const char* GetReason(ErrCode code) {
    auto iter = codeMaps.find(code);
    if (iter == codeMaps.end()) {
#if (_POSIX_C_SOURCE >= 200112L) && !_GNU_SOURCE
        (void)strerror_r(static_cast<int>(code), err_buf, sizeof(err_buf));
        return err_buf;
#else
        char* s = strerror_r(static_cast<int>(code), err_buf, sizeof(err_buf));
        return s;
#endif
    }
    return iter->second;
}

}  // namespace blobstore
