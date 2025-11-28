#include "status.h"

#include <unordered_map>

namespace blobstore {

static std::unordered_map<ErrCode, const char*> codeMaps = {
    {ErrCode::OK, "OK"},
    {ErrCode::ErrInvalid, "invalid"},
    {ErrCode::ErrNotFound, "not found"},
    {ErrCode::ErrTimeout, "timeout"},
    {ErrCode::ErrConflict, "conflict"},
    {ErrCode::ErrEOF, "end of file"},
    {ErrCode::ErrTooLarge, "too large"},
    {ErrCode::ErrEIO, "I/O error"},
    {ErrCode::ErrClosed, "closed"},
    {ErrCode::ErrUnsupported, "unsupported"},
    {ErrCode::ErrDevice, "dev: device error"},
    {ErrCode::ErrUnknown, "unknown error"},

    {ErrCode::ErrNetwork, "net: rpc network error"},
    {ErrCode::ErrNetworkPipe, "net: broken pipe"},
    {ErrCode::ErrNetworkReset, "net: connection reset"},
    {ErrCode::ErrNetworkProtocol, "net: protocol error"},
};

const char* GetReason(ErrCode code) {
    auto iter = codeMaps.find(code);
    if (iter != codeMaps.end()) {
        return iter->second;
    }
    return codeMaps.at(ErrCode::ErrUnknown);
}

}  // namespace blobstore
