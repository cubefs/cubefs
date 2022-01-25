package rpc

import (
	"net/http"

	"github.com/cubefs/blobstore/common/crc32block"
)

type crcDecoder struct{}

var _ ProgressHandler = (*crcDecoder)(nil)

func (*crcDecoder) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	if req.Header.Get(HeaderCrcEncoded) != "" && w.Header().Get(HeaderAckCrcEncoded) == "" {
		decoder := crc32block.NewDecoderReader(req.Body)
		size := req.ContentLength
		if size >= 0 {
			size = crc32block.DecodeSizeWithDefualtBlock(size)
		}
		req.ContentLength = size
		req.Body = &readCloser{decoder, req.Body}
		w.Header().Set(HeaderAckCrcEncoded, "1")
	}
	f(w, req)
}
