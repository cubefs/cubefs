package flashgroupmaster

import (
	"encoding/json"
	"github.com/cubefs/cubefs/util/compressor"
	"net/http"
	"strconv"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response", r.URL, r.RemoteAddr)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply. URL[%v],remoteAddr[%v] err:[%v]", r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
	}
}

func newSuccessHTTPReply(data interface{}) *proto.HTTPReply {
	return &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: proto.ErrSuc.Error(), Data: data}
}

func newErrHTTPReply(err error) *proto.HTTPReply {
	if err == nil {
		return newSuccessHTTPReply("")
	}

	code, ok := proto.Err2CodeMap[err]
	if ok {
		return &proto.HTTPReply{Code: code, Msg: err.Error()}
	}

	return &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()}
}

func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
		return
	}
}

func sendOkReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) (err error) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply. URL[%v],remoteAddr[%v] err:[%v]", r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}

	if acceptEncoding := r.Header.Get(proto.HeaderAcceptEncoding); acceptEncoding != "" {
		if compressed, errx := compressor.New(acceptEncoding).Compress(reply); errx == nil {
			w.Header().Set(proto.HeaderContentEncoding, acceptEncoding)
			reply = compressed
		}
	}

	send(w, r, reply)
	return
}
