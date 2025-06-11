package flashgroupmanager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/compressor"
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

func parseRequestToGetTaskResponse(r *http.Request) (tr *proto.AdminTask, err error) {
	var body []byte
	if err = r.ParseForm(); err != nil {
		return
	}
	if body, err = io.ReadAll(r.Body); err != nil {
		return
	}
	tr = &proto.AdminTask{}
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(body)))
	decoder.UseNumber()
	err = decoder.Decode(tr)
	return
}

func parseRequestForRaftNode(r *http.Request) (id uint64, host string, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var idStr string
	if idStr = r.FormValue(idKey); idStr == "" {
		err = keyNotFound(idKey)
		return
	}

	if id, err = strconv.ParseUint(idStr, 10, 64); err != nil {
		return
	}
	if host = r.FormValue(addrKey); host == "" {
		err = keyNotFound(addrKey)
		return
	}

	if arr := strings.Split(host, colonSplit); len(arr) < 2 {
		err = unmatchedKey(addrKey)
		return
	}
	return
}

func parseAndExtractSetNodeInfoParams(r *http.Request) (params map[string]interface{}, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	noParams := true
	params = make(map[string]interface{})

	if value = r.FormValue(cfgFlashNodeHandleReadTimeout); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(cfgFlashNodeHandleReadTimeout)
			return
		}
		params[cfgFlashNodeHandleReadTimeout] = val
	}

	if value = r.FormValue(cfgFlashNodeReadDataNodeTimeout); value != "" {
		noParams = false
		val := int64(0)
		val, err = strconv.ParseInt(value, 10, 32)
		if err != nil {
			err = unmatchedKey(cfgFlashNodeReadDataNodeTimeout)
			return
		}
		params[cfgFlashNodeReadDataNodeTimeout] = val
	}

	if noParams {
		err = fmt.Errorf("no key assigned")
		return
	}
	return
}
