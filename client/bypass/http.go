package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	ControlBroadcastRefreshExtents = "/broadcast/refreshExtents"
	ControlReadProcessRegister     = "/readProcess/register"

	portKey		=	"port"
	aliveKey 	= 	"alive"
	volKey		=	"vol"
	inoKey		=	"ino"
)

func GetVersionHandleFunc(w http.ResponseWriter, r *http.Request) {
	var resp = struct {
		Branch string
		Commit string
		Debug  string
		Build  string
	}{
		Branch: BranchName,
		Commit: CommitID,
		Debug:  Debug,
		Build:  fmt.Sprintf("%s %s %s %s", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime),
	}
	var encoded []byte
	encoded, _ = json.Marshal(&resp)
	w.Write(encoded)
	return
}

func (c *client) broadcastRefreshExtentsHandleFunc(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter error: %v", err))
		return
	}
	vol := r.FormValue(volKey)
	ino, err := strconv.ParseUint(r.FormValue(inoKey), 10, 64)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter [%v] error: %v", inoKey, err))
		return
	}
	if vol == c.volName {
		if err = c.ec.RefreshExtentsCache(context.Background(), ino); err != nil {
			log.LogWarnf("broadcastRefreshExtentsHandleFunc: err(%v), refresh ino(%v)", err, ino)
		}
		log.LogInfof("broadcastRefreshExtentsHandleFunc: refresh ino(%v) vol(%v)", ino, vol)
	}
	buildSuccessResp(w, "success")
}

func (c *client) registerReadProcStatusHandleFunc(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter error: %v", err))
		return
	}
	portStr := r.FormValue(portKey)
	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter error: %v", err))
		return
	}
	alive, err := strconv.ParseBool(r.FormValue(aliveKey))
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter [%v] error: %v", aliveKey, err))
		return
	}
	if port != c.listenPort {
		c.readProcMapLock.Lock()
		if alive {
			c.readProcErrMap[port] = 0
		} else {
			delete(c.readProcErrMap, port)
		}
		log.LogInfof("registerReadProcStatusHandleFunc: set read port(%v), alive(%v), readProcessMap(%v)", port, alive, c.readProcErrMap)
		c.readProcMapLock.Unlock()
	}
	buildSuccessResp(w, "success")
}

func (c *client) broadcastRefreshExtents(port uint64, vol string, inode uint64)  {
	url := fmt.Sprintf("http://127.0.0.1:%v%v?%v=%v&%v=%v", port, ControlBroadcastRefreshExtents, volKey, vol, inoKey, inode)
	if reply, err := sendURL(url); err != nil {
		c.readProcErrMap[port]++
		log.LogWarnf("broadcastRefreshExtents: failed, send url(%v) err(%v) reply(%v)", url, err, reply)
	}
}

func (c *client) registerReadProcStatus(readPort uint64, alive bool) {
	// For read process, 'c.profPort[0]' is port of write process
	url := fmt.Sprintf("http://127.0.0.1:%v%v?%v=%v&%v=%v", c.profPort[0], ControlReadProcessRegister, portKey, readPort, aliveKey, alive)
	if reply, err := sendURL(url); err != nil {
		log.LogWarnf("registerReadProcStatus: failed, send url(%v) err(%v) reply(%v)", url, err, reply)
	}
}

func sendURL(reqURL string) (reply *proto.HTTPReply, err error) {
	client := http.DefaultClient
	client.Timeout = 500 * time.Millisecond
	resp, err := client.Get(reqURL)
	if err != nil {
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("resp status code[%v]", resp.StatusCode)
		return
	}
	reply = &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		return
	}
	if reply.Code != http.StatusOK {
		err = fmt.Errorf("failed,msg[%v],data[%v]", reply.Msg, reply.Data)
		return
	}
	log.LogInfof("client send url(%v) reply(%v)", reqURL, reply)
	return
}

func buildSuccessResp(w http.ResponseWriter, data interface{}) {
	buildJSONResp(w, http.StatusOK, data, "")
}

func buildFailureResp(w http.ResponseWriter, code int, msg string) {
	buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := &proto.HTTPReply{
		Code: int32(code),
		Msg:  msg,
		Data: data,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	_, _ = w.Write(jsonBody)
}