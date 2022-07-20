package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	ControlBroadcastRefreshExtents = "/broadcast/refreshExtents"
	ControlReadProcessRegister     = "/readProcess/register"
	ControlGetReadProcs            = "/get/readProcs"
	ControlSetUpgrade              = "/set/clientUpgrade"
	ControlUnsetUpgrade            = "/unset/clientUpgrade"

	aliveKey   = "alive"
	volKey     = "vol"
	inoKey     = "ino"
	clientKey  = "client" // ip:port
	versionKey = "version"
	MaxRetry   = 5
)

var (
	CommitID    string
	BranchName  string
	BuildTime   string
	Debug       string
	NextVersion string
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

func broadcastRefreshExtentsHandleFunc(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
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

func (c *fClient) SetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Parse parameter error: %v", err))
		return
	}
	version := r.FormValue(versionKey)
	if version == "" {
		buildFailureResp(w, http.StatusBadRequest, "Invalid version parameter.")
		return
	}
	if version == CommitID {
		buildFailureResp(w, http.StatusBadRequest, "Current version is same to expected.")
		return
	}
	if NextVersion != "" && version != NextVersion {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Last version %s is upgrading. please waiting.", NextVersion))
		return
	}
	if version == NextVersion {
		buildSuccessResp(w, "Please waiting")
		return
	}
	if err := setClientUpgrade(c.mc, version); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	buildSuccessResp(w, "Set successful. Upgrading.")
	return
}

func SetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	if err := r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Parse parameter error: %v", err))
		return
	}
	version := r.FormValue(versionKey)
	if version == "" {
		buildFailureResp(w, http.StatusBadRequest, "Invalid version parameter.")
		return
	}
	if version == CommitID {
		buildFailureResp(w, http.StatusBadRequest, "Current version is same to expected.")
		return
	}
	if NextVersion != "" && version != NextVersion {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Last version %s is upgrading. please waiting.", NextVersion))
		return
	}
	if version == NextVersion {
		buildSuccessResp(w, "Please waiting")
		return
	}
	if err := setClientUpgrade(c.mc, version); err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	buildSuccessResp(w, "Set successful. Upgrading.")
	return
}

func setClientUpgrade(mc *master.MasterClient, version string) (err error) {
	if version == "test" {
		os.Setenv("RELOAD_CLIENT", version)
		return
	}
	NextVersion = version
	defer func() {
		if err != nil {
			NextVersion = ""
		}
	}()
	addr, err := mc.ClientAPI().GetClientPkgAddr()
	if err != nil {
		return
	}
	filename := fmt.Sprintf("libcfssdk_%s.so", version)
	var url string
	if addr[len(addr)-1] == '/' {
		url = fmt.Sprintf("%s%s", addr, filename)
	} else {
		url = fmt.Sprintf("%s/%s", addr, filename)
	}
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("Download %s error: %v", url, err)
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("Download %s error: %s", url, resp.Status)
	}
	defer resp.Body.Close()

	tmpfile := fmt.Sprintf("/tmp/.%s", filename)
	os.Remove(tmpfile)
	out, err := os.Create(tmpfile)
	if err != nil {
		return fmt.Errorf("Create %s error: %v", tmpfile, err)
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return fmt.Errorf("Write %s err: %v", tmpfile, err)
	}
	os.Rename(tmpfile, "/usr/lib64/libcfssdk.so")
	os.Setenv("RELOAD_CLIENT", "1")
	return
}

func UnsetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	os.Unsetenv("RELOAD_CLIENT")
	NextVersion = ""
	buildSuccessResp(w, "Success")
}

func registerReadProcStatusHandleFunc(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	if err := r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter error: %v", err))
		return
	}
	readClient := r.FormValue(clientKey)
	addr := strings.Split(readClient, ":")
	if len(addr) != 2 {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("invalid parameter %s", clientKey))
		return
	}
	if addr[0] == "" {
		readClient = strings.Split(r.RemoteAddr, ":")[0] + readClient
	}
	_, err := strconv.ParseUint(addr[1], 10, 64)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse port error: %v", err))
		return
	}
	alive, err := strconv.ParseBool(r.FormValue(aliveKey))
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter [%v] error: %v", aliveKey, err))
		return
	}

	c.readProcMapLock.Lock()
	if alive {
		c.readProcErrMap[readClient] = 0
		log.LogInfof("registerReadClient: %s", readClient)
	} else {
		delete(c.readProcErrMap, readClient)
		log.LogInfof("unregisterReadClient: %s", readClient)
	}
	c.readProcMapLock.Unlock()
	buildSuccessResp(w, "success")
}

func (c *client) broadcastRefreshExtents(readClient string, inode uint64) {
	url := fmt.Sprintf("http://%s%s?%s=%s&%s=%d", readClient, ControlBroadcastRefreshExtents, volKey, c.volName, inoKey, inode)
	reply, err := sendWithRetry(url, MaxRetry)
	if err != nil {
		c.readProcErrMap[readClient]++
		log.LogErrorf("broadcastRefreshExtents: failed, send url(%v) err(%v) reply(%v)", url, err, reply)
	} else {
		c.readProcErrMap[readClient] = 0
	}
}

func (c *client) registerReadProcStatus(alive bool) {
	if len(c.masterClient) == 0 {
		return
	}
	url := fmt.Sprintf("http://%s%s?%s=:%d&%s=%t", c.masterClient, ControlReadProcessRegister, clientKey, gClientManager.profPort, aliveKey, alive)
	if reply, err := sendWithRetry(url, MaxRetry); err != nil {
		msg := fmt.Sprintf("send url(%v) err(%v) reply(%v)", url, err, reply)
		handleError(c, "registerReadProcStatus", msg)
	}
}

func getReadProcsHandleFunc(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	var encoded []byte
	c.readProcMapLock.Lock()
	encoded, _ = json.Marshal(&c.readProcErrMap)
	c.readProcMapLock.Unlock()
	w.Write(encoded)
	return
}

func sendWithRetry(url string, maxRetry int) (reply *proto.HTTPReply, err error) {
	for i := 0; i < maxRetry; i++ {
		if reply, err = sendURL(url); err != nil {
			log.LogWarnf("sendURL failed, url(%v) err(%v) reply(%v). Retry......", url, err, reply)
			continue
		}
		break
	}
	return
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
