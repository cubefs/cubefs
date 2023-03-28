package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"gopkg.in/ini.v1"
)

const (
	ControlBroadcastRefreshExtents = "/broadcast/refreshExtents"
	ControlReadProcessRegister     = "/readProcess/register"
	ControlGetReadProcs            = "/get/readProcs"
	ControlSetReadWrite            = "/set/readwrite"
	ControlSetReadOnly             = "/set/readonly"
)

func setReadWrite(w http.ResponseWriter, r *http.Request) {
	var err error
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}

	err = c.mc.ClientAPI().ApplyVolMutex(c.app, c.volName, c.localAddr)
	if err == proto.ErrVolWriteMutexUnable {
		c.readOnly = false
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume %s not support WriteMutex", c.volName))
		return
	}
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("set readwrite failed: %v", err.Error()))
		return
	}
	c.readOnly = false
	log.LogInfof("Set ReadWrite %s successfully.", c.volName)
	buildSuccessResp(w, "success")
}

func setReadOnly(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	err := c.mc.ClientAPI().ReleaseVolMutex(c.app, c.volName, c.localAddr)
	if err == proto.ErrVolWriteMutexUnable {
		c.readOnly = false
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume %s not support WriteMutex", c.volName))
		return
	}
	if err == proto.ErrVolWriteMutexOccupied {
		c.readOnly = true
		c.readProcMapLock.Lock()
		c.readProcs = make(map[string]string)
		c.readProcMapLock.Unlock()
		log.LogWarnf("Set ReadOnly:  volume %s WriteMutex occupied by others.", c.volName)
		buildSuccessResp(w, "success")
		return
	}
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("set readonly failed: %v", err.Error()))
		return
	}
	c.readOnly = true
	c.readProcMapLock.Lock()
	c.readProcs = make(map[string]string)
	c.readProcMapLock.Unlock()
	log.LogInfof("Set ReadOnly %s successfully.", c.volName)
	buildSuccessResp(w, "success")
}

func getReadStatus(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}

	c.readProcMapLock.Lock()
	status := struct {
		Readstatus string
		Slaves     map[string]string
	}{Slaves: c.readProcs}
	c.readProcMapLock.Unlock()

	if c.readOnly {
		status.Readstatus = "read only"
	} else {
		status.Readstatus = "read write"
	}
	buildSuccessResp(w, status)
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

func SetClientUpgrade(w http.ResponseWriter, r *http.Request) {
	const defaultClientID = 1
	var err error
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Parse parameter error: %v", err))
		return
	}

	vol := r.FormValue(volKey)
	if vol != "" && vol != c.volName {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume %s is not expected to update", c.volName))
		return
	}

	current := r.FormValue(currentKey)
	if current != "" && current != CommitID {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Current version %s is not expected to update", CommitID))
		return
	}

	version := r.FormValue(versionKey)
	if version == "" {
		buildFailureResp(w, http.StatusBadRequest, "Invalid version parameter.")
		return
	}

	if version == CommitID {
		buildSuccessResp(w, fmt.Sprintf("Current version %s is same to expected.", CommitID))
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

	if err = checkBypassConfigFile(c.configPath, c.volName, c.mw.Cluster()); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("Invalid configFile %s: %v", c.configPath, err))
		return
	}

	NextVersion = version
	defer func() {
		if err != nil {
			NextVersion = ""
		}
	}()

	if version == "test" {
		os.Setenv("RELOAD_CLIENT", version)
		buildSuccessResp(w, "Set successful. Upgrading.")
		return
	}
	if version == "reload" {
		os.Setenv("RELOAD_CLIENT", "1")
		buildSuccessResp(w, "Set successful. Upgrading.")
		return
	}

	tmpPath := TmpLibsPath + fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	err = os.MkdirAll(tmpPath, 0777)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	defer os.RemoveAll(tmpPath)

	fileNames, err := downloadAndCheck(c.mc, tmpPath, version)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	targetPath := filepath.Dir(c.configPath)
	for _, fileName := range fileNames {
		if fileName == CheckFile {
			continue
		}

		src := filepath.Join(tmpPath, fileName)
		dst := filepath.Join(targetPath, fileName+".tmp")
		if err = moveFile(src, dst); err != nil {
			buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	for _, fileName := range fileNames {
		if fileName == CheckFile {
			continue
		}
		src := filepath.Join(targetPath, fileName+".tmp")
		dst := filepath.Join(targetPath, fileName)
		if err = os.Rename(src, dst); err != nil {
			buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	os.Setenv("RELOAD_CLIENT", "1")
	buildSuccessResp(w, "Set successful. Upgrading.")
	return
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
		if _, ok := c.readProcs[readClient]; !ok {
			c.readProcs[readClient] = time.Now().Format("2006-01-02 15:04:05")
		}
		log.LogInfof("registerReadClient: %s", readClient)
	} else {
		delete(c.readProcs, readClient)
		log.LogInfof("unregisterReadClient: %s", readClient)
	}
	c.readProcMapLock.Unlock()
	buildSuccessResp(w, "success")
}

func (c *client) broadcastRefreshExtents(readClient string, inode uint64) {
	url := fmt.Sprintf("http://%s%s?%s=%s&%s=%d", readClient, ControlBroadcastRefreshExtents, volKey, c.volName, inoKey, inode)
	reply, err := sendWithRetry(url, MaxRetry)
	if err != nil {
		msg := fmt.Sprintf("send url(%v) err(%v) reply(%v)", url, err, reply)
		log.LogErrorf("broadcastRefreshExtents: %s", msg)
	}
}

func (c *client) registerReadProcStatus(alive bool) {
	if len(c.masterClient) == 0 || c.masterClient == c.localAddr {
		return
	}

	url := fmt.Sprintf("http://%s%s?%s=%s&%s=%t", c.masterClient, ControlReadProcessRegister, clientKey, c.localAddr, aliveKey, alive)
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
	encoded, _ = json.Marshal(&c.readProcs)
	c.readProcMapLock.Unlock()
	w.Write(encoded)
	return
}

func checkBypassConfigFile(configFile, volName, clusterName string) (err error) {
	var (
		actualVolName string
		masterAddr    string
		info          *proto.ClusterInfo
	)
	cfg, err := ini.Load(configFile)
	if err != nil {
		return err
	}

	actualVolName = cfg.Section("").Key("volName").String()
	masterAddr = cfg.Section("").Key("masterAddr").String()
	if actualVolName != volName {
		err = fmt.Errorf("actual volName: %s, expect: %s", actualVolName, volName)
		return
	}

	masters := strings.Split(masterAddr, ",")
	mc := master.NewMasterClient(masters, false)
	if info, err = mc.AdminAPI().GetClusterInfo(); err != nil {
		err = fmt.Errorf("get cluster info fail: err(%v). Please check masterAddr %s and retry.", err, masterAddr)
		return err
	}
	if info.Cluster != clusterName {
		err = fmt.Errorf("actual clusterName: %s, expect: %s", info.Cluster, clusterName)
		return
	}
	return nil
}
