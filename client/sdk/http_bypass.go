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

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

func setReadWrite(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		force bool
	)
	const defaultClientID = 1
	c, exist := getClient(defaultClientID)
	if !exist {
		buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("client [%v] not exist", defaultClientID))
		return
	}
	if err = r.ParseForm(); err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse parameter error: %v", err))
		return
	}
	if forceStr := r.FormValue(forceKey); forceStr != "" {
		if force, err = strconv.ParseBool(forceStr); err != nil {
			buildFailureResp(w, http.StatusBadRequest, "invalid parameter force")
			return
		}
	}
	err = c.mc.ClientAPI().ApplyVolMutex(c.volName, force)
	if err == proto.ErrVolWriteMutexUnable {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume %s not support WriteMutex", c.volName))
		return
	}
	if err == proto.ErrVolWriteMutexOccupied {
		clientIP, err := c.mc.ClientAPI().GetVolMutex(c.volName)
		if err != nil {
			buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume WriteMutex occupied, fail to get holder: %v", err.Error()))
			return
		}
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume WriteMutex occupied by %s", clientIP))
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
	err := c.mc.ClientAPI().ReleaseVolMutex(c.volName)
	if err == proto.ErrVolWriteMutexUnable {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume %s not support WriteMutex", c.volName))
		return
	}
	if err == proto.ErrVolWriteMutexOccupied {
		clientIP, err := c.mc.ClientAPI().GetVolMutex(c.volName)
		if err != nil {
			buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume WriteMutex occupied by others, fail to get holder: %v", err.Error()))
			return
		}
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("volume WriteMutex occupied by %s", clientIP))
		return
	}
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("set readonly failed: %v", err.Error()))
		return
	}
	c.readOnly = true
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

	if c.readOnly {
		buildSuccessResp(w, "read only.")
	} else {
		buildSuccessResp(w, "read write.")
	}
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

	if err = checkConfigFile(c.configPath, bypassConfigType, c.volName, c.mw.Cluster()); err != nil {
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
		log.LogErrorf("broadcastRefreshExtents: failed, send url(%v) err(%v) reply(%v)", url, err, reply)
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
	encoded, _ = json.Marshal(&c.readProcs)
	c.readProcMapLock.Unlock()
	w.Write(encoded)
	return
}
