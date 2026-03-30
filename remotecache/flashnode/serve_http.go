// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package flashnode

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/google/uuid"
)

func (f *FlashNode) registerAPIHandler() {
	http.HandleFunc("/stat", f.handleStat)
	http.HandleFunc("/statAll", f.handleStatAll)
	http.HandleFunc("/sampleStat", f.handleSampleStat)
	http.HandleFunc("/evictVol", f.handleEvictVolume)
	http.HandleFunc("/evictAll", f.handleEvictAll)
	http.HandleFunc("/inactiveDisk", f.handleInactiveDisk)
	http.HandleFunc("/resetCacheErrCnt", f.handleResetCacheErrCnt)
	http.HandleFunc("/setWriteDiskQos", f.handleSetWriteDiskQos)
	http.HandleFunc("/setReadDiskQos", f.handleSetReadDiskQos)
	http.HandleFunc("/getDiskQos", f.handleGetDiskQos)
	http.HandleFunc("/scannerControl", f.handleScannerCommand)
	http.HandleFunc("/setWaitForCacheBlock", f.handleSetWaitForCacheBlock)
	http.HandleFunc("/slotStat", f.handleSlotStat)
	http.HandleFunc("/submitTask", f.handleSubmitTask)
	http.HandleFunc("/batchReadPoolStatus", f.handleBatchReadPoolStatus)
	http.HandleFunc("/setWarmupMetaTotalToken", f.handleSetWarmupMetaTotalToken)
	http.HandleFunc("/addWarmupPath", f.handleAddWarmupPath)
	http.HandleFunc("/setDiskCacheCapacity", f.handleSetDiskCacheCapacity)
	http.HandleFunc("/queryCacheVols", f.handleQueryCacheVols)
	http.HandleFunc("/queryDisableTTLVols", f.handleQueryDisableTTLVols)
	http.HandleFunc("/resetLocalFlowChange", f.handleResetLocalFlowChange)
	http.HandleFunc("/setPrepareLoadRoutineNum", f.handleSetPrepareLoadRoutineNum)
}

func (f *FlashNode) handleStat(w http.ResponseWriter, r *http.Request) {
	replyOK(w, r, proto.FlashNodeStat{
		NodeLimit:         uint64(f.readLimiter.Limit()),
		CacheStatus:       f.cacheEngine.Status(),
		WaitForCacheBlock: f.waitForCacheBlock,
	})
}

func (f *FlashNode) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	var (
		bytes []byte
		err   error
	)
	if bytes, err = io.ReadAll(r.Body); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	defer r.Body.Close()
	req := proto.FlashManualTask{}
	if err = json.Unmarshal(bytes, &req); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	log.LogInfof("submit mannual task with http arg:%+v", &req)

	// Validate file size limits
	if req.ManualTaskConfig.MinFileSizeLimit > req.ManualTaskConfig.MaxFileSizeLimit {
		err = fmt.Errorf("MinFileSizeLimit(%d) cannot be greater than MaxFileSizeLimit(%d)",
			req.ManualTaskConfig.MinFileSizeLimit, req.ManualTaskConfig.MaxFileSizeLimit)
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	// Backward Compatibility
	if req.TopoName == "" {
		req.TopoName = proto.DefaultTopoName
	}
	start := time.Now()
	req.StartTime = &start
	req.UpdateTime = &start
	if req.Id == "" {
		req.Id = uuid.New().String()
	}

	// Set default warm up path expiration if not provided
	if req.ManualTaskConfig.WarmUpPathExpire == 0 {
		req.ManualTaskConfig.WarmUpPathExpire = int64(_defaultWarmUpPathExpire.Seconds())
	}

	rootDir := req.GetPathPrefix()
	var tmpDir string
	f.manualScanners.Range(func(k, v interface{}) bool {
		t := v.(*proto.FlashManualTask)
		if t.VolName != req.VolName {
			return true
		}
		tmpDir = t.GetPathPrefix()
		if rootDir != tmpDir &&
			tmpDir != "" && rootDir != "" &&
			tmpDir != "/" && rootDir != "/" &&
			!strings.HasPrefix(rootDir, tmpDir+"/") &&
			!strings.HasPrefix(tmpDir, rootDir+"/") {
			return true
		}
		if !proto.ManualTaskDone(t.Status) {
			err = fmt.Errorf("manual task[%v] is running on an overlapping directory[%v]", t.Id, tmpDir)
			return false
		}
		return true
	})
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	req.ManualTaskStatistics = &proto.ManualTaskStatistics{
		FlashNode: f.localAddr,
	}
	adminTask := &proto.AdminTask{
		Request: &proto.FlashNodeManualTaskRequest{
			Task:       &req,
			FnNodeAddr: f.localAddr,
		},
		Response: &proto.FlashNodeManualTaskResponse{
			FlashNode: f.localAddr,
		},
	}
	err = f.startTaskScan(adminTask)
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	replyOK(w, r, fmt.Sprintf("create flash manual scan task(%v) success", req.Id))
}

func (f *FlashNode) handleStatAll(w http.ResponseWriter, r *http.Request) {
	replyOK(w, r, proto.FlashNodeStat{
		NodeLimit:   uint64(f.readLimiter.Limit()),
		CacheStatus: f.cacheEngine.StatusAll(),
	})
}

func (f *FlashNode) handleSampleStat(w http.ResponseWriter, r *http.Request) {
	cacheStatus := f.cacheEngine.Status()
	for _, status := range cacheStatus {
		status.Keys = make([]string, 0)
	}
	replyOK(w, r, proto.FlashNodeStat{
		NodeLimit:         uint64(f.readLimiter.Limit()),
		CacheStatus:       cacheStatus,
		WaitForCacheBlock: f.waitForCacheBlock,
	})
}

func (f *FlashNode) handleEvictVolume(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	volume := r.FormValue("volume")
	if volume == "" {
		replyErr(w, r, proto.ErrCodeParamError, "volume name can not be empty", nil)
		return
	}
	replyOK(w, r, f.cacheEngine.EvictCacheByVolume(volume))
}

func (f *FlashNode) handleEvictAll(w http.ResponseWriter, r *http.Request) {
	f.cacheEngine.EvictCacheAll()
	replyOK(w, r, nil)
}

func (f *FlashNode) handleInactiveDisk(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	dataPath := r.FormValue("dataPath")
	if dataPath == "" {
		replyErr(w, r, proto.ErrCodeParamError, "dataPath can not be empty", nil)
		return
	}
	f.cacheEngine.DoInactiveDisk(dataPath)
	replyOK(w, r, nil)
}

func (f *FlashNode) handleResetCacheErrCnt(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	dataPath := r.FormValue("dataPath")
	if dataPath == "" {
		replyErr(w, r, proto.ErrCodeParamError, "dataPath can not be empty", nil)
		return
	}
	if err := f.cacheEngine.ResetCacheErrCnt(dataPath); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	replyOK(w, r, nil)
}

func replyOK(w http.ResponseWriter, r *http.Request, data interface{}) {
	replyErr(w, r, proto.ErrCodeSuccess, "OK", data)
}

func replyErr(w http.ResponseWriter, r *http.Request, code int32, msg string, data interface{}) {
	remote := fmt.Sprintf("url(%s) addr(%s)", r.URL.String(), r.RemoteAddr)
	reply := proto.HTTPReply{Code: code, Msg: msg, Data: data}
	replyBytes, err := json.Marshal(reply)
	if err != nil {
		log.LogErrorf("to %s marshal reply[%v] err:[%v]", remote, reply, err)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(replyBytes)))
	if _, err = w.Write(replyBytes); err != nil {
		log.LogErrorf("to %s write reply len[%d] err:[%v]", remote, len(replyBytes), err)
	}
	log.LogInfof("to %s respond", remote)
}

func (f *FlashNode) handleSetWriteDiskQos(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	parser := func(key string) (val int, err error, has bool) {
		valStr := r.FormValue(key)
		if valStr == "" {
			return 0, nil, false
		}
		has = true
		val, err = strconv.Atoi(valStr)
		return
	}

	updated := false
	for key, pVal := range map[string]*int{
		paramFlow:   &f.diskWriteFlow,
		paramIocc:   &f.diskWriteIocc,
		paramFactor: &f.diskWriteIoFactorFlow,
	} {
		val, err, has := parser(key)
		if err != nil {
			replyErr(w, r, http.StatusBadRequest, err.Error(), nil)
			return
		}
		if has {
			if paramFlow == key {
				f.localChangeWriteFlow = true
			}
			updated = true
			*pVal = val
		}
	}
	if f.diskWriteIoFactorFlow == 0 {
		f.diskWriteIoFactorFlow = _defaultDiskWriteFactor
	}
	if updated {
		f.limitWrite.ResetIOEx(f.diskWriteIocc*len(f.disks), f.diskWriteIoFactorFlow, f.handleReadTimeout)
		f.limitWrite.ResetFlow(f.diskWriteFlow)
		replyOK(w, r, nil)
	} else {
		replyErr(w, r, http.StatusBadRequest, "request param is not an update key", nil)
	}
}

func (f *FlashNode) handleSetReadDiskQos(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	parser := func(key string) (val int, err error, has bool) {
		valStr := r.FormValue(key)
		if valStr == "" {
			return 0, nil, false
		}
		has = true
		val, err = strconv.Atoi(valStr)
		return
	}

	updated := false
	for key, pVal := range map[string]*int{
		paramFlow:   &f.diskReadFlow,
		paramIocc:   &f.diskReadIocc,
		paramFactor: &f.diskReadIoFactorFlow,
	} {
		val, err, has := parser(key)
		if err != nil {
			replyErr(w, r, http.StatusBadRequest, err.Error(), nil)
			return
		}
		if has {
			if paramFlow == key {
				f.localChangeReadFlow = true
			}
			updated = true
			*pVal = val
		}
	}
	if f.diskReadIoFactorFlow == 0 {
		f.diskReadIoFactorFlow = _defaultDiskReadFactor
	}
	if updated {
		f.limitRead.ResetIOEx(f.diskReadIocc*len(f.disks), f.diskReadIoFactorFlow, f.handleReadTimeout)
		f.limitRead.ResetFlow(f.diskReadFlow)
		replyOK(w, r, nil)
	} else {
		replyErr(w, r, http.StatusBadRequest, "request param is not an update key", nil)
	}
}

func (f *FlashNode) handleGetDiskQos(w http.ResponseWriter, r *http.Request) {
	writeStatus := proto.FlashNodeLimiterStatus{Status: f.limitWrite.Status(true), DiskNum: len(f.disks), ReadTimeout: f.handleReadTimeout}
	readStatus := proto.FlashNodeLimiterStatus{Status: f.limitRead.Status(true), DiskNum: len(f.disks), ReadTimeout: f.handleReadTimeout}
	info := proto.FlashNodeLimiterStatusInfo{WriteStatus: writeStatus, ReadStatus: readStatus}
	replyOK(w, r, info)
}

func (f *FlashNode) handleScannerCommand(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		msg := fmt.Sprintf("httpServiceScanner ParseForm failed: %v", err)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}
	id := r.FormValue("id")
	if id == "" {
		http.Error(w, "invalid task id", http.StatusBadRequest)
		return
	}
	log.LogInfof("receive httpServiceScanner id: %v", id)
	opCode := r.FormValue("opCode")
	if opCode == "" {
		http.Error(w, "invalid task opCode", http.StatusBadRequest)
		return
	}
	log.LogInfof("receive httpServiceScanner opCode: %v", opCode)
	mScanner, ok := f.manualScanners.Load(id)
	if !ok {
		msg := fmt.Sprintf("task id(%v) not exist", id)
		http.Error(w, msg, http.StatusNotFound)
		return
	}
	scanner := mScanner.(*ManualScanner)
	if opCode == "info" {
		resp := scanner.copyResponse()
		if resp.Done {
			resp.LoadProgress = "100%"
		} else if scanner.manualTask.ManualTaskConfig.PrintProgress && atomic.LoadInt32(&scanner.loadedEntries) == 1 {
			if resp.TotalEntryNum <= 0 {
				resp.LoadProgress = "0%"
			} else {
				doneEntries := resp.TotalFileScannedNum + resp.TotalDirScannedNum
				percent := (doneEntries * 100) / resp.TotalEntryNum
				if percent > 99 {
					percent = 99
				}
				if percent < 0 {
					percent = 0
				}
				resp.LoadProgress = fmt.Sprintf("%d%%", percent)
			}
		}
		info := map[string]interface{}{
			"task":             resp,
			"dirChanLen":       scanner.dirChan.Len(),
			"fileChanLen":      len(scanner.fileChan),
			"fileRPoolRunning": scanner.fileRPool.RunningNum(),
			"dirRPoolRunning":  scanner.dirRPool.RunningNum(),
			"pause":            scanner.pause,
			"createTime":       scanner.createTime,
		}
		if scanner.RemoteCache != nil {
			info["prepareChLen"] = len(scanner.RemoteCache.PrepareCh)
		} else {
			info["prepareChLen"] = 0
		}
		replyOK(w, r, info)
		return
	}
	scanner.processCommand(opCode)
	w.WriteHeader(http.StatusOK)
}

func (f *FlashNode) handleSetWaitForCacheBlock(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	valStr := r.FormValue("waitForCacheBlock")

	if valStr == "" {
		replyErr(w, r, proto.ErrCodeParamError, "invalid parameter", nil)
		return
	}
	val, err := strconv.ParseBool(valStr)
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, "parse  waitForCacheBlock failed", nil)
		return
	}
	f.waitForCacheBlock = val
	replyOK(w, r, nil)
}

func (f *FlashNode) handleSlotStat(w http.ResponseWriter, r *http.Request) {
	replyOK(w, r, proto.FlashNodeSlotStat{
		NodeId:   f.nodeID,
		Addr:     f.localAddr,
		SlotStat: f.GetFlashNodeSlotStat(),
	})
}

func (f *FlashNode) handleBatchReadPoolStatus(w http.ResponseWriter, r *http.Request) {
	status := f.GetBatchReadPoolStatus()
	if status == nil {
		replyErr(w, r, proto.ErrCodeParamError, "batchReadPool is not initialized", nil)
		return
	}

	response := map[string]interface{}{
		"nodeId":      f.nodeID,
		"addr":        f.localAddr,
		"concurrency": status.Concurrency,
		"queueSize":   status.QueueSize,
		"running":     status.Running,
		"waiting":     status.Waiting,
	}

	replyOK(w, r, response)
}

func (f *FlashNode) handleSetWarmupMetaTotalToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}

	tokenStr := r.FormValue("token")
	if tokenStr == "" {
		replyErr(w, r, proto.ErrCodeParamError, "token parameter cannot be empty", nil)
		return
	}

	token, err := strconv.Atoi(tokenStr)
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, "invalid token value, must be a positive integer", nil)
		return
	}

	if token <= 0 {
		replyErr(w, r, proto.ErrCodeParamError, "token value must be greater than 0", nil)
		return
	}

	f.currentWarmUpWorkerMutex.Lock()
	oldToken := f.warmupMetaTotalToken
	f.warmupMetaTotalToken = token
	f.currentWarmUpWorkerMutex.Unlock()

	log.LogInfof("handleSetWarmupMetaTotalToken: warmupMetaTotalToken changed from %d to %d", oldToken, token)

	replyOK(w, r, map[string]interface{}{
		"oldToken": oldToken,
		"newToken": token,
		"message":  fmt.Sprintf("warmupMetaTotalToken updated from %d to %d", oldToken, token),
	})
}

// handleAddWarmupPath adds a warmup path into f.warmUpPaths via HTTP.
// Request body (JSON): {"volName":"<volume>", "dirPath":"<path>", "expireSeconds":<int64, optional>}
func (f *FlashNode) handleAddWarmupPath(w http.ResponseWriter, r *http.Request) {
	var (
		bytes []byte
		err   error
	)
	if bytes, err = io.ReadAll(r.Body); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	defer r.Body.Close()

	var req struct {
		VolName       string `json:"volName"`
		DirPath       string `json:"dirPath"`
		ExpireSeconds int64  `json:"expireSeconds"`
	}
	if err = json.Unmarshal(bytes, &req); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	if req.VolName == "" {
		replyErr(w, r, proto.ErrCodeParamError, "volName cannot be empty", nil)
		return
	}

	// Normalize dirPath similar to ManualTask.GetPathPrefix behavior.
	dirPath := strings.TrimPrefix(strings.TrimRight(req.DirPath, "/"), "/")
	if dirPath == "" {
		dirPath = "/"
	}

	// Check duplicates (clean up expired entries along the way)
	dup := false
	f.warmUpPaths.Range(func(key, value interface{}) bool {
		info := value.(*proto.WarmUpPathInfo)
		if time.Now().UnixNano() > info.Expiration {
			f.warmUpPaths.Delete(key)
			return true
		}
		if info.VolName == req.VolName && info.DirPath == dirPath {
			dup = true
			return false
		}
		return true
	})
	if dup {
		replyErr(w, r, proto.ErrCodeParamError, "warmup path already exists", map[string]string{"volName": req.VolName, "dirPath": dirPath})
		return
	}

	// Build WarmUpPathInfo and set expiration
	wup := &proto.WarmUpPathInfo{VolName: req.VolName, DirPath: dirPath}
	if req.ExpireSeconds > 0 {
		wup.SetExpiration(time.Duration(req.ExpireSeconds) * time.Second)
	} else {
		wup.SetExpiration(_defaultWarmUpPathExpire)
	}
	key := uuid.New().String()
	f.enableWarmUpPaths = true
	f.warmUpPaths.Store(key, wup)

	replyOK(w, r, map[string]interface{}{
		"id":         key,
		"volName":    wup.VolName,
		"dirPath":    wup.DirPath,
		"expiration": wup.Expiration,
	})
}

func (f *FlashNode) handleSetDiskCacheCapacity(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	dataPath := r.FormValue("dataPath")
	capacityStr := r.FormValue("capacity")
	if capacityStr == "" {
		replyErr(w, r, proto.ErrCodeParamError, "capacity can not be empty", nil)
		return
	}
	capacity, err := strconv.Atoi(capacityStr)
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, "capacity must be integer", nil)
		return
	}
	if capacity <= 0 {
		replyErr(w, r, proto.ErrCodeParamError, "capacity must be positive", nil)
		return
	}
	if err := f.cacheEngine.SetDiskCacheCapacity(dataPath, capacity); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	replyOK(w, r, nil)
}

func (f *FlashNode) handleQueryCacheVols(w http.ResponseWriter, r *http.Request) {
	volCacheSizeMap := f.cacheEngine.GetVolCacheSizeMap()
	replyOK(w, r, volCacheSizeMap)
}

func (f *FlashNode) handleQueryDisableTTLVols(w http.ResponseWriter, r *http.Request) {
	disableTTLMap := f.cacheEngine.GetRemoteCacheDisableTTLMap()
	volumes := make([]string, 0, len(disableTTLMap))
	for volume, disableTTL := range disableTTLMap {
		if disableTTL {
			volumes = append(volumes, volume)
		}
	}
	sort.Strings(volumes)
	replyOK(w, r, map[string]interface{}{
		"count":   len(volumes),
		"volumes": volumes,
		"map":     disableTTLMap,
	})
}

func (f *FlashNode) handleSetPrepareLoadRoutineNum(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}
	s := r.FormValue("prepareLoadRoutineNum")
	if s == "" {
		replyErr(w, r, proto.ErrCodeParamError, "prepareLoadRoutineNum cannot be empty", nil)
		return
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		replyErr(w, r, proto.ErrCodeParamError, "prepareLoadRoutineNum must be an integer", nil)
		return
	}
	if n < 0 {
		replyErr(w, r, proto.ErrCodeParamError, "prepareLoadRoutineNum must be non-negative", nil)
		return
	}
	f.prepareLoadRoutineMu.Lock()
	if f.cacheEngine == nil {
		f.prepareLoadRoutineMu.Unlock()
		replyErr(w, r, proto.ErrCodeParamError, "cacheEngine is not initialized", nil)
		return
	}
	old := f.prepareLoadRoutineNum
	f.prepareLoadRoutineNum = n
	f.prepareLoadRoutineMu.Unlock()
	f.cacheEngine.StartCachePrepareWorkers(f.limitWrite, n)
	log.LogInfof("handleSetPrepareLoadRoutineNum: prepareLoadRoutineNum %d -> %d", old, n)
	replyOK(w, r, map[string]interface{}{
		"oldPrepareLoadRoutineNum": old,
		"newPrepareLoadRoutineNum": n,
	})
}

func (f *FlashNode) handleResetLocalFlowChange(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		replyErr(w, r, proto.ErrCodeParamError, err.Error(), nil)
		return
	}

	ioType := r.FormValue("ioType")
	if ioType == "" {
		replyErr(w, r, proto.ErrCodeParamError, "ioType parameter cannot be empty, must be 'read' or 'write' or 'all'", nil)
		return
	}

	resetRead := false
	resetWrite := false

	switch strings.ToLower(ioType) {
	case "read":
		resetRead = true
	case "write":
		resetWrite = true
	case "all":
		resetRead = true
		resetWrite = true
	default:
		replyErr(w, r, proto.ErrCodeParamError, "invalid ioType, must be 'read', 'write', or 'all'", nil)
		return
	}

	result := make(map[string]interface{})

	if resetRead {
		oldValue := f.localChangeReadFlow
		f.localChangeReadFlow = false
		result["read"] = map[string]interface{}{
			"oldValue": oldValue,
			"newValue": false,
		}
		log.LogInfof("handleResetLocalFlowChange: reset localChangeReadFlow from %v to false", oldValue)
	}

	if resetWrite {
		oldValue := f.localChangeWriteFlow
		f.localChangeWriteFlow = false
		result["write"] = map[string]interface{}{
			"oldValue": oldValue,
			"newValue": false,
		}
		log.LogInfof("handleResetLocalFlowChange: reset localChangeWriteFlow from %v to false", oldValue)
	}

	replyOK(w, r, result)
}
