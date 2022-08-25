package compact

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/mysql"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

func (cw *CompactWorker) compactInfo(w http.ResponseWriter, r *http.Request) {
	view := cw.collectCompactWorkerViewInfo()
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: view})
}

func (cw *CompactWorker) collectCompactWorkerViewInfo() *proto.CompactWorkerViewInfo {
	cw.RLock()
	defer cw.RUnlock()
	view := &proto.CompactWorkerViewInfo{}
	view.Port = cw.port
	view.Clusters = make([]*proto.ClusterCompactView, 0, len(cw.clusterMap))
	for _, clusterInfo := range cw.clusterMap {
		cluster := &proto.ClusterCompactView{
			ClusterName: clusterInfo.name,
			Nodes:       clusterInfo.Nodes(),
			VolumeInfo:  make([]*proto.VolumeCompactView, 0, len(clusterInfo.volumeMap)),
		}
		for _, volInfo := range clusterInfo.volumeMap {
			cluster.VolumeInfo = append(cluster.VolumeInfo, volInfo.GetVolumeCompactView())
		}
		sort.SliceStable(cluster.VolumeInfo, func(i, j int) bool {
			return cluster.VolumeInfo[i].Name < cluster.VolumeInfo[j].Name
		})
		view.Clusters = append(view.Clusters, cluster)
		sort.SliceStable(view.Clusters, func(i, j int) bool {
			return view.Clusters[i].ClusterName < view.Clusters[j].ClusterName
		})
	}
	return view
}

func (cw *CompactWorker) info(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError,
			Msg: err.Error()})
		return
	}
	clusterName := r.FormValue(CLIClusterKey)
	volumeName := r.FormValue(CLIVolNameKey)
	volInfo := cw.volInfo(clusterName, volumeName)
	var data *proto.VolumeCompactView = nil
	if volInfo != nil {
		data = volInfo.GetVolumeCompactView()
	}
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: data})
}

func (cw *CompactWorker) volInfo(clusterName, volumeName string) (volInfo *CompactVolumeInfo) {
	cw.RLock()
	defer cw.RUnlock()

	clusterInfo, ok := cw.clusterMap[clusterName]
	if !ok {
		return
	}
	volInfo, ok = clusterInfo.volumeMap[volumeName]
	return
}

func (cw *CompactWorker) stop(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	clusterName := r.FormValue(CLIClusterKey)
	volumeName := r.FormValue(CLIVolNameKey)
	var msg string
	if vol, ok := cw.getVolumeInfo(clusterName, volumeName); ok {
		vol.State = CmpVolClosingST
		msg = "Success"
	} else {
		msg = fmt.Sprintf("cluster(%v) volume(%v) doesn't exist", clusterName, volumeName)
	}
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: msg})
}

func (cw *CompactWorker) status(w http.ResponseWriter, r *http.Request) {
	reply := []byte("ok")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}
}

func (cw *CompactWorker) setLimit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	limit := r.FormValue(CLILimitSize)
	var limitNum int64
	var err error
	if limitNum, err = strconv.ParseInt(limit,10,32); err != nil {
		msg := fmt.Sprintf("limit(%v) should be numeric", limit)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	if limitNum <= 0 {
		msg := fmt.Sprintf("limit(%v) should be greater than 0", limit)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	cw.concurrencyLimiter.Reset(int32(limitNum))
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success"})
}

func (cw *CompactWorker) getLimit(w http.ResponseWriter, r *http.Request) {
	limit := cw.concurrencyLimiter.Limit()
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: limit})
}

func (cw *CompactWorker) addMp(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	clusterName := r.FormValue(CLIClusterKey)
	volumeName := r.FormValue(CLIVolNameKey)
	mpId := r.FormValue(CLIMpIdKey)
	if volumeName == "" {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "volume name should not be an empty string"})
		return
	}
	var mpIdNum int
	var err error
	if mpIdNum, err = strconv.Atoi(mpId); err != nil {
		msg := fmt.Sprintf("mpId(%v) should be numeric", mpId)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	var msg string
	var mc *master.MasterClient
	if mc, msg, err = cw.prepareClusterVolume(clusterName, volumeName); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	// verify vol force row and compact tag
	var volInfo *proto.SimpleVolView
	if volInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
		msg = "failed to get volume simple info"
		log.LogErrorf("addMp GetVolumeSimpleInfo cluster(%v) volName(%v) err(%v)", clusterName, volumeName, err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	if !volInfo.ForceROW {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Please open force row first."})
		return
	}
	// force row and compact are on, task write to db
	if volInfo.ForceROW && volInfo.CompactTag == proto.CompactOpenName {
		task := proto.NewDataTask(proto.WorkerTypeCompact, clusterName, volumeName, 0, uint64(mpIdNum), proto.CompactOpenName)
		var exist bool
		var oldTask *proto.Task
		if exist, oldTask, err = mysql.CheckMPTaskExist(task.Cluster, task.VolName, int(task.TaskType), task.MpId); err != nil {
			log.LogErrorf("CompactWorker addMp CheckMPTaskExist failed, cluster(%v), volume(%v), err(%v)",
				clusterName, volumeName, err)
			sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Task write to db error, please try again."})
			return
		}
		if exist {
			msg = fmt.Sprintf("The same MP task is running, task:%v", oldTask)
			log.LogDebugf(msg)
			sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "The same MP task is running"})
			return
		}
		if _, err = cw.AddTask(task); err != nil {
			log.LogErrorf("CompactWorker addMp AddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
				clusterName, volumeName, task, err)
			sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Task write to db error, please try again."})
			return
		}
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Task write to db, waiting for execution."})
		return
	}

	var vol *CompactVolumeInfo
	var ok bool
	if vol, ok = cw.getVolumeInfo(clusterName, volumeName); !ok {
		msg = fmt.Sprintf("cluster(%v) volName(%v) does not exist", clusterName, volumeName)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	// reset compact modify time
	authKey := CalcAuthKey(volInfo.Owner)
	if _, err = mc.AdminAPI().SetCompact(volumeName, volInfo.CompactTag, authKey); err != nil {
		log.LogErrorf("addInode SetCompact cluster(%v) volName(%v) err(%v)", clusterName, volumeName, err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	// force row is on, but compact is off, exec compact tasks directly
	go func() {
		task := &proto.Task{
			TaskId:     0,
			TaskType:   proto.WorkerTypeCompact,
			Cluster:    clusterName,
			VolName:    volumeName,
			DpId:       0,
			MpId:       uint64(mpIdNum),
			WorkerAddr: cw.LocalIp,
		}
		cmpTask := NewMpCmpTask(task, mc, vol)
		var isFinished bool
		if isFinished, err = cmpTask.RunOnce(true); err != nil {
			log.LogErrorf("addMp cmpTask.RunOnce cluster(%v) volName(%v) mpId(%v) err(%v)", clusterName, volumeName, mpId, err)
		} else {
			log.LogInfof("addMp cmpTask.RunOnce cluster(%v) volName(%v) mpId(%v) compact finished, result(%v)", clusterName, volumeName, mpId, isFinished)
		}
	}()
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "start exec cmpMpTask compact task"})
}

func (cw *CompactWorker) addInode(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	clusterName := r.FormValue(CLIClusterKey)
	volumeName := r.FormValue(CLIVolNameKey)
	mpId := r.FormValue(CLIMpIdKey)
	inodeId := r.FormValue(CLIInodeIdKey)
	if volumeName == "" {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "volume name should not be an empty string"})
		return
	}
	var mpIdNum int
	var err error
	if mpIdNum, err = strconv.Atoi(mpId); err != nil {
		msg := fmt.Sprintf("mpId(%v) should be numeric", mpId)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	var inodeIdNum int
	if inodeIdNum, err = strconv.Atoi(inodeId); err != nil {
		msg := fmt.Sprintf("inodeId(%v) should be numeric", inodeId)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	var msg string
	var mc *master.MasterClient
	if mc, msg, err = cw.prepareClusterVolume(clusterName, volumeName); err != nil {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}

	// verify vol force row and compact tag
	var volInfo *proto.SimpleVolView
	if volInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(volumeName); err != nil {
		msg = "failed to get volume simple info"
		log.LogErrorf("addInode GetVolumeSimpleInfo cluster(%v) volName(%v) err(%v)", clusterName, volumeName, err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	if !volInfo.ForceROW {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Please open force row first."})
		return
	}

	if volInfo.ForceROW && volInfo.CompactTag != proto.CompactOpenName {
		// reset compact modify time
		authKey := CalcAuthKey(volInfo.Owner)
		if _, err = mc.AdminAPI().SetCompact(volumeName, volInfo.CompactTag, authKey); err != nil {
			log.LogErrorf("addInode SetCompact cluster(%v) volName(%v) err(%v)", clusterName, volumeName, err)
			sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
			return
		}
	}

	var vol *CompactVolumeInfo
	var ok bool
	if vol, ok = cw.getVolumeInfo(clusterName, volumeName); !ok {
		msg = fmt.Sprintf("cluster(%v) volName(%v) does not exist", clusterName, volumeName)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	var cmpInodes []*proto.CmpInodeInfo
	inodeConcurrentPerMP := vol.GetInodeConcurrentPerMP()
	minEkLen, minInodeSize, maxEkAvgSize := vol.GetInodeFilterParams()
	cmpInodes, err = vol.metaClient.GetCmpInode_ll(context.Background(), uint64(mpIdNum), []uint64{uint64(inodeIdNum)}, inodeConcurrentPerMP, minEkLen, minInodeSize, maxEkAvgSize)
	if err != nil {
		msg = fmt.Sprintf("cluster(%v) volName(%v) mpIdNum(%v) err(%v)", clusterName, volumeName, mpIdNum, err)
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	if len(cmpInodes) <= 0 {
		sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "need not compact"})
		return
	}
	go func() {
		task := &proto.Task{
			TaskId:     0,
			TaskType:   proto.WorkerTypeCompact,
			Cluster:    clusterName,
			VolName:    volumeName,
			DpId:       0,
			MpId:       uint64(mpIdNum),
			WorkerAddr: cw.LocalIp,
		}
		cmpMpTask := NewMpCmpTask(task, mc, vol)
		inodeTask := NewCmpInodeTask(cmpMpTask, cmpInodes[0], vol)
		var isFinished bool
		if isFinished, err = inodeTask.RunOnce(true); err != nil {
			log.LogErrorf("addInode inodeTask.RunOnce cluster(%v) volName(%v) mpId(%v) inodeId(%v) err(%v)", clusterName, volumeName, mpId, inodeId, err)
		} else {
			log.LogInfof("addInode inodeTask.RunOnce cluster(%v) volName(%v) mpId(%v) inodeId(%v) compact finished, result(%v)", clusterName, volumeName, mpId, inodeId, isFinished)
		}
	}()
	sendReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "start exec inode compact task"})
}

func (cw *CompactWorker) prepareClusterVolume(clusterName, volumeName string) (mc *master.MasterClient, msg string, err error) {
	if err = cw.addClusterVolume(clusterName, volumeName); err != nil {
		msg = "failed to add cluster volume"
		log.LogErrorf("prepareClusterVolume addClusterVolume failed, err:%v", err)
		return
	}
	var ok bool
	if mc, ok = cw.getMasterClient(clusterName); !ok {
		msg = "failed to get master client"
		err = errors.New(msg)
		return
	}
	return
}

func sendReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))

	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}

	log.LogInfof("URL[%v], remoteAddr[%v], response[%v]", r.URL, r.RemoteAddr, string(reply[:]))
	return
}

func CalcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
