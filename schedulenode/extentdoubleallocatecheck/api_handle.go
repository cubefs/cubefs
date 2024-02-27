package extentdoubleallocatecheck

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/notify"
	"net/http"
	"path"
	"strconv"
	"time"
)

func (w *ExtentDoubleAllocateCheckWorker) registerHandle() {
	http.HandleFunc(proto.Version, func(w http.ResponseWriter, r *http.Request) {
		version := proto.MakeVersion("blck")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
		return
	})
	http.HandleFunc("/runTask", w.runCheckTask)
	http.HandleFunc("/getParams", w.getParameters)
	http.HandleFunc("/updateParams", w.updateParameters)
}

func (w *ExtentDoubleAllocateCheckWorker) runCheckTask(respWriter http.ResponseWriter, r *http.Request) {
	var err error
	resp := common.NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = respWriter.Write(data); err != nil {
			log.LogErrorf("[runCheckTask] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	//parse cluster, volName
	clusterName := r.FormValue("clusterName")
	masterClient, ok := w.mcw[clusterName]
	if !ok {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("%s not exist in clusters config", clusterName)
		return
	}

	volName := r.FormValue("volName")
	if volName == "" {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("volName is needed")
		return
	}

	needSendEmail, _ := strconv.ParseBool(r.FormValue("sendEmail"))

	go func() {
		task := &proto.Task{
			TaskType:      proto.WorkerTypeExtentDoubleAllocateCheck,
			Cluster:       clusterName,
			VolName:       volName,
			WorkerAddr:    w.LocalIp,
		}
		exportDir := path.Join(w.exportDir, clusterName, fmt.Sprintf("%s_%s", volName, time.Now().Format(proto.TimeFormat2)))
		t := NewExtentDoubleAllocateCheckTask(task, masterClient, exportDir)
		t.RunOnce()
		notifyServer := notify.NewNotify(w.NotifyConfig)
		notifyServer.SetAlarmEmails(w.mailTo)
		notifyServer.SetAlarmErps(w.alarmErps)
		if needSendEmail && len(t.doubleAllocateExtentsMap) != 0 {
			notifyServer.AlarmToEmailWithHtmlContent("ChubaoFS Extent重复分配检查结果通知", t.formatDoubleAllocateExtents())
		}
	}()
}

func (w *ExtentDoubleAllocateCheckWorker) updateParameters(respWriter http.ResponseWriter, r *http.Request) {
	resp := common.NewAPIResponse(http.StatusOK, "success")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := respWriter.Write(data); err != nil {
			log.LogErrorf("[removeCluster] response %s", err)
		}
	}()

	err := r.ParseForm()
	if err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	newParallelMPCnt, _ := strconv.ParseInt(r.FormValue("parallelMPCnt"), 10, 32)
	newParallelInodeCnt, _ := strconv.ParseInt(r.FormValue("parallelInodeCnt"), 10, 32)

	if newParallelMPCnt != 0 && int32(newParallelMPCnt) != parallelMpCnt.Load() {
		parallelMpCnt.Store(int32(newParallelMPCnt))
	}

	if newParallelInodeCnt != 0 && int32(newParallelInodeCnt) != parallelInodeCnt.Load() {
		parallelInodeCnt.Store(int32(newParallelInodeCnt))
	}

	resp.Data = &struct {
		ParallelMPCount    int32 `json:"parallelMPCnt"`
		ParallelInodeCount int32 `json:"parallelInodeCnt"`
	}{
		ParallelMPCount:    parallelMpCnt.Load(),
		ParallelInodeCount: parallelInodeCnt.Load(),
	}
	return
}

func (w *ExtentDoubleAllocateCheckWorker) getParameters(respWriter http.ResponseWriter, r *http.Request) {
	resp := common.NewAPIResponse(http.StatusOK, "success")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := respWriter.Write(data); err != nil {
			log.LogErrorf("[getParameters] response %s", err)
		}
	}()
	resp.Data = &struct {
		ParallelMPCount    int32 `json:"parallelMPCnt"`
		ParallelInodeCount int32 `json:"parallelInodeCnt"`
	}{
		ParallelMPCount:    parallelMpCnt.Load(),
		ParallelInodeCount: parallelInodeCnt.Load(),
	}
	return
}