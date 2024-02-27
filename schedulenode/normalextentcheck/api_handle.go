package normalextentcheck

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/notify"
	"net/http"
	"strings"
)

func (w *NormalExtentCheckWorker) registerHandle() {
	http.HandleFunc(proto.Version, func(w http.ResponseWriter, r *http.Request) {
		version := proto.MakeVersion("neck")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
		return
	})
	http.HandleFunc("/runTask", w.runCheckTask)
}

func (w *NormalExtentCheckWorker) runCheckTask(respWriter http.ResponseWriter, r *http.Request) {
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

	var mailTo = make([]string, 0)
	mailToStr := r.FormValue("mailTo")
	if mailToStr != "" {
		mailTo = strings.Split(mailToStr, ",")
	}

	metaDataSnapDir := r.FormValue("metaDataSnapDir")

	task := &proto.Task{
		TaskType:      proto.WorkerTypeNormalExtentMistakeDelCheck,
		Cluster:       clusterName,
		VolName:       volName,
		WorkerAddr:    w.LocalIp,
	}
	normalExtentCheckTask := NewNormalExtentCheckTask(task, masterClient, true, metaDataSnapDir, w.mailTo, w.alarmErps, w.alarmPhones)
	go func(){
		if err = normalExtentCheckTask.RunOnce(); err != nil {
			log.LogErrorf("runCheckTask task run failed, cluster(%v) taskType(%v) err(%v)", task.Cluster, proto.WorkerTypeToName(task.TaskType), err)
			return
		}
		notifyServer := notify.NewNotify(w.NotifyConfig)
		if len(mailTo) != 0 {
			notifyServer.SetAlarmEmails(mailTo)
		} else {
			notifyServer.SetAlarmEmails(w.mailTo)
		}
		notifyServer.SetAlarmErps(w.alarmErps)
		if len(normalExtentCheckTask.mistakeDelEK) == 0 && len(normalExtentCheckTask.extentConflict) == 0 {
			notifyServer.AlarmToEmailWithHtmlContent("ChubaoFS Extent检查结果", fmt.Sprintf("%s %s 检查结果正常，不存在误删除的Extent",
				normalExtentCheckTask.Cluster, normalExtentCheckTask.VolName))
			return
		}

		if len(normalExtentCheckTask.mistakeDelEK) != 0 {
			notifyServer.AlarmToEmailWithHtmlContent("ChubaoFS 误删除Extent查找结果", normalExtentCheckTask.formatMistakeDelEKSearchEmailContent())
		}

		if len(normalExtentCheckTask.extentConflict) != 0 {
			notifyServer.AlarmToEmailWithHtmlContent("ChubaoFS 冲突Extent查找结果", normalExtentCheckTask.formatExtentConflictEmailContent())
		}
		return
	}()
}