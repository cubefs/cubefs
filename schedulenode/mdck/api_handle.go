package mdck

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
)

func (w *MetaDataCheckWorker) registerHandle() {
	http.HandleFunc(proto.Version, func(w http.ResponseWriter, r *http.Request) {
		version := proto.MakeVersion("mdck")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
		return
	})
	http.HandleFunc("/runTask", w.runCheckTask)
}

func (w *MetaDataCheckWorker) runCheckTask(respWriter http.ResponseWriter, r *http.Request) {
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

	task := &proto.Task{
		TaskType:      proto.WorkerTypeBlockCheck,
		Cluster:       clusterName,
		VolName:       volName,
		WorkerAddr:    w.LocalIp,
	}
	mdckTask := NewMetaDataCheckTask(task, masterClient, w.mailTo)
	go mdckTask.RunOnce()
}