package smart

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http"
)

const (
	SmartVolumeAPIStatus = "/smart/status"
)

func (sv *SmartVolumeWorker) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("SmartVolume")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc(SmartVolumeAPIStatus, sv.getSmartWorkerStatus)
}

func (sv *SmartVolumeWorker) getSmartWorkerStatus(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("smartWorker(%s) is running", sv.WorkerAddr)))
}
