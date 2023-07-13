package fsck

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
)

func (fsckWorker *FSCheckWorker) registerHandle() {
	http.HandleFunc(proto.Version, func(w http.ResponseWriter, r *http.Request) {
		version := proto.MakeVersion("fsck")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
		return
	})
	http.HandleFunc("/list/cluster", fsckWorker.listCluster)

}

func (fsckWorker *FSCheckWorker) listCluster(w http.ResponseWriter, r *http.Request) {
	fsckWorker.RLock()
	defer fsckWorker.RUnlock()

	marshal, _ := json.Marshal(fsckWorker.clusterConfigMap)
	if _, err := w.Write(marshal); err != nil {
		log.LogErrorf("write list cluster has err:[%s]", err.Error())
	}
	return
}

