package metanode

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util/btree"
)

func (m *MetaNode) registerHandler() (err error) {
	// Register http handler
	http.HandleFunc("/getAllPartitions", m.allPartitionsHandle)
	http.HandleFunc("/getInodeInfo", m.inodeInfoHandle)
	http.HandleFunc("/getInodeRange", m.rangeHandle)
	http.HandleFunc("/getExtents", m.getExtents)
	return
}
func (m *MetaNode) allPartitionsHandle(w http.ResponseWriter, r *http.Request) {
	mm := m.metaManager.(*metaManager)
	data, err := json.Marshal(mm.partitions)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	w.Write(data)
}

func (m *MetaNode) inodeInfoHandle(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	id, err := strconv.ParseUint(r.FormValue("id"), 10, 64)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	mp, err := m.metaManager.GetPartition(id)
	if err != nil {
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
		return
	}
	msg := make(map[string]interface{})
	leader, _ := mp.IsLeader()
	msg["leaderAddr"] = leader
	conf := mp.GetBaseConfig()
	msg["peers"] = conf.Peers
	msg["nodeId"] = conf.NodeId
	msg["cursor"] = conf.Cursor
	data, err := json.Marshal(msg)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(data)
}

func (m *MetaNode) rangeHandle(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	id, err := strconv.ParseUint(r.FormValue("id"), 10, 64)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	t := r.FormValue("type")
	mp, err := m.metaManager.GetPartition(id)
	if err != nil {
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
		return
	}
	mpp := mp.(*metaPartition)
	f := func(i btree.Item) bool {
		var data []byte
		if data, err = json.Marshal(i); err != nil {
			return false
		}
		if _, err = w.Write(data); err != nil {
			return false
		}
		data[0] = byte('\n')
		if _, err = w.Write(data[:1]); err != nil {
			return false
		}
		return true
	}
	mpp.RangeInode(f)
	if t == "clone" {
		inoTree := mpp.getInodeTree()
		inoTree.Ascend(f)
	}
}

func (m *MetaNode) getExtents(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	pidVal := r.FormValue("pid")
	idVal := r.FormValue("ino")
	if pidVal == "" || idVal == "" {
		w.WriteHeader(400)
		return
	}
	pid, err := strconv.ParseUint(pidVal, 10, 64)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	id, err := strconv.ParseUint(idVal, 10, 64)
	if err != nil {
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	mp, err := m.metaManager.GetPartition(pid)
	if err != nil {
		w.WriteHeader(404)
		w.Write([]byte(err.Error()))
		return
	}
	mm := mp.(*metaPartition)
	resp := mm.getInode(NewInode(id, 0))
	if resp.Status != proto.OpOk {
		w.WriteHeader(404)
		w.Write([]byte("inode id not exsited"))
		return
	}
	data, err := resp.Msg.Extents.Marshal()
	if err != nil {
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(data)
	return
}
