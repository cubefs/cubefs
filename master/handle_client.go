package master

import (
	"encoding/json"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/util/log"
	"net/http"
	"regexp"
	"strconv"
)

type VolStatInfo struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

type DataPartitionResponse struct {
	PartitionID   uint64
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
}

type DataPartitionsView struct {
	DataPartitions []*DataPartitionResponse
}

func NewDataPartitionsView() (dataPartitionsView *DataPartitionsView) {
	dataPartitionsView = new(DataPartitionsView)
	dataPartitionsView.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
}

type VolView struct {
	Name           string
	VolType        string
	MetaPartitions []*MetaPartitionView
	DataPartitions []*DataPartitionResponse
}

func NewVolView(name, volType string) (view *VolView) {
	view = new(VolView)
	view.Name = name
	view.VolType = volType
	view.MetaPartitions = make([]*MetaPartitionView, 0)
	view.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

func NewMetaPartitionView(partitionID, start, end uint64, status int8) (mpView *MetaPartitionView) {
	mpView = new(MetaPartitionView)
	mpView.PartitionID = partitionID
	mpView.Start = start
	mpView.End = end
	mpView.Status = status
	mpView.Members = make([]string, 0)
	return
}

func (m *Master) getDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code int
		name string
		vol  *Vol
		ok   bool
		err  error
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		goto errDeal
	}

	if body, err = vol.getDataPartitionsView(m.cluster.getLiveDataNodesRate()); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getDataPartitions", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) getVol(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code int
		err  error
		name string
		vol  *Vol
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		goto errDeal
	}
	if body, err = json.Marshal(m.getVolView(vol)); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getVol", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) getVolStatInfo(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code int
		err  error
		name string
		vol  *Vol
		ok   bool
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		goto errDeal
	}
	if body, err = json.Marshal(volStat(vol)); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getVolStatInfo", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) getVolView(vol *Vol) (view *VolView) {
	view = NewVolView(vol.Name, vol.VolType)
	setMetaPartitions(vol, view, m.cluster.getLiveMetaNodesRate())
	setDataPartitions(vol, view, m.cluster.getLiveDataNodesRate())
	return
}
func setDataPartitions(vol *Vol, view *VolView, liveRate float32) {
	if liveRate < NodesAliveRate || vol.dataPartitions.readWriteDataPartitions < MinReadWriteDataPartitionsForClient {
		return
	}
	vol.dataPartitions.RLock()
	defer vol.dataPartitions.RUnlock()
	view.DataPartitions = vol.dataPartitions.GetDataPartitionsView(0)
}
func setMetaPartitions(vol *Vol, view *VolView, liveRate float32) {
	if liveRate < NodesAliveRate {
		return
	}
	vol.mpsLock.RLock()
	defer vol.mpsLock.RUnlock()
	for _, mp := range vol.MetaPartitions {
		view.MetaPartitions = append(view.MetaPartitions, getMetaPartitionView(mp))
	}
}

func volStat(vol *Vol) (stat *VolStatInfo) {
	stat = new(VolStatInfo)
	stat.Name = vol.Name
	for _, dp := range vol.dataPartitions.dataPartitions {
		stat.TotalSize = stat.TotalSize + dp.total
		usedSize := dp.getMaxUsedSize()
		stat.UsedSize = stat.UsedSize + usedSize
	}
	if stat.UsedSize > stat.TotalSize {
		stat.UsedSize = stat.TotalSize
	}
	log.LogDebugf("total[%v],usedSize[%v]", stat.TotalSize, stat.UsedSize)
	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *MetaPartitionView) {
	mpView = NewMetaPartitionView(mp.PartitionID, mp.Start, mp.End, mp.Status)
	mp.Lock()
	defer mp.Unlock()
	for _, metaReplica := range mp.Replicas {
		mpView.Members = append(mpView.Members, metaReplica.Addr)
		if metaReplica.IsLeader {
			mpView.LeaderAddr = metaReplica.Addr
		}
	}
	return
}

func (m *Master) getMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		body        []byte
		code        int
		err         error
		name        string
		partitionID uint64
		vol         *Vol
		mp          *MetaPartition
		ok          bool
	)
	if name, partitionID, err = parseGetMetaPartitionPara(r); err != nil {
		goto errDeal
	}
	if vol, ok = m.cluster.vols[name]; !ok {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		goto errDeal
	}
	if mp, ok = vol.MetaPartitions[partitionID]; !ok {
		err = errors.Annotatef(MetaPartitionNotFound, "%v not found", partitionID)
		goto errDeal
	}
	if body, err = mp.toJson(); err != nil {
		code = http.StatusMethodNotAllowed
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getMetaPartition", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func parseGetMetaPartitionPara(r *http.Request) (name string, partitionID uint64, err error) {
	r.ParseForm()
	if name, err = checkVolPara(r); err != nil {
		return
	}
	if partitionID, err = checkMetaPartitionID(r); err != nil {
		return
	}
	return
}

func checkMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(ParaId); value == "" {
		err = paraNotFound(ParaId)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseGetVolPara(r *http.Request) (name string, err error) {
	r.ParseForm()
	return checkVolPara(r)
}

func checkVolPara(r *http.Request) (name string, err error) {
	if name = r.FormValue(ParaName); name == "" {
		err = paraNotFound(name)
		return
	}

	pattern := "^[a-zA-Z0-9]{3,256}$"
	reg, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}

	if !reg.MatchString(name) {
		return "", errors.New("name can only be number and letters")
	}

	return
}
