package master

import (
	"encoding/json"
	"github.com/chubaoio/cbfs/proto"
	"math/rand"
	"sync"
	"time"
)

type MetaNode struct {
	ID                 uint64
	Addr               string
	IsActive           bool
	Sender             *AdminTaskSender
	RackName           string `json:"Rack"`
	MaxMemAvailWeight  uint64 `json:"MaxMemAvailWeight"`
	Total              uint64 `json:"TotalWeight"`
	Used               uint64 `json:"UsedWeight"`
	Ratio              float64
	SelectCount        uint64
	Carry              float64
	Threshold          float32
	ReportTime         time.Time
	metaPartitionInfos []*proto.MetaPartitionReport
	MetaPartitionCount int
	sync.RWMutex
}

func NewMetaNode(addr, clusterID string) (node *MetaNode) {
	return &MetaNode{
		Addr:   addr,
		Sender: NewAdminTaskSender(addr, clusterID),
		Carry:  rand.Float64(),
	}
}

func (metaNode *MetaNode) clean() {
	time.Sleep(DefaultCheckHeartbeatIntervalSeconds * time.Second)
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.Sender.exitCh <- struct{}{}
}

func (metaNode *MetaNode) SetCarry(carry float64) {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.Carry = carry
}

func (metaNode *MetaNode) SelectNodeForWrite() {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.SelectCount++
	metaNode.Carry = metaNode.Carry - 1.0
}

func (metaNode *MetaNode) IsWriteAble() (ok bool) {
	metaNode.RLock()
	defer metaNode.RUnlock()
	if metaNode.IsActive && metaNode.MaxMemAvailWeight > DefaultMetaNodeReservedMem &&
		!metaNode.isArriveThreshold() && metaNode.MetaPartitionCount < DefaultMetaPartitionCountOnEachNode {
		ok = true
	}
	return
}

func (metaNode *MetaNode) IsAvailCarryNode() (ok bool) {
	metaNode.RLock()
	defer metaNode.RUnlock()
	return metaNode.Carry >= 1
}

func (metaNode *MetaNode) setNodeAlive() {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.ReportTime = time.Now()
	metaNode.IsActive = true
}

func (metaNode *MetaNode) updateMetric(resp *proto.MetaNodeHeartbeatResponse, threshold float32) {
	metaNode.Lock()
	defer metaNode.Unlock()
	metaNode.metaPartitionInfos = resp.MetaPartitionInfo
	metaNode.MetaPartitionCount = len(metaNode.metaPartitionInfos)
	metaNode.Total = resp.Total
	metaNode.Used = resp.Used
	metaNode.Ratio = float64(resp.Used) / float64(resp.Total)
	metaNode.MaxMemAvailWeight = resp.Total - resp.Used
	metaNode.RackName = resp.RackName
	metaNode.Threshold = threshold
}

func (metaNode *MetaNode) isArriveThreshold() bool {
	if metaNode.Threshold <= 0 {
		metaNode.Threshold = DefaultMetaPartitionThreshold
	}
	return float32(float64(metaNode.Used)/float64(metaNode.Total)) > metaNode.Threshold
}

func (metaNode *MetaNode) generateHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpMetaNodeHeartbeat, metaNode.Addr, request)
	return
}

func (metaNode *MetaNode) checkHeartbeat() {
	metaNode.Lock()
	defer metaNode.Unlock()
	if time.Since(metaNode.ReportTime) > time.Second*time.Duration(DefaultNodeTimeOutSec) {
		metaNode.IsActive = false
	}
}

func (metaNode *MetaNode) toJson() (body []byte, err error) {
	metaNode.RLock()
	defer metaNode.RUnlock()
	return json.Marshal(metaNode)
}
