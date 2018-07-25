package master

import (
	"math/rand"
	"sync"
	"time"

	"encoding/json"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
)

const (
	ReservedVolCount = 1
)

type DataNode struct {
	MaxDiskAvailWeight        uint64 `json:"MaxDiskAvailWeight"`
	CreatedVolWeights         uint64
	RemainWeightsForCreateVol uint64
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	Available                 uint64
	RackName                  string `json:"Rack"`
	Addr                      string
	ReportTime                time.Time
	isActive                  bool
	sync.RWMutex
	Ratio              float64
	SelectCount        uint64
	Carry              float64
	Sender             *AdminTaskSender
	dataPartitionInfos []*proto.PartitionReport
	DataPartitionCount uint32
}

func NewDataNode(addr, clusterID string) (dataNode *DataNode) {
	dataNode = new(DataNode)
	dataNode.Carry = rand.Float64()
	dataNode.Total = 1
	dataNode.Addr = addr
	dataNode.Sender = NewAdminTaskSender(dataNode.Addr, clusterID)
	return
}

/*check node heartbeat if reportTime > DataNodeTimeOut,then IsActive is false*/
func (dataNode *DataNode) checkHeartBeat() {
	dataNode.Lock()
	defer dataNode.Unlock()
	if time.Since(dataNode.ReportTime) > time.Second*time.Duration(DefaultNodeTimeOutSec) {
		dataNode.isActive = false
	}

	return
}

/*set node is online*/
func (dataNode *DataNode) setNodeAlive() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.ReportTime = time.Now()
	dataNode.isActive = true
}

func (dataNode *DataNode) UpdateNodeMetric(resp *proto.DataNodeHeartBeatResponse) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.MaxDiskAvailWeight = resp.MaxWeightsForCreatePartition
	dataNode.CreatedVolWeights = resp.CreatedPartitionWeights
	dataNode.RemainWeightsForCreateVol = resp.RemainWeightsForCreatePartition
	dataNode.Total = resp.Total
	dataNode.Used = resp.Used
	dataNode.Available = resp.Available
	dataNode.RackName = resp.RackName
	dataNode.DataPartitionCount = resp.CreatedPartitionCnt
	dataNode.dataPartitionInfos = resp.PartitionInfo
	dataNode.Ratio = (float64)(dataNode.Used) / (float64)(dataNode.Total)
	dataNode.ReportTime = time.Now()
}

func (dataNode *DataNode) IsWriteAble() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	if dataNode.isActive == true && dataNode.MaxDiskAvailWeight > (uint64)(util.DefaultDataPartitionSize) &&
		dataNode.Total-dataNode.Used > (uint64)(util.DefaultDataPartitionSize)*ReservedVolCount {
		ok = true
	}

	return
}

func (dataNode *DataNode) IsAvailCarryNode() (ok bool) {
	dataNode.RLock()
	defer dataNode.RUnlock()

	return dataNode.Carry >= 1
}

func (dataNode *DataNode) SetCarry(carry float64) {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Carry = carry
}

func (dataNode *DataNode) SelectNodeForWrite() {
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Ratio = float64(dataNode.Used) / float64(dataNode.Total)
	dataNode.SelectCount++
	dataNode.Used += (uint64)(util.DefaultDataPartitionSize)
	dataNode.Carry = dataNode.Carry - 1.0
}

func (dataNode *DataNode) clean() {
	time.Sleep(DefaultCheckHeartbeatIntervalSeconds * time.Second)
	dataNode.Lock()
	defer dataNode.Unlock()
	dataNode.Sender.exitCh <- struct{}{}
}

func (dataNode *DataNode) generateHeartbeatTask(masterAddr string) (task *proto.AdminTask) {
	request := &proto.HeartBeatRequest{
		CurrTime:   time.Now().Unix(),
		MasterAddr: masterAddr,
	}
	task = proto.NewAdminTask(proto.OpDataNodeHeartbeat, dataNode.Addr, request)
	return
}

func (dataNode *DataNode) toJson() (body []byte, err error) {
	dataNode.RLock()
	defer dataNode.RUnlock()
	return json.Marshal(dataNode)
}
