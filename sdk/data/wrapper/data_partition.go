package wrapper

import (
	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"strings"
)

type DataPartition struct {
	PartitionID   uint64
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
	RandomWrite   bool
	LeaderAddr    string
	Metrics       *DataPartitionMetrics
}

type DataPartitionMetrics struct {
	WriteCnt        uint64
	ReadCnt         uint64
	SumWriteLatency uint64
	SumReadLatency  uint64
	WriteLatency    float64
	ReadLatency     float64
}

type DataPartitionSlice []*DataPartition

func (ds DataPartitionSlice) Len() int {
	return len(ds)
}
func (ds DataPartitionSlice) Swap(i, j int) {
	ds[i], ds[j] = ds[j], ds[i]
}
func (ds DataPartitionSlice) Less(i, j int) bool {
	return ds[i].Metrics.WriteLatency < ds[j].Metrics.WriteLatency
}

func NewDataPartitionMetrics() *DataPartitionMetrics {
	metrics := new(DataPartitionMetrics)
	metrics.WriteCnt = 1
	metrics.ReadCnt = 1
	return metrics
}

func (dp *DataPartition) String() string {
	return fmt.Sprintf("PartitionID(%v) Status(%v) ReplicaNum(%v) PartitionType(%v) Hosts(%v)",
		dp.PartitionID, dp.Status, dp.ReplicaNum, dp.PartitionType, dp.Hosts)
}

func (dp *DataPartition) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

func isExcluded(partitionId uint64, excludes []uint64) bool {
	for _, id := range excludes {
		if id == partitionId {
			return true
		}
	}
	return false
}

func NewGetDataPartitionMetricsPacket(partitionid uint64) (p *proto.Packet) {
	p = new(proto.Packet)
	p.PartitionID = partitionid
	p.Magic = proto.ProtoMagic
	p.StoreMode = proto.NormalExtentMode
	p.ReqID = proto.GeneratorRequestID()
	p.Opcode = proto.OpGetDataPartitionMetrics

	return
}
