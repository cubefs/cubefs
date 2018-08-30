package wrapper

import (
	"encoding/json"
	"fmt"
	"github.com/chubaoio/cbfs/proto"
	"github.com/chubaoio/cbfs/util/log"
	"github.com/juju/errors"
	"math"
	"net"
	"strings"
)

type DataPartition struct {
	PartitionID   uint32
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
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

func isExcluded(partitionId uint32, excludes []uint32) bool {
	for _, id := range excludes {
		if id == partitionId {
			return true
		}
	}
	return false
}

func (dp *DataPartition) updateMetrics() (err error) {
	leaderMetrics, err := dp.sendGetDataPartitionMetricsPacket(dp.Hosts[0])
	if err != nil {
		log.LogWarnf(err.Error())
		return
	}
	if dp.Status == proto.ReadOnly || dp.Status == proto.Unavaliable {
		dp.Metrics.WriteLatency = math.MaxUint64
		dp.Metrics.ReadLatency = leaderMetrics.ReadLatency
	} else {
		if leaderMetrics.WriteLatency!=0 {
			dp.Metrics.WriteLatency = leaderMetrics.WriteLatency
		}
		if leaderMetrics.ReadLatency!=0 {
			dp.Metrics.ReadLatency = leaderMetrics.ReadLatency
		}
	}
	dp.Metrics.ReadCnt=leaderMetrics.ReadCnt
	dp.Metrics.WriteCnt=leaderMetrics.WriteCnt
	dp.Metrics.SumReadLatency=leaderMetrics.SumReadLatency
	dp.Metrics.SumWriteLatency=leaderMetrics.SumWriteLatency

	for _, h := range dp.Hosts[1:] {
		metrics, err := dp.sendGetDataPartitionMetricsPacket(h)
		if err != nil {
			log.LogWarnf(err.Error())
			continue
		}
		if dp.Status == proto.Unavaliable {
			dp.Metrics.ReadLatency = math.MaxUint64
		} else {
			if metrics.ReadLatency!=0 {
				dp.Metrics.ReadLatency += metrics.ReadLatency
			}
			dp.Metrics.ReadCnt+=metrics.ReadCnt
			dp.Metrics.SumReadLatency+=leaderMetrics.SumReadLatency
		}
	}

	return
}

func (dp *DataPartition) sendGetDataPartitionMetricsPacket(host string) (metrics *DataPartitionMetrics, err error) {
	var conn *net.TCPConn
	conn, err = GconnPool.Get(host)
	if err != nil {
		return nil, errors.Annotatef(err, "datapartition(%v) updateMetrics cannot get connection "+
			"from Host(%v) failed", dp.PartitionID, host)
	}
	defer func() {
		if err != nil {
			GconnPool.Put(conn, true)
		} else {
			GconnPool.Put(conn, false)
		}
	}()
	p := NewGetDataPartitionMetricsPacket(dp.PartitionID)
	if err = p.WriteToConn(conn); err != nil {
		return nil, errors.Annotatef(err, "datapartition(%v) updateMetrics write to Host(%v) failed",
			dp.PartitionID, host)
	}
	if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return nil, errors.Annotatef(err, "datapartition(%v) updateMetrics read body from Host(%v) failed",
			dp.PartitionID, host)

	}
	metrics = NewDataPartitionMetrics()
	if err = json.Unmarshal(p.Data, metrics); err != nil {
		return nil, errors.Annotatef(err, "datapartition(%v) updateMetrics unmarshal body from Host(%v) failed",
			dp.PartitionID, host)
	}

	return
}

func NewGetDataPartitionMetricsPacket(partitionid uint32) (p *proto.Packet) {
	p = new(proto.Packet)
	p.PartitionID = partitionid
	p.Magic = proto.ProtoMagic
	p.StoreMode = proto.ExtentStoreMode
	p.ReqID = proto.GetReqID()
	p.Opcode = proto.OpGetDataPartitionMetrics

	return
}
