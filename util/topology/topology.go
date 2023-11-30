package topology

import (
	"github.com/cubefs/cubefs/proto"
	"strings"
	"sync"
)

type DataPartition struct {
	PartitionID     uint64
	Hosts           []string
	EcHosts         []string
	EcMigrateStatus uint8
}

func (dp *DataPartition) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

func (dp *DataPartition) GetAllEcAddrs() (m string) {
	return strings.Join(dp.EcHosts[1:], proto.AddrSplit) + proto.AddrSplit
}

type DataPartitionsView map[uint64]*DataPartition

type VolumeTopologyInfo struct {
	name               string
	dataPartitionsView sync.Map
	config             *VolumeConfig
}

func NewVolumeTopologyInfo(name string) *VolumeTopologyInfo {
	return &VolumeTopologyInfo{
		name: name,
	}
}

func (volumeTopo *VolumeTopologyInfo) DataPartitionsView() (dataPartitionsView DataPartitionsView) {
	dataPartitionsView = make(map[uint64]*DataPartition)
	volumeTopo.dataPartitionsView.Range(func(key, value interface{}) bool {
		dataPartition := value.(*DataPartition)
		dataPartitionsView[dataPartition.PartitionID] = dataPartition
		return true
	})
	return
}

func (volumeTopo *VolumeTopologyInfo) Config() *VolumeConfig {
	return volumeTopo.config
}

func (volumeTopo *VolumeTopologyInfo) updateDataPartitionsView(partitions []*DataPartition) {
	for _, partition := range partitions {
		volumeTopo.dataPartitionsView.Store(partition.PartitionID, partition)
	}
}

func (volumeTopo *VolumeTopologyInfo) updateVolConf(newConfig *VolumeConfig) {
	if volumeTopo.config == nil {
		volumeTopo.config = newConfig
	} else {
		volumeTopo.config.update(newConfig)
	}
}
