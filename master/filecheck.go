package master

import (
	"github.com/tiglabs/baudstorage/proto"
)

/*check File: recover File,if File lack or timeOut report or crc bad*/
func (partition *DataPartition) checkFile(clusterID string) {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicas(DefaultDataPartitionTimeOutSec)
	if len(liveReplicas) == 0 {
		return
	}

	switch partition.PartitionType {
	case proto.ExtentPartition:
		partition.checkExtentFile(liveReplicas, clusterID)
	case proto.TinyPartition:
		partition.checkChunkFile(liveReplicas, clusterID)
	}

	return
}

func (partition *DataPartition) checkChunkFile(liveReplicas []*DataReplica, clusterID string) {
	for _, fc := range partition.FileInCoreMap {
		fc.generateFileCrcTask(partition.PartitionID, liveReplicas, proto.TinyPartition, clusterID)
	}
	return
}

func (partition *DataPartition) checkExtentFile(liveReplicas []*DataReplica, clusterID string) (tasks []*proto.AdminTask) {
	for _, fc := range partition.FileInCoreMap {
		fc.generateFileCrcTask(partition.PartitionID, liveReplicas, proto.ExtentPartition, clusterID)
	}
	return
}
