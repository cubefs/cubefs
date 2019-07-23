package master

import (
	"fmt"
	"testing"
	"time"
)

func buildPanicCluster() *Cluster {
	c := newCluster(server.cluster.Name, server.cluster.leaderInfo, server.cluster.fsm, server.cluster.partition, server.config)
	v := buildPanicVol()
	c.putVol(v)
	return c
}

func buildPanicVol() *Vol {
	id, err := server.cluster.idAlloc.allocateCommonID()
	if err != nil {
		return nil
	}
	vol := newVol(id, commonVol.Name, commonVol.Owner, commonVol.dataPartitionSize, commonVol.Capacity)
	vol.dataPartitions = nil
	return vol
}

func TestCheckDataPartitions(t *testing.T) {
	server.cluster.checkDataPartitions()
}

func TestPanicCheckDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.checkDataPartitions()
	t.Logf("catched panic")
}

func TestCheckBackendLoadDataPartitions(t *testing.T) {
	server.cluster.scheduleToLoadDataPartitions()
}

func TestPanicBackendLoadDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.scheduleToLoadDataPartitions()
	t.Logf("catched panic")
}

func TestCheckReleaseDataPartitions(t *testing.T) {
	server.cluster.releaseDataPartitionAfterLoad()
}
func TestPanicCheckReleaseDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.releaseDataPartitionAfterLoad()
	t.Logf("catched panic")
}

func TestCheckHeartbeat(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
}

func TestCheckMetaPartitions(t *testing.T) {
	server.cluster.checkMetaPartitions()
}

func TestPanicCheckMetaPartitions(t *testing.T) {
	c := buildPanicCluster()
	vol, err := c.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	partitionID, err := server.cluster.idAlloc.allocateMetaPartitionID()
	if err != nil {
		t.Error(err)
	}
	mp := newMetaPartition(partitionID, 1, defaultMaxMetaPartitionInodeID, vol.mpReplicaNum, vol.Name, vol.ID)
	vol.addMetaPartition(mp)
	mp = nil
	c.checkMetaPartitions()
	t.Logf("catched panic")
}

func TestCheckAvailSpace(t *testing.T) {
	server.cluster.scheduleToUpdateStatInfo()
}

func TestPanicCheckAvailSpace(t *testing.T) {
	c := buildPanicCluster()
	c.dataNodeStatInfo = nil
	c.scheduleToUpdateStatInfo()
}

func TestCheckCreateDataPartitions(t *testing.T) {
	server.cluster.scheduleToCheckAutoDataPartitionCreation()
	//time.Sleep(150 * time.Second)
}

func TestPanicCheckCreateDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.scheduleToCheckAutoDataPartitionCreation()
}

func TestPanicCheckBadDiskRecovery(t *testing.T) {
	c := buildPanicCluster()
	vol, err := c.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	partitionID, err := server.cluster.idAlloc.allocateDataPartitionID()
	if err != nil {
		t.Error(err)
	}
	dp := newDataPartition(partitionID, vol.dpReplicaNum, vol.Name, vol.ID)
	c.BadDataPartitionIds.Store(fmt.Sprintf("%v", dp.PartitionID), dp)
	c.scheduleToCheckDiskRecoveryProgress()
}

func TestCheckBadDiskRecovery(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	//clear
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		server.cluster.BadDataPartitionIds.Delete(key)
		return true
	})
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	vol.RLock()
	dps := make([]*DataPartition, 0)
	for _, dp := range vol.dataPartitions.partitions {
		dps = append(dps, dp)
	}
	dpsMapLen := len(vol.dataPartitions.partitionMap)
	vol.RUnlock()
	dpsLen := len(dps)
	if dpsLen != dpsMapLen {
		t.Errorf("dpsLen[%v],dpsMapLen[%v]", dpsLen, dpsMapLen)
		return
	}
	for _, dp := range dps {
		dp.RLock()
		if len(dp.Replicas) == 0 {
			dpsLen--
			dp.RUnlock()
			return
		}
		addr := dp.Replicas[0].dataNode.Addr
		server.cluster.putBadDataPartitionIDs(dp.Replicas[0], addr, dp.PartitionID)
		dp.RUnlock()
	}
	count := 0
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		count = count + len(badDataPartitionIds)
		return true
	})

	if count != dpsLen {
		t.Errorf("expect bad partition num[%v],real num[%v]", dpsLen, count)
		return
	}
	//check recovery
	server.cluster.checkDiskRecoveryProgress()

	count = 0
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expect bad partition num[0],real num[%v]", count)
		return
	}
}
