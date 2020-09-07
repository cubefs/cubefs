package master

import (
	"fmt"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
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
	var createTime = time.Now().Unix() // record create time of this volume
	arg := createVolArg{
		name:         commonVol.Name,
		owner:        commonVol.Owner,
		size:         commonVol.dataPartitionSize,
		capacity:     commonVol.Capacity,
		dpReplicaNum: defaultReplicaNum,
	}
	vol := newVol(id, createTime, &arg)
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
	mp := newMetaPartition(partitionID, 1, defaultMaxMetaPartitionInodeID, vol.mpReplicaNum, vol.Name, vol.ID, vol.mpStoreType)
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

func TestPanicCheckBadMetaPartitionRecovery(t *testing.T) {
	c := buildPanicCluster()
	vol, err := c.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	partitionID, err := server.cluster.idAlloc.allocateMetaPartitionID()
	if err != nil {
		t.Error(err)
	}
	dp := newMetaPartition(partitionID, 0, defaultMaxMetaPartitionInodeID, vol.mpReplicaNum, vol.Name, vol.ID)
	c.BadMetaPartitionIds.Store(fmt.Sprintf("%v", dp.PartitionID), dp)
	c.scheduleToCheckMetaPartitionRecoveryProgress()
}

func TestCheckBadMetaPartitionRecovery(t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	//clear
	server.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		server.cluster.BadMetaPartitionIds.Delete(key)
		return true
	})
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	vol.RLock()
	mps := make([]*MetaPartition, 0)
	for _, mp := range vol.MetaPartitions {
		mps = append(mps, mp)
	}
	mpsMapLen := len(vol.MetaPartitions)
	vol.RUnlock()
	mpsLen := len(mps)
	if mpsLen != mpsMapLen {
		t.Errorf("mpsLen[%v],mpsMapLen[%v]", mpsLen, mpsMapLen)
		return
	}
	for _, mp := range mps {
		mp.RLock()
		if len(mp.Replicas) == 0 {
			mpsLen--
			mp.RUnlock()
			return
		}
		addr := mp.Replicas[0].metaNode.Addr
		server.cluster.putBadMetaPartitions(addr, mp.PartitionID)
		mp.RUnlock()
	}
	count := 0
	server.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		count = count + len(badMetaPartitionIds)
		return true
	})

	if count != mpsLen {
		t.Errorf("expect bad partition num[%v],real num[%v]", mpsLen, count)
		return
	}
	//check recovery
	server.cluster.checkMetaPartitionRecoveryProgress()

	count = 0
	server.cluster.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expect bad partition num[0],real num[%v]", count)
		return
	}
}

func TestUpdateInodeIDUpperBound(t *testing.T) {
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	maxPartitionID := vol.maxPartitionID()
	vol.RLock()
	mp := vol.MetaPartitions[maxPartitionID]
	mpLen := len(vol.MetaPartitions)
	vol.RUnlock()
	mr := &proto.MetaPartitionReport{
		PartitionID: mp.PartitionID,
		Start:       mp.Start,
		End:         mp.End,
		Status:      int(mp.Status),
		MaxInodeID:  mp.Start + 1,
		IsLeader:    false,
		VolName:     mp.volName,
	}
	metaNode, err := server.cluster.metaNode(mp.Hosts[0])
	if err != nil {
		t.Error(err)
		return
	}
	if err = server.cluster.updateInodeIDUpperBound(mp, mr, true, metaNode); err != nil {
		t.Error(err)
		return
	}
	curMpLen := len(vol.MetaPartitions)
	if curMpLen == mpLen {
		t.Errorf("split failed,oldMpLen[%v],curMpLen[%v]", mpLen, curMpLen)
	}

}
