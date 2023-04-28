package master

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	commonVol.Capacity = 300 * unit.TB
	dpCount := len(commonVol.dataPartitions.partitions)
	commonVol.dataPartitions.readableAndWritableCnt = 0
	server.cluster.DisableAutoAllocate = false
	commonVol.checkAutoDataPartitionCreation(server.cluster)
	newDpCount := len(commonVol.dataPartitions.partitions)
	if dpCount == newDpCount {
		t.Errorf("autoCreateDataPartitions failed,expand 0 data partitions,oldCount[%v],curCount[%v]", dpCount, newDpCount)
		return
	}
}

func TestCheckVol(t *testing.T) {
	commonVol.checkStatus(server.cluster)
	commonVol.checkMetaPartitions(server.cluster)
	commonVol.checkDataPartitions(server.cluster)
	log.LogFlush()
}

func TestVol(t *testing.T) {
	capacity := 200
	name := "test1"
	createVol(name, testZone2, t)
	//report mp/dp info to master
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	//check status
	server.cluster.checkMetaPartitions()
	server.cluster.checkDataPartitions()
	server.cluster.checkLoadMetaPartitions()
	server.cluster.doLoadDataPartitions()
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	vol.checkStatus(server.cluster)
	getVol(name, t)
	updateVol(name, "", capacity, t)
	statVol(name, t)
	markDeleteVol(name, t)
	getSimpleVol(name, t)
	vol.checkStatus(server.cluster)
	vol.deleteVolFromStore(server.cluster)
}

func createVol(name, zone string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfs&mpCount=2&zoneName=%v", hostAddr, proto.AdminCreateVol, name, zone)
	process(reqURL, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	checkDataPartitionsWritableTest(vol, t)
	checkMetaPartitionsWritableTest(vol, t)
}

func TestVolMultiZoneDowngrade(t *testing.T) {
	var vol *Vol
	var err error
	testMultiZone := "multiZoneDowngrade"
	zoneList := []string{testZone1, testZone2, testZone3}
	zone := strings.Join(zoneList, ",")
	server.cluster.t.putZoneIfAbsent(newZone(testZone3))
	createVol(testMultiZone, zone, t)
	//report mp/dp info to master
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(3 * time.Second)
	//check status
	server.cluster.checkMetaPartitions()
	server.cluster.checkDataPartitions()
	server.cluster.checkLoadMetaPartitions()
	server.cluster.doLoadDataPartitions()
	vol, err = server.cluster.getVol(testMultiZone)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}

	vol.checkStatus(server.cluster)
	getVol(testMultiZone, t)
	updateVol(testMultiZone, zone, 200, t)
	statVol(testMultiZone, t)

	time.Sleep(3 * time.Second)
	server.cluster.cfg = newClusterConfig()

	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()

	server.cluster.checkVolRepairDataPartitions()
	server.cluster.checkVolRepairMetaPartitions()

	/*time.Sleep(time.Second * 10)
	var mps map[uint64]*MetaPartition
	mps = vol.cloneMetaPartitionMap()
	var isRecover bool
	if isRecover, err = checkZoneRecover(mps, zoneList, t); err != nil {
		t.Errorf("err is %v", err)
	}
	if isRecover {
		t.Errorf("checkVolRepairMetaPartition is forbidden when recover pool size equals -1")
	}*/
	//test normal recover
	server.cluster.cfg.MetaPartitionsRecoverPoolSize = maxMetaPartitionsRecoverPoolSize
	server.cluster.cfg.DataPartitionsRecoverPoolSize = maxDataPartitionsRecoverPoolSize
	server.cluster.checkVolRepairDataPartitions()
	server.cluster.checkVolRepairMetaPartitions()
	//wait for the partitions to be repaired
	/*time.Sleep(time.Second * 10)
	mps = vol.cloneMetaPartitionMap()
	if isRecover, err = checkZoneRecover(mps, zoneList, t); err != nil {
		t.Errorf("err is %v", err)
	}
	if !isRecover {
		t.Errorf("checkVolRepairMetaPartition recover failed")
	}*/
	markDeleteVol(testMultiZone, t)
	getSimpleVol(testMultiZone, t)
	vol.checkStatus(server.cluster)
	vol.deleteVolFromStore(server.cluster)
}

func checkZoneRecover(mps map[uint64]*MetaPartition, zoneList []string, t *testing.T) (isRecover bool, err error) {
	var curZone []string
	isRecover = true
	for _, mp := range mps {
		curZone = make([]string, 0)
		for _, host := range mp.Hosts {
			var mn *MetaNode
			if mn, err = server.cluster.metaNode(host); err != nil {
				return
			}
			if !contains(curZone, mn.ZoneName) {
				curZone = append(curZone, mn.ZoneName)
			}
		}
		if len(curZone) != len(zoneList) {
			t.Logf("vol[%v], meta partition[%v] recover from downgrade failed, curZone:%v, zoneList:%v", mp.volName, mp.PartitionID, curZone, zoneList)
			isRecover = false
			continue
		}
		t.Logf("vol[%v], meta partition[%v] recover from downgrade successfully!", mp.volName, mp.PartitionID)
	}
	return
}
func TestVolMultiZone(t *testing.T) {
	var vol *Vol
	var err error
	testMultiZone := "multiZone"
	zoneList := []string{testZone1, testZone2, testZone3}
	zone := strings.Join(zoneList, ",")
	createVol(testMultiZone, zone, t)
	//report mp/dp info to master
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(3 * time.Second)
	//check status
	server.cluster.checkMetaPartitions()
	server.cluster.checkDataPartitions()
	server.cluster.checkLoadMetaPartitions()
	server.cluster.doLoadDataPartitions()
	vol, err = server.cluster.getVol(testMultiZone)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	vol.checkStatus(server.cluster)
	getVol(testMultiZone, t)
	updateVol(testMultiZone, testZone1+","+testZone2, 200, t)
	statVol(testMultiZone, t)
	//check repair the first replica
	server.cluster.checkVolRepairDataPartitions()
	server.cluster.checkVolRepairMetaPartitions()
	//set partition isRecovering to false
	server.cluster.checkDiskRecoveryProgress()
	server.cluster.checkMigratedDataPartitionsRecoveryProgress()
	server.cluster.checkMetaPartitionRecoveryProgress()
	server.cluster.checkMigratedMetaPartitionRecoveryProgress()
	//check repair the second replica, so all replicas should have been repaired
	server.cluster.checkVolRepairDataPartitions()
	server.cluster.checkVolRepairMetaPartitions()
	//wait for the partitions to be repaired
	/*time.Sleep(time.Second * 5)
	mps := vol.cloneMetaPartitionMap()
	var isRecover bool
	if isRecover, err = checkZoneRecover(mps, []string{testZone1, testZone2}, t); err != nil {
		t.Errorf("err is %v", err)
	}
	if !isRecover {
		t.Errorf("checkVolRepairMetaPartition recover failed")
	}*/

	markDeleteVol(testMultiZone, t)
	getSimpleVol(testMultiZone, t)
	vol.checkStatus(server.cluster)
	vol.deleteVolFromStore(server.cluster)
}

func checkDataPartitionsWritableTest(vol *Vol, t *testing.T) {
	if len(vol.dataPartitions.partitions) == 0 {
		return
	}
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(3 * time.Second)
	//after check data partitions ,the status must be writable
	vol.checkDataPartitions(server.cluster)
	partition := vol.dataPartitions.partitions[0]
	if partition.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
		return
	}
}

func checkMetaPartitionsWritableTest(vol *Vol, t *testing.T) {
	if len(vol.MetaPartitions) == 0 {
		t.Error("no meta partition")
		return
	}
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(3 * time.Second)
	maxPartitionID := vol.maxPartitionID()
	maxMp := vol.MetaPartitions[maxPartitionID]
	//after check meta partitions ,the status must be writable
	maxMp.checkStatus(server.cluster.Name, false, int(vol.mpReplicaNum), maxPartitionID)
	if maxMp.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, maxMp.Status)
		return
	}
}

func getSimpleVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.AdminGetVol, name)
	process(reqURL, t)
}

func getVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.ClientVol, name, buildAuthKey("cfs"))
	process(reqURL, t)
}

func updateVol(name, zone string, capacity int, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v&zoneName=%v",
		hostAddr, proto.AdminUpdateVol, name, capacity, buildAuthKey("cfs"), zone)
	process(reqURL, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Capacity != uint64(capacity) {
		t.Errorf("update vol failed,expect[%v],real[%v]", capacity, vol.Capacity)
		return
	}
	if zone == "" {
		return
	}
	if vol.zoneName != zone {
		t.Errorf("update vol failed,expect[%v],real[%v]", zone, vol.zoneName)
		return
	}
}

func statVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v",
		hostAddr, proto.ClientVolStat, name)
	process(reqURL, t)
}

func markDeleteVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v",
		hostAddr, proto.AdminDeleteVol, name, buildAuthKey("cfs"))
	process(reqURL, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Status != proto.VolStMarkDelete {
		t.Errorf("markDeleteVol failed,expect[%v],real[%v]", proto.VolStMarkDelete, vol.Status)
		return
	}
}

//func TestVolReduceReplicaNum(t *testing.T) {
//	volName := "reduce-replica-num"
//	vol, err := server.cluster.createVol(volName, volName, testZone2, 3, 3, unit.DefaultDataPartitionSize,
//		100, false, false, false, false)
//	if err != nil {
//		t.Error(err)
//		return
//	}
//	server.cluster.checkDataNodeHeartbeat()
//	time.Sleep(2 * time.Second)
//	for _, dp := range vol.dataPartitions.partitionMap {
//		t.Logf("dp[%v] replicaNum[%v],hostLen[%v]\n", dp.PartitionID, dp.ReplicaNum, len(dp.Hosts))
//	}
//	oldReplicaNum := vol.dpReplicaNum
//	reqURL := fmt.Sprintf("%v%v?name=%v&capacity=%v&replicaNum=%v&authKey=%v",
//		hostAddr, proto.AdminUpdateVol, volName, 100, 2, buildAuthKey(volName))
//	process(reqURL, t)
//	if vol.dpReplicaNum != 2 {
//		t.Error("update vol replica Num to [2] failed")
//		return
//	}
//	for i := 0; i < int(oldReplicaNum); i++ {
//		t.Logf("before check,needToLowerReplica[%v] \n", vol.NeedToLowerReplica)
//		vol.NeedToLowerReplica = true
//		t.Logf(" after check,needToLowerReplica[%v]\n", vol.NeedToLowerReplica)
//		vol.checkReplicaNum(server.cluster)
//	}
//	vol.NeedToLowerReplica = true
//	//check more once,the replica num of data partition must be equal with vol.dpReplicaNun
//	vol.checkReplicaNum(server.cluster)
//	for _, dp := range vol.dataPartitions.partitionMap {
//		if dp.ReplicaNum != vol.dpReplicaNum || len(dp.Hosts) != int(vol.dpReplicaNum) {
//			t.Errorf("dp.replicaNum[%v],hosts[%v],vol.dpReplicaNum[%v]\n", dp.ReplicaNum, len(dp.Hosts), vol.dpReplicaNum)
//			return
//		}
//	}
//}

func TestConcurrentReadWriteDataPartitionMap(t *testing.T) {
	name := "TestConcurrentReadWriteDataPartitionMap"
	var volID uint64 = 1
	var createTime = time.Now().Unix()
	vol := newVol(volID, name, name, "", unit.DefaultDataPartitionSize, 100, defaultReplicaNum,
		defaultReplicaNum, false, false,
		false, true, false, false, false, false, false, createTime, createTime, "", "", "", 0,
		0, 0, 0.0, 30, 0, proto.StoreModeMem, proto.VolConvertStInit, proto.MetaPartitionLayout{0, 0},
		strings.Split(testSmartRules, ","), proto.CompactDefault, proto.DpFollowerReadDelayConfig{false, 0},
		0, 0)
	// unavailable mp
	mp1 := newMetaPartition(1, 1, defaultMaxMetaPartitionInodeID, 3, 0, name, volID)
	vol.addMetaPartition(mp1)
	//readonly mp
	mp2 := newMetaPartition(2, 1, defaultMaxMetaPartitionInodeID, 3, 0, name, volID)
	mp2.Status = proto.ReadOnly
	vol.addMetaPartition(mp2)
	vol.updateViewCache(server.cluster)
	for id := 0; id < 30000; id++ {
		dp := newDataPartition(uint64(id), 3, name, volID)
		vol.dataPartitions.put(dp)
	}
	go func() {
		var id uint64 = 30000
		for {
			id++
			dp := newDataPartition(id, 3, name, volID)
			vol.dataPartitions.put(dp)
			time.Sleep(time.Second)
		}
	}()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		vol.updateViewCache(server.cluster)
	}
}

func TestVolBatchUpdateDps(t *testing.T) {
	volName := commonVolName
	vol, err := server.cluster.getVol(volName)
	if err != nil || vol == nil {
		t.Errorf("getVol:%v err:%v", volName, err)
		return
	}
	for _, dataPartition := range vol.cloneDataPartitionMap() {
		dataPartition.IsManual = false
	}

	count := vol.dataPartitions.readableAndWritableCnt / 2
	reqURL := fmt.Sprintf("%v%v?name=%v&isManual=%v&count=%v&start=1&end=1000&medium=all",
		hostAddr, proto.AdminVolBatchUpdateDps, vol.Name, true, count)
	process(reqURL, t)

	manualDPCount := 0
	var minIsManualDpID, maxIsManualDpID uint64
	minIsManualDpID = math.MaxUint64
	for _, dataPartition := range vol.cloneDataPartitionMap() {
		if dataPartition.IsManual {
			manualDPCount++
			if dataPartition.PartitionID < minIsManualDpID {
				minIsManualDpID = dataPartition.PartitionID
			}
			if dataPartition.PartitionID > maxIsManualDpID {
				maxIsManualDpID = dataPartition.PartitionID
			}
		}
	}
	if manualDPCount != count {
		t.Errorf("expect count is %v,but get manualDPCount:%v", count, manualDPCount)
		return
	}
	reqURL = fmt.Sprintf("%v%v?name=%v&isManual=%v&start=%v&end=%v&medium=hdd",
		hostAddr, proto.AdminVolBatchUpdateDps, vol.Name, false, minIsManualDpID, maxIsManualDpID)
	process(reqURL, t)
	reqURL = fmt.Sprintf("%v%v?name=%v&isManual=%v&start=%v&end=%v&medium=ssd",
		hostAddr, proto.AdminVolBatchUpdateDps, vol.Name, false, minIsManualDpID, maxIsManualDpID)
	process(reqURL, t)
	manualDPCount = 0
	for _, dataPartition := range vol.cloneDataPartitionMap() {
		if dataPartition.IsManual {
			manualDPCount++
		}
	}
	if manualDPCount != 0 {
		t.Errorf("expect manualDPCount is 0,but get :%v", manualDPCount)
	}
}

func TestShrinkVolCapacity(t *testing.T) {
	volName := commonVolName
	vol, err := server.cluster.getVol(volName)
	if err != nil || vol == nil {
		t.Errorf("getVol:%v err:%v", volName, err)
		return
	}
	newCapacity := vol.totalUsedSpace()/unit.GB + 10
	if newCapacity >= vol.Capacity {
		t.Logf("newCapacity more than vol Capacity, need increase it")
		reqURL := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v",
			hostAddr, proto.AdminUpdateVol, commonVol.Name, newCapacity+1000, buildAuthKey("cfs"))
		process(reqURL, t)
	}
	reqURL := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v",
		hostAddr, proto.AdminShrinkVolCapacity, commonVol.Name, newCapacity, buildAuthKey(vol.Owner))
	process(reqURL, t)
	if vol.Capacity != newCapacity {
		t.Errorf("expect Capacity is %v,but get :%v", newCapacity, vol.Capacity)
	}
}

func TestGetMaxCapacityWithReservedTrashSpace(t *testing.T) {
	volName := commonVolName
	vol, err := server.cluster.getVol(volName)
	if err != nil || vol == nil {
		t.Errorf("getVol:%v err:%v", volName, err)
		return
	}
	oldCap := vol.Capacity
	oldTrashRemainingDays := vol.trashRemainingDays
	defer func() {
		vol.Capacity = oldCap
		vol.trashRemainingDays = oldTrashRemainingDays
	}()
	capacity1 := uint64(volLowCapThresholdForReservedTrashSpace - 1)
	capacity2 := uint64(volLowCapThresholdForReservedTrashSpace)
	testCases := []struct {
		capacity uint64
		trashDay uint32
		expect   uint64
	}{
		{vol.Capacity, 0, vol.Capacity},
		{capacity1, 1, uint64(float64(capacity1) * volLowCapMaxCapacityRatioForReservedTrashSpace)},
		{capacity2, 1, uint64(float64(capacity2) * volDefaultMaxCapacityRatioForReservedTrashSpace)},
	}
	for i, testCase := range testCases {
		vol.Capacity = testCase.capacity
		vol.trashRemainingDays = testCase.trashDay
		maxCap := vol.getMaxCapacityWithReservedTrashSpace()
		assert.Equal(t, testCase.expect, maxCap, fmt.Sprintf("testCase:%v", i))
	}
}

func TestCheckAndUpdatePartitionReplicaNum(t *testing.T) {
	volName := "test_update_replica"
	createVol(volName, testZone2, t)
	vol, err := server.cluster.getVol(volName)
	if err != nil || vol == nil {
		t.Errorf("getVol:%v err:%v", volName, err)
		return
	}
	//will not change
	vol.dpReplicaNum = 5
	vol.mpReplicaNum = 5
	vol.checkAndUpdateDataPartitionReplicaNum(server.cluster)
	vol.checkAndUpdateMetaPartitionReplicaNum(server.cluster)
	for _, partition := range vol.allDataPartition() {
		assert.Equal(t, uint8(3), partition.ReplicaNum)
	}
	for _, partition := range vol.allMetaPartition() {
		assert.Equal(t, uint8(3), partition.ReplicaNum)
	}
	mpCount := len(vol.allMetaPartition())
	dpCount := len(vol.allDataPartition())
	diffMpIDs, diffDpIDs := vol.checkIsDataPartitionAndMetaPartitionReplicaNumSameWithVolReplicaNum()
	assert.Equal(t, mpCount, len(diffMpIDs))
	assert.Equal(t, dpCount, len(diffDpIDs))

	//update hosts info
	fakeHosts := []string{"192.168.1.901:6000", "192.168.1.902:6000"}
	for _, partition := range vol.allDataPartition() {
		partition.Hosts = append(partition.Hosts, fakeHosts...)
		dpCount--
		_, diffDpIDs = vol.checkIsDataPartitionAndMetaPartitionReplicaNumSameWithVolReplicaNum()
		assert.Equal(t, dpCount, len(diffDpIDs))
	}
	for _, partition := range vol.allMetaPartition() {
		partition.Hosts = append(partition.Hosts, fakeHosts...)
		mpCount--
		diffMpIDs, _ = vol.checkIsDataPartitionAndMetaPartitionReplicaNumSameWithVolReplicaNum()
		assert.Equal(t, mpCount, len(diffMpIDs))
	}
	diffMpIDs, diffDpIDs = vol.checkIsDataPartitionAndMetaPartitionReplicaNumSameWithVolReplicaNum()
	assert.Equal(t, 0, len(diffMpIDs))
	assert.Equal(t, 0, len(diffDpIDs))

	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.AdminCheckVolPartitionReplica, volName)
	process(reqURL, t)
	testCases := []struct {
		DpReplicaNum uint8
		MpReplicaNum uint8
	}{
		{3, 5},
		{5, 3},
		{3, 3},
		{5, 5},
		{3, 3},
	}
	for i, testCase := range testCases {
		vol.dpReplicaNum = testCase.DpReplicaNum
		vol.mpReplicaNum = testCase.MpReplicaNum

		vol.checkAndUpdateDataPartitionReplicaNum(server.cluster)
		vol.checkAndUpdateMetaPartitionReplicaNum(server.cluster)
		for _, partition := range vol.allDataPartition() {
			assert.Equal(t, testCase.DpReplicaNum, partition.ReplicaNum, fmt.Sprintf("testCase:%v,dp", i))
		}
		for _, partition := range vol.allMetaPartition() {
			assert.Equal(t, testCase.MpReplicaNum, partition.ReplicaNum, fmt.Sprintf("testCase:%v,mp", i))
		}
	}
	markDeleteVol(volName, t)
}
