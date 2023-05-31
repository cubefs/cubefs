package master

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	commonVol, err := server.cluster.getVol(commonVolName)
	assert.NoError(t, err)
	commonVol.Capacity = 300 * unit.TB
	dpCount := len(commonVol.dataPartitions.partitions)
	commonVol.dataPartitions.readableAndWritableCnt = 0
	server.cluster.DisableAutoAllocate = false
	commonVol.checkAutoDataPartitionCreation(server.cluster)
	newDpCount := len(commonVol.dataPartitions.partitions)
	assert.NotEqualf(t, newDpCount, dpCount, "autoCreateDataPartitions failed,expand 0 data partitions,oldCount[%v],curCount[%v]", dpCount, newDpCount)
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
	if !assert.NoError(t, err) {
		return
	}
	vol.checkStatus(server.cluster)
	getVol(name, t)
	updateVol(name, "", capacity, t)
	statVol(name, t)
	markDeleteVol(name, t)
	getSimpleVol(name, t)
	vol.ConvertPartitionsToNewVol(server.cluster)
	oldDeleteMarkDelVolInterval := server.cluster.cfg.DeleteMarkDelVolInterval
	server.cluster.cfg.DeleteMarkDelVolInterval = 1
	defer func() { server.cluster.cfg.DeleteMarkDelVolInterval = oldDeleteMarkDelVolInterval }()
	vol.checkStatus(server.cluster)
	vol.deleteVolFromStore(server.cluster)
}

func createVol(name, zone string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfs&mpCount=2&zoneName=%v", hostAddr, proto.AdminCreateVol, name, zone)
	process(reqURL, t)
	vol, err := server.cluster.getVol(name)
	if !assert.NoError(t, err) {
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
	if !assert.NoError(t, err) {
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
	vol.ConvertPartitionsToNewVol(server.cluster)
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
	if !assert.NoError(t, err) {
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
	vol.ConvertPartitionsToNewVol(server.cluster)
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
	assert.Equalf(t, proto.ReadWrite, int(partition.Status), "expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
}

func checkMetaPartitionsWritableTest(vol *Vol, t *testing.T) {
	if !assert.NotZero(t, len(vol.MetaPartitions), "no meta partition") {
		return
	}
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(3 * time.Second)
	maxPartitionID := vol.maxPartitionID()
	maxMp := vol.MetaPartitions[maxPartitionID]
	//after check meta partitions ,the status must be writable
	maxMp.checkStatus(server.cluster.Name, false, int(vol.mpReplicaNum), maxPartitionID)
	if !assert.Equalf(t, proto.ReadWrite, int(maxMp.Status), "expect partition status[%v],real status[%v]\n", proto.ReadWrite, maxMp.Status) {
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
	if !assert.NoError(t, err) {
		return
	}
	if !assert.Equalf(t, uint64(capacity), vol.Capacity, "update vol failed,expect[%v],real[%v]", capacity, vol.Capacity) {
		return
	}
	if zone == "" {
		return
	}
	if !assert.Equalf(t, zone, vol.zoneName, "update vol failed,expect[%v],real[%v]", zone, vol.zoneName) {
		return
	}
}

func statVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v",
		hostAddr, proto.ClientVolStat, name)
	process(reqURL, t)
}

func markDeleteVol(name string, t *testing.T) {
	oldDeleteMarkDelVolInterval := server.cluster.cfg.DeleteMarkDelVolInterval
	server.cluster.cfg.DeleteMarkDelVolInterval = 0
	defer func() { server.cluster.cfg.DeleteMarkDelVolInterval = oldDeleteMarkDelVolInterval }()
	vol, err := server.cluster.getVol(name)
	if !assert.NoError(t, err) {
		return
	}
	reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v",
		hostAddr, proto.AdminDeleteVol, name, buildAuthKey("cfs"))
	process(reqURL, t)
	if !assert.Equalf(t, proto.VolStMarkDelete, vol.Status, "markDeleteVol failed,expect[%v],real[%v]", proto.VolStMarkDelete, vol.Status) {
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
		false, true, false, false, false, false, createTime, createTime, "", "", "", 0,
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
	assert.Zerof(t, manualDPCount, "expect manualDPCount is 0,but get :%v", manualDPCount)
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
	assert.Equalf(t, newCapacity, vol.Capacity, "expect Capacity is %v,but get :%v", newCapacity, vol.Capacity)
}

func TestRecoverVol(t *testing.T) {
	oldDeleteMarkDelVolInterval := server.cluster.cfg.DeleteMarkDelVolInterval
	defer func() { server.cluster.cfg.DeleteMarkDelVolInterval = oldDeleteMarkDelVolInterval }()
	server.cluster.cfg.DeleteMarkDelVolInterval = 1000000
	oldVolName := "oldVol123_del"
	newVolNameMarkDel := "newVol456_mark_del"
	owner := "test2"
	newVolNameRecover := "newVol456_recover"
	//do delete:oldVolName --> newVolNameMarkDel
	zoneName := fmt.Sprintf("%s,%s,%s", testZone1, testZone2, testZone3)
	reqURL := fmt.Sprintf("%v%v?name=%v&replicas=3&capacity=100&owner=%v&mpCount=10&enableToken=true&zoneName=%v", hostAddr, proto.AdminCreateVol, oldVolName, "cfs", zoneName)
	fmt.Println(reqURL)
	process(reqURL, t)
	oldVol, err := server.cluster.getVol(oldVolName)
	assertErrNilOtherwiseFailNow(t, err)
	oldVolTokens := oldVol.getAllTokens()
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)

	if err = server.cluster.renameVolToNewVolName(oldVolName, buildAuthKey("cfs"), newVolNameMarkDel, owner, proto.VolStMarkDelete); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err = server.associateVolWithUser(owner, newVolNameMarkDel); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err = server.user.deleteVolPolicy(oldVolName); err != nil {
		assert.FailNow(t, err.Error())
	}

	newVolMarkDel, err := server.cluster.getVol(newVolNameMarkDel)
	assertErrNilOtherwiseFailNow(t, err)
	assert.Equal(t, newVolMarkDel.OldVolName, oldVol.Name)
	assert.Equal(t, newVolMarkDel.ID, oldVol.NewVolID)
	compareVolValue(t, newVolMarkDel, oldVol)
	if err = server.cluster.batchConvertRenamedOldVolMpMetadataToNewVol(oldVol, newVolMarkDel, 2); err != nil {
		assert.FailNow(t, err.Error())
	}
	if err = server.cluster.batchConvertRenamedOldVolDpMetadataToNewVol(oldVol, newVolMarkDel, 3); err != nil {
		assert.FailNow(t, err.Error())
	}
	oldVol.ConvertPartitionsToNewVol(server.cluster)

	checkNewRenamedVolDp(t, newVolMarkDel)
	checkNewRenamedVolMp(t, newVolMarkDel)
	checkClientViews(t, newVolMarkDel)

	//rename recover:newVolNameMarkDel --> newVolNameRecover
	err = mc.AdminAPI().RecoverVolume(newVolNameMarkDel, buildAuthKey(owner), newVolNameRecover)
	assertErrNilOtherwiseFailNow(t, err)
	newVolMarkDel, err = server.cluster.getVol(newVolNameMarkDel)
	assertErrNilOtherwiseFailNow(t, err)
	assert.Equal(t, newVolMarkDel.NewVolName, newVolNameRecover)
	assert.Equal(t, newVolMarkDel.RenameConvertStatus, proto.VolRenameConvertStatusOldVolInConvertPartition)
	newRenamedVol, err := server.cluster.getVol(newVolMarkDel.NewVolName)
	assertErrNilOtherwiseFailNow(t, err)
	assert.Equal(t, newRenamedVol.OldVolName, newVolMarkDel.Name)
	assert.Equal(t, newVolMarkDel.NewVolID, newRenamedVol.ID)
	assert.Equal(t, newRenamedVol.RenameConvertStatus, proto.VolRenameConvertStatusNewVolInConvertPartition)
	assert.Equal(t, newRenamedVol.FinalVolStatus, proto.VolStNormal)
	newVolRecover, err := server.cluster.getVol(newVolNameRecover)
	assertErrNilOtherwiseFailNow(t, err)
	oldMps := newVolMarkDel.cloneMetaPartitionMap()
	oldDps := newVolMarkDel.cloneDataPartitionMap()

	wg := new(sync.WaitGroup)
	wg.Add(3)
	go func() {
		defer wg.Done()
		for _, mp := range oldMps {
			err1 := mc.AdminAPI().DecommissionMetaPartition(mp.PartitionID, mp.Hosts[len(mp.Hosts)-1], "", 0)
			if err1 != nil {
				t.Errorf("DecommissionMetaPartition err:%v", err1)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for _, dp := range oldDps {
			err1 := mc.AdminAPI().DecommissionDataPartition(dp.PartitionID, dp.Hosts[len(dp.Hosts)-1], "")
			if err1 != nil {
				t.Errorf("DecommissionDataPartition err:%v", err1)
			}
		}
	}()
	go func() {
		defer wg.Done()
		newVolMarkDel.ConvertPartitionsToNewVol(server.cluster)
		if err = server.cluster.batchConvertRenamedOldVolMpMetadataToNewVol(newVolMarkDel, newVolRecover, 2); err != nil {
			t.Error(err)
			return
		}
		if err = server.cluster.batchConvertRenamedOldVolDpMetadataToNewVol(newVolMarkDel, newVolRecover, 3); err != nil {
			t.Error(err)
			return
		}
		newVolMarkDel.ConvertPartitionsToNewVol(server.cluster)
	}()
	wg.Wait()
	newVolRecover, err = server.cluster.getVol(newVolNameRecover)
	assertErrNilOtherwiseFailNow(t, err)
	assert.Equal(t, newVolRecover.Status, proto.VolStNormal)
	assert.Equal(t, newVolMarkDel.RenameConvertStatus, proto.VolRenameConvertStatusOldVolNeedDel)
	newMps := newVolRecover.cloneMetaPartitionMap()
	newDps := newVolRecover.cloneDataPartitionMap()

	assert.Equal(t, len(newMps), len(oldMps))
	assert.Equal(t, len(newDps), len(oldDps))
	for _, mp := range newMps {
		assert.Equal(t, mp.volName, newVolRecover.Name)
	}
	for _, dp := range newDps {
		assert.Equal(t, dp.VolName, newVolRecover.Name)
	}

	newVolTokens := newVolRecover.getAllTokens()
	t.Log(fmt.Sprintf("oldVolTokensCount:%v,newVolTokensCount:%v", len(oldVolTokens), len(newVolTokens)))
	assert.Equal(t, len(oldVolTokens), len(newVolTokens))
	for _, token := range newVolTokens {
		flag := false
		for _, oldVolToken := range oldVolTokens {
			if token.TokenType == oldVolToken.TokenType && token.Value == oldVolToken.Value {
				flag = true
				break
			}
		}
		if !flag {
			t.Errorf("can not found new token:%v in old token info", token.Value)
		}
	}
}

func compareVolValue(t *testing.T, newVolMarkDel, oldVol *Vol) {
	newVolMarkDelVolValue := newVolValue(newVolMarkDel)
	oldVolVolValue := newVolValue(oldVol)

	newVolMarkDelVolValue.ID = 0
	newVolMarkDelVolValue.Name = ""
	newVolMarkDelVolValue.Owner = ""
	newVolMarkDelVolValue.NewVolName = ""
	newVolMarkDelVolValue.OldVolName = ""
	newVolMarkDelVolValue.NewVolID = 0
	newVolMarkDelVolValue.Status = 0
	newVolMarkDelVolValue.MarkDeleteTime = 0
	newVolMarkDelVolValue.RenameConvertStatus = 0
	newVolMarkDelVolValue.FinalVolStatus = 0

	oldVolVolValue.ID = 0
	oldVolVolValue.Name = ""
	oldVolVolValue.Owner = ""
	oldVolVolValue.NewVolName = ""
	oldVolVolValue.OldVolName = ""
	oldVolVolValue.NewVolID = 0
	oldVolVolValue.Status = 0
	oldVolVolValue.MarkDeleteTime = 0
	oldVolVolValue.RenameConvertStatus = 0
	oldVolVolValue.FinalVolStatus = 0
	assert.Equal(t, *newVolMarkDelVolValue, *oldVolVolValue)
}

func checkNewRenamedVolDp(t *testing.T, newRenamedVol *Vol) {
	dataPartitionMap := newRenamedVol.cloneDataPartitionMap()
	if !assert.NotZerof(t, len(dataPartitionMap), "dpCount should not be 0") {
		return
	}
	nodeDpMap := make(map[string][]uint64)
	for _, partition := range dataPartitionMap {
		for _, host := range partition.Hosts {
			nodeDpMap[host] = append(nodeDpMap[host], partition.PartitionID)
		}
	}
	for addr, dpIDs := range nodeDpMap {
		dataNodeInfo, err := mc.NodeAPI().GetDataNode(addr)
		assertErrNilOtherwiseFailNow(t, err)
		for _, id := range dpIDs {
			assert.Containsf(t, dataNodeInfo.PersistenceDataPartitions, id, "id:%v expect in node:%v but not", id, addr)
		}
	}
}

func checkNewRenamedVolMp(t *testing.T, newRenamedVol *Vol) {
	mps := newRenamedVol.cloneMetaPartitionMap()
	if !assert.NotZero(t, len(mps), "mpCount should not be 0") {
		return
	}
	nodeMpMap := make(map[string][]uint64)
	for _, partition := range mps {
		for _, host := range partition.Hosts {
			nodeMpMap[host] = append(nodeMpMap[host], partition.PartitionID)
		}
	}
	for addr, mpIDs := range nodeMpMap {
		metaNodeInfo, err := mc.NodeAPI().GetMetaNode(addr)
		assertErrNilOtherwiseFailNow(t, err)
		for _, id := range mpIDs {
			assert.Containsf(t, metaNodeInfo.PersistenceMetaPartitions, id, "id:%v expect in node:%v but not", id, addr)
		}
	}
}

func checkClientViews(t *testing.T, newRenamedVol *Vol) {
	mps := newRenamedVol.cloneMetaPartitionMap()
	dataPartitionMap := newRenamedVol.cloneDataPartitionMap()
	newRenamedVol.mpsCache = nil
	newRenamedVol.viewCache = nil
	volView, err := mc.ClientAPI().GetVolume(newRenamedVol.Name, buildAuthKey(newRenamedVol.Owner))
	assertErrNilOtherwiseFailNow(t, err)
	assert.Equal(t, len(volView.MetaPartitions), len(mps))
	assert.Equal(t, len(volView.DataPartitions), len(dataPartitionMap))
	dataPartitionsView, err := mc.ClientAPI().GetDataPartitions(newRenamedVol.Name)
	assertErrNilOtherwiseFailNow(t, err)
	assert.Equal(t, len(dataPartitionsView.DataPartitions), len(dataPartitionMap))
	metaPartitions, err := mc.ClientAPI().GetMetaPartitions(newRenamedVol.Name)
	assertErrNilOtherwiseFailNow(t, err)
	assert.Equal(t, len(metaPartitions), len(mps))
	for _, partition := range dataPartitionsView.DataPartitions {
		_, ok := dataPartitionMap[partition.PartitionID]
		if !ok {
			t.Errorf("can not found dpId:%v", partition.PartitionID)
		} else {
			delete(dataPartitionMap, partition.PartitionID)
		}
	}
	assert.Equal(t, len(dataPartitionMap), 0, "rest dps count")
	for _, partition := range volView.MetaPartitions {
		_, ok := mps[partition.PartitionID]
		if !ok {
			t.Errorf("can not found mpId:%v", partition.PartitionID)
		} else {
			delete(mps, partition.PartitionID)
		}
	}
	assert.Equal(t, len(mps), 0, "rest mps count")
}

func TestForceDeleteVolume(t *testing.T) {
	//delete vol by rename then force delete the vol
	volName := "test_force_delete"
	testOwner := "cfs"
	zoneName := fmt.Sprintf("%s,%s,%s", testZone1, testZone2, testZone3)
	reqURL := fmt.Sprintf("%v%v?name=%v&replicas=3&capacity=100&owner=%v&mpCount=10&enableToken=true&zoneName=%v", hostAddr, proto.AdminCreateVol, volName, testOwner, zoneName)
	fmt.Println(reqURL)
	process(reqURL, t)
	oldVol, err := server.cluster.getVol(volName)
	assertErrNilOtherwiseFailNow(t, err)
	oldDeleteMarkDelVolInterval := server.cluster.cfg.DeleteMarkDelVolInterval
	defer func() { server.cluster.cfg.DeleteMarkDelVolInterval = oldDeleteMarkDelVolInterval }()
	server.cluster.cfg.DeleteMarkDelVolInterval = 1000000
	err = mc.AdminAPI().DeleteVolume(volName, buildAuthKey(testOwner))
	assertErrNilOtherwiseFailNow(t, err)
	if err = mc.AdminAPI().ForceDeleteVolume(oldVol.Name, buildAuthKey(testOwner)); err != nil {
		assert.FailNow(t, err.Error())
	}
	newVolName := oldVol.NewVolName
	nVol, err := server.cluster.getVol(newVolName)
	assertErrNilOtherwiseFailNow(t, err)
	if err = mc.AdminAPI().ForceDeleteVolume(newVolName, buildAuthKey(testOwner)); err != nil {
		assert.FailNow(t, err.Error())
	}
	for i := 0; i < 3; i++ {
		oldVol.checkStatus(server.cluster)
		nVol.checkStatus(server.cluster)
		time.Sleep(time.Second)
	}
	isDeleteStatus := func(v *Vol) bool {
		t.Logf("vol:%v Status:%v,RenameConvertStatus:%v,MarkDeleteTime:%v", v.Name, v.Status, v.RenameConvertStatus, v.MarkDeleteTime)
		if v.Status == 1 && v.RenameConvertStatus == 0 && v.MarkDeleteTime == 0 {
			return true
		}
		return false
	}
	if oldVol, err = server.cluster.getVol(oldVol.Name); err == nil && !isDeleteStatus(oldVol) {
		t.Errorf("vol:%v should not be exist, Status:%v,RenameConvertStatus:%v,MarkDeleteTime:%v", oldVol.Name, oldVol.Status, oldVol.RenameConvertStatus, oldVol.MarkDeleteTime)
		return
	}
	if nVol, err = server.cluster.getVol(newVolName); err == nil && !isDeleteStatus(nVol) {
		t.Errorf("vol:%v should not be exist, Status:%v,RenameConvertStatus:%v,MarkDeleteTime:%v", newVolName, nVol.Status, nVol.RenameConvertStatus, nVol.MarkDeleteTime)
		return
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
