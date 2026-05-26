package master

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/cubefs/cubefs/util/log"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}

	commonVol.dataPartitions.lastAutoCreateTime = time.Unix(time.Now().Unix()-3600, 0)

	commonVol.Capacity = 300 * util.TB
	dpCount := len(commonVol.dataPartitions.partitions)
	commonVol.dataPartitions.setReadWriteDataPartitionCntByMediaType(0, defaultMediaType)
	commonVol.dataPartitions.lastAutoCreateTime = time.Now().Add(-time.Minute)
	server.cluster.DisableAutoAllocate = false
	t.Logf("status[%v],disableAutoAlloc[%v],cap[%v],volStorageClass[%v]\n",
		commonVol.Status, server.cluster.DisableAutoAllocate, commonVol.Capacity,
		proto.StorageClassString(commonVol.volStorageClass))

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
	t.Logf("writable data partitions[%v]\n", commonVol.dataPartitions.readableAndWritableCnt)
}

func TestVol(t *testing.T) {
	name := "test1"
	createVol(map[string]interface{}{nameKey: name}, t)
	// report mp/dp info to master
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	// check status
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
	statVol(name, t)
	delVol(name, t)
	time.Sleep(5 * time.Second)
	getSimpleVol(name, true, t)
	vol.checkStatus(server.cluster)
	err = vol.deleteVolFromStore(server.cluster)
	if err != nil {
		panic(err)
	}
}

func TestCreateColdVol(t *testing.T) {
	volName1 := "coldVol"
	volName2 := "coldVol2"
	volName3 := "coldVol3"

	req := map[string]interface{}{}
	// name can't be empty
	checkCreateVolParam(nameKey, req, "", volName1, t)
	// name regex is illegal
	checkCreateVolParam(nameKey, req, "_vol", volName1, t)
	// owner empty
	checkCreateVolParam(volOwnerKey, req, "", testOwner, t)
	// owner illegal
	checkCreateVolParam(volOwnerKey, req, "+owner", testOwner, t)
	// capacity can't be empty
	checkCreateVolParam(volCapacityKey, req, "", 100, t)
	req[remoteCacheReadTimeout] = proto.ReadDeadlineTime
	req[zoneNameKey] = testZone2
	req[StoreModeKey] = proto.StoreModeMem
	processWithFatalV2(proto.AdminCreateVol, true, req, t)

	// check default val of normal vol
	vol, err := server.cluster.getVol(volName1)
	require.NoError(t, err)
	require.EqualValues(t, defaultInitMetaPartitionCount, len(vol.MetaPartitions))
	require.False(t, vol.FollowerRead)
	require.False(t, vol.authenticate)
	require.False(t, vol.crossZone)
	require.EqualValues(t, 100, vol.capacity())
	require.EqualValues(t, proto.VolumeTypeHot, vol.VolType)
	require.EqualValues(t, 0, vol.domainId)

	delVol(volName1, t)
	deleteMarkedVolFromStore(volName1, t)

	req[nameKey] = volName2
	req[poolIdKey] = proto.DefaultECPoolId

	processWithFatalV2(proto.AdminCreateVol, true, req, t)

	// check default val of LF vol
	vol, err = server.cluster.getVol(volName2)
	require.NoError(t, err)
	require.EqualValues(t, defaultEbsBlkSize, vol.EbsBlkSize)

	delVol(volName2, t)
	deleteMarkedVolFromStore(volName2, t)

	req[nameKey] = volName3

	blkSize := 7 * 1024 * 1024

	// check with illegal args
	checkCreateVolParam(ebsBlkSizeKey, req, -1, blkSize, t)
	checkCreateVolParam(followerReadKey, req, -1, true, t)

	processWithFatalV2(proto.AdminCreateVol, true, req, t)

	view := getSimpleVol(volName3, true, t)
	assert.True(t, view.ObjBlockSize == blkSize)

	delVol(volName3, t)
	deleteMarkedVolFromStore(volName3, t)

	// NOTE: check all vols
	timeout := time.Now().Add(100 * time.Second)
	for time.Now().Before(timeout) {
		_, err = server.cluster.getVol(volName1)
		if err == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		require.ErrorIs(t, err, proto.ErrVolNotExists)

		_, err = server.cluster.getVol(volName2)
		if err == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		require.ErrorIs(t, err, proto.ErrVolNotExists)

		_, err = server.cluster.getVol(volName3)
		if err == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		require.ErrorIs(t, err, proto.ErrVolNotExists)
		return
	}

	t.Errorf("Delete cold vols timeout")
}

func deleteMarkedVolFromStore(name string, t *testing.T) {
	t.Helper()

	vol, err := server.cluster.getVol(name)
	require.NoError(t, err)
	require.Equal(t, proto.VolStatusMarkDelete, vol.Status)
	require.NoError(t, vol.deleteVolFromStore(server.cluster))

	_, err = server.cluster.getVol(name)
	require.ErrorIs(t, err, proto.ErrVolNotExists)
}

func checkCreateVolParam(key string, req map[string]interface{}, wrong, correct interface{}, t *testing.T) {
	checkParam(key, proto.AdminCreateVol, req, wrong, correct, t)
}

func checkParam(key, url string, req map[string]interface{}, wrong, correct interface{}, t *testing.T) {
	req[key] = wrong
	processWithFatalV2(url, false, req, t)
	// set correct
	req[key] = correct
}

func setParam(key, url string, req map[string]interface{}, val interface{}, t *testing.T) {
	req[key] = val
	processWithFatalV2(url, true, req, t)
}

func buildUrl(host, op string, kv map[string]interface{}) string {
	url := fmt.Sprintf("%s%s?", host, op)
	for k, v := range kv {
		url += fmt.Sprintf("%s=%v&", k, v)
	}
	return url[:len(url)-1]
}

func checkWithDefault(kv map[string]interface{}, key string, val interface{}) {
	if kv[key] != nil {
		return
	}

	kv[key] = val
}

const testOwner = "cfs"

func createVol(kv map[string]interface{}, t *testing.T) {
	checkWithDefault(kv, volTypeKey, proto.VolumeTypeHot)
	checkWithDefault(kv, volOwnerKey, testOwner)
	checkWithDefault(kv, zoneNameKey, testZone2)
	checkWithDefault(kv, volCapacityKey, 300)
	checkWithDefault(kv, StoreModeKey, proto.StoreModeMem)

	switch kv[volTypeKey].(int) {
	case proto.VolumeTypeHot:
		checkWithDefault(kv, replicaNumKey, 3)
	case proto.VolumeTypeCold:
		checkWithDefault(kv, replicaNumKey, 1)
	default:
		// do nothing
	}

	processWithFatalV2(proto.AdminCreateVol, true, kv, t)

	vol, err := server.cluster.getVol(kv[nameKey].(string))
	if err != nil {
		t.Fatal(err)
		return
	}

	dpReplicaNum := kv[replicaNumKey].(int)
	assert.True(t, dpReplicaNum == int(vol.dpReplicaNum))

	checkDataPartitionsWritableTest(vol, t)
	checkMetaPartitionsWritableTest(vol, t)
}

func checkDataPartitionsWritableTest(vol *Vol, t *testing.T) {
	if len(vol.dataPartitions.partitions) == 0 {
		return
	}
	time.Sleep(time.Second * 20)
	partition := vol.dataPartitions.partitions[0]
	if partition.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
		return
	}

	// after check data partitions ,the status must be writable
	vol.checkDataPartitions(server.cluster)
	partition = vol.dataPartitions.partitions[0]
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

	for _, mp := range vol.MetaPartitions {
		if mp.Status != proto.ReadWrite {
			t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, mp.Status)
			return
		}
	}

	maxPartitionID := vol.maxMetaPartitionID()
	maxMp := vol.MetaPartitions[maxPartitionID]
	// after check meta partitions ,the status must be writable
	maxMp.checkStatus(server.cluster.Name, false, int(vol.mpReplicaNum), maxPartitionID, 4194304, vol.Forbidden, defaultMetaPartitionTimeOutSec)
	if maxMp.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, maxMp.Status)
		return
	}
}

func getSimpleVol(name string, success bool, t *testing.T) *proto.SimpleVolView {
	req := map[string]interface{}{
		nameKey: name,
	}

	reply := processWithFatalV2(proto.AdminGetVol, success, req, t)
	if !success {
		return nil
	}

	view := &proto.SimpleVolView{}
	err := json.Unmarshal([]byte(reply.Data), view)

	assert.True(t, err == nil)
	assert.True(t, view.RemoteCacheOnlyForNotSSD, "RemoteCacheOnlyForNotSSD must stay true (SDK/master default after flag removal)")

	return view
}

func getVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, proto.ClientVol, name, buildAuthKey("cfs"))
	process(reqURL, t)
}

func statVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v", hostAddr, proto.ClientVolStat, name)
	process(reqURL, t)
}

func TestVolMpsLock(t *testing.T) {
	name := "TestVolMpsLock"
	var volID uint64 = 1
	createTime := time.Now().Unix()

	vv := volValue{
		ID:                volID,
		Name:              name,
		Owner:             name,
		ZoneName:          "",
		DataPartitionSize: util.DefaultDataPartitionSize,
		Capacity:          100,
		DpReplicaNum:      defaultReplicaNum,
		ReplicaNum:        defaultReplicaNum,
		FollowerRead:      false,
		Authenticate:      false,
		CrossZone:         false,
		DefaultPriority:   false,
		CreateTime:        createTime,
		Description:       "",
	}
	expireTime := time.Microsecond * 50
	vol := newVol(vv)
	if vol.mpsLock.enable == 0 {
		return
	}
	vol.mpsLock.Lock()
	mpsLock := vol.mpsLock
	assert.True(t, !(mpsLock.vol.status() == proto.VolStatusMarkDelete || atomic.LoadInt32(&mpsLock.enable) == 0))

	assert.True(t, mpsLock.onLock == true)
	time.Sleep(time.Microsecond * 100)
	tm := time.Now()
	if tm.After(mpsLock.lockTime.Add(expireTime)) {
		log.LogWarnf("vol %v mpsLock hang more than %v since time %v stack(%v)",
			mpsLock.vol.Name, expireTime, mpsLock.lockTime, mpsLock.lastEffectStack)
		mpsLock.hang = true
	}

	assert.True(t, strings.Contains(vol.mpsLock.lastEffectStack, "Lock stack"))
	assert.True(t, vol.mpsLock.enable == 1)
	assert.True(t, vol.mpsLock.hang == true)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		vol.mpsLock.RLock()
		assert.True(t, strings.Contains(vol.mpsLock.lastEffectStack, "RLock stack"))
		vol.mpsLock.RUnlock()
		wg.Done()
	}()
	vol.mpsLock.UnLock()
	wg.Wait()
	assert.True(t, vol.mpsLock.hang == false)
	assert.True(t, strings.Contains(vol.mpsLock.lastEffectStack, "RUnlock stack"))
}

func TestConcurrentReadWriteDataPartitionMap(t *testing.T) {
	name := "TestConcurrentReadWriteDataPartitionMap"
	var volID uint64 = 1
	createTime := time.Now().Unix()

	vv := volValue{
		ID:                volID,
		Name:              name,
		Owner:             name,
		ZoneName:          "",
		DataPartitionSize: util.DefaultDataPartitionSize,
		Capacity:          100,
		DpReplicaNum:      defaultReplicaNum,
		ReplicaNum:        defaultReplicaNum,
		FollowerRead:      false,
		Authenticate:      false,
		CrossZone:         false,
		DefaultPriority:   false,
		CreateTime:        createTime,
		Description:       "",
	}

	vol := newVol(vv)
	// unavailable mp
	mp1 := newMetaPartition(1, 1, defaultMaxMetaPartitionInodeID, 3, name, volID, 0)
	vol.addMetaPartition(mp1)
	// readonly mp
	mp2 := newMetaPartition(2, 1, defaultMaxMetaPartitionInodeID, 3, name, volID, 0)
	mp2.Status = proto.ReadOnly
	vol.addMetaPartition(mp2)
	vol.updateViewCache(server.cluster)
	for id := 0; id < 30000; id++ {
		dp := newDataPartition(uint64(id), 3, name, volID, 0, defaultMediaType, defaultPoolId)
		vol.dataPartitions.put(dp)
	}
	go func() {
		var id uint64 = 30000
		for {
			id++
			dp := newDataPartition(id, 3, name, volID, 0, defaultMediaType, defaultPoolId)
			vol.dataPartitions.put(dp)
			time.Sleep(time.Second)
		}
	}()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		vol.updateViewCache(server.cluster)
	}
}

func TestVolSameCreateMode(t *testing.T) {
	vol1 := createTestVol("test-vol-1")
	vol2 := createTestVol("test-vol-2")
	vol3 := createTestVol("test-vol-3")

	t.Run("SameCreateMode", func(t *testing.T) {
		vol1.crossZone = true
		vol1.zoneName = "zone1"
		vol1.VolType = proto.VolumeTypeHot
		vol1.mpReplicaNum = 3
		vol1.dpReplicaNum = 3
		vol1.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}

		vol2.crossZone = true
		vol2.zoneName = "zone1"
		vol2.VolType = proto.VolumeTypeHot
		vol2.mpReplicaNum = 3
		vol2.dpReplicaNum = 3
		vol2.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}

		assert.True(t, vol1.sameCreateMode(vol2), "sameCreateMode should return true")
		assert.True(t, vol2.sameCreateMode(vol1), "sameCreateMode should return true (symmetric)")
	})

	t.Run("DifferentCreateMode", func(t *testing.T) {
		vol3.crossZone = false
		vol3.zoneName = "zone2"
		vol3.VolType = proto.VolumeTypeCold
		vol3.mpReplicaNum = 2
		vol3.dpReplicaNum = 2
		vol3.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}

		assert.False(t, vol1.sameCreateMode(vol3), "sameCreateMode should return false")
		assert.False(t, vol3.sameCreateMode(vol1), "sameCreateMode should return false (symmetric)")
	})

	t.Run("NilVolume", func(t *testing.T) {
		assert.False(t, vol1.sameCreateMode(nil), "sameCreateMode should return false")
	})

	t.Run("PartialDifference", func(t *testing.T) {
		vol2.crossZone = false
		assert.False(t, vol1.sameCreateMode(vol2), "crossZone different should return false")

		vol2.crossZone = true
		vol2.zoneName = "different-zone"
		assert.False(t, vol1.sameCreateMode(vol2), "zoneName different should return false")

		vol2.zoneName = "zone1"
		vol2.VolType = proto.VolumeTypeCold
		assert.False(t, vol1.sameCreateMode(vol2), "VolType different should return false")

		vol2.VolType = proto.VolumeTypeHot
		vol2.mpReplicaNum = 2
		assert.False(t, vol1.sameCreateMode(vol2), "mpReplicaNum different should return false")

		vol2.mpReplicaNum = 3
		vol2.dpReplicaNum = 2
		assert.False(t, vol1.sameCreateMode(vol2), "dpReplicaNum different should return false")
	})
}

func TestVolCompareStorageClasses(t *testing.T) {
	vol := createTestVol("test-vol")

	t.Run("SameStorageClasses", func(t *testing.T) {
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}
		other := []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}

		assert.True(t, vol.compareStorageClasses(other), "same storage classes should return true")
	})

	t.Run("DifferentStorageClasses", func(t *testing.T) {
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}
		other := []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_BlobStore}

		assert.False(t, vol.compareStorageClasses(other), "different storage classes should return false")
	})

	t.Run("DifferentLength", func(t *testing.T) {
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}
		other := []uint32{proto.StorageClass_Replica_HDD}

		assert.False(t, vol.compareStorageClasses(other), "different length of storage classes should return false")
	})

	t.Run("EmptySlices", func(t *testing.T) {
		vol.allowedStorageClass = []uint32{}
		other := []uint32{}

		assert.True(t, vol.compareStorageClasses(other), "two empty slices should return true")
	})

	t.Run("OneEmptyOneNotEmpty", func(t *testing.T) {
		vol.allowedStorageClass = []uint32{}
		other := []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}

		assert.False(t, vol.compareStorageClasses(other), "one empty one not empty should return false")
	})

	t.Run("SameElementsDifferentOrder", func(t *testing.T) {
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}
		other := []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}

		assert.True(t, vol.compareStorageClasses(other), "same elements but different order should return true")
	})

	t.Run("DuplicateElements", func(t *testing.T) {
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}
		other := []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}

		assert.True(t, vol.compareStorageClasses(other), "same slice with duplicate elements should return true")
	})

	t.Run("DifferentDuplicateCount", func(t *testing.T) {
		vol.allowedStorageClass = []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD, proto.StorageClass_BlobStore}
		other := []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD}

		assert.False(t, vol.compareStorageClasses(other), "different number of duplicate elements should return false")
	})
}

func createTestVol(name string) *Vol {
	vol := &Vol{
		Name:          name,
		ID:            1,
		Owner:         "test-owner",
		Status:        proto.VolStatusNormal,
		VolType:       proto.VolumeTypeHot,
		zoneName:      "test-zone",
		createTime:    time.Now().Unix(),
		description:   "test volume",
		TrashInterval: 0,

		dpReplicaNum:      3,
		mpReplicaNum:      3,
		dataPartitionSize: 120 * util.GB,
		Capacity:          100,
		dpRepairBlockSize: proto.DefaultDpRepairBlockSize,

		MetaPartitions: make(map[uint64]*MetaPartition),
		dataPartitions: newDataPartitionMap(name),

		NeedToLowerReplica:       false,
		FollowerRead:             false,
		MetaFollowerRead:         false,
		MetaNearRead:             false,
		DirectRead:               false,
		IgnoreTinyRecover:        false,
		MaximallyRead:            false,
		enableQuota:              false,
		DisableAuditLog:          false,
		DpReadOnlyWhenVolFull:    false,
		ReadOnlyForVolFull:       false,
		AccessTimeInterval:       0,
		EnablePersistAccessTime:  false,
		AccessTimeValidInterval:  0,
		LeaderRetryTimeout:       0,
		EnableAutoDpMetaRepair:   atomicutil.Bool{},
		EnableAutoMpMetaRepair:   atomicutil.Bool{},
		ForbidWriteOpOfProtoVer0: atomicutil.Bool{},

		allowedStorageClass:     []uint32{proto.StorageClass_Replica_HDD, proto.StorageClass_Replica_SSD},
		volStorageClass:         proto.StorageClass_Replica_HDD,
		StatByStorageClass:      []*proto.StatOfStorageClass{},
		StatMigrateStorageClass: []*proto.StatOfStorageClass{},
		StatByDpMediaType:       []*proto.StatOfStorageClass{},
		QuotaByClass:            []*proto.StatOfStorageClass{},
		DefaultStoreMode:        proto.StoreModeMem,

		mpsLock: newMpsLockManager(nil),
		volLock: sync.RWMutex{},
	}

	vol.VersionMgr = newVersionMgr(vol)
	vol.quotaManager = &MasterQuotaManager{
		MpQuotaInfoMap: make(map[uint64][]*proto.QuotaReportInfo),
		IdQuotaInfoMap: make(map[uint32]*proto.QuotaInfo),
		vol:            vol,
	}

	return vol
}
