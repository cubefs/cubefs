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
	commonVol.dataPartitions.readableAndWritableCnt = 0
	commonVol.dataPartitions.lastAutoCreateTime = time.Now().Add(-time.Minute)
	server.cluster.DisableAutoAllocate = false
	t.Logf("status[%v],disableAutoAlloc[%v],cap[%v]\n",
		commonVol.Status, server.cluster.DisableAutoAllocate, commonVol.Capacity)

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
	checkCreateVolParam(cacheCapacity, req, 102, 0, t)
	// zoneName must equal to testZone if no default zone
	checkCreateVolParam(zoneNameKey, req, "default", testZone2, t)

	processWithFatalV2(proto.AdminCreateVol, true, req, t)

	// check default val of normal vol
	vol, err := server.cluster.getVol(volName1)
	require.NoError(t, err)
	require.EqualValues(t, 120*util.GB, vol.dataPartitionSize)
	require.EqualValues(t, defaultInitMetaPartitionCount, len(vol.MetaPartitions))
	require.False(t, vol.FollowerRead)
	require.False(t, vol.authenticate)
	require.False(t, vol.crossZone)
	require.EqualValues(t, 100, vol.capacity())
	require.EqualValues(t, proto.VolumeTypeHot, vol.VolType)
	require.EqualValues(t, defaultReplicaNum, vol.dpReplicaNum)
	require.EqualValues(t, 0, vol.domainId)

	delVol(volName1, t)
	// time.Sleep(30 * time.Second)

	req[nameKey] = volName2
	req[volStorageClassKey] = proto.StorageClass_BlobStore

	processWithFatalV2(proto.AdminCreateVol, true, req, t)

	// check default val of LF vol
	vol, err = server.cluster.getVol(volName2)
	require.NoError(t, err)
	require.EqualValues(t, "", vol.CacheRule)
	require.EqualValues(t, defaultEbsBlkSize, vol.EbsBlkSize)
	require.EqualValues(t, 0, vol.CacheCapacity)
	require.EqualValues(t, proto.NoCache, vol.CacheAction)
	require.EqualValues(t, vol.CacheThreshold, defaultCacheThreshold)
	require.EqualValues(t, 0, vol.dpReplicaNum)
	require.True(t, vol.FollowerRead)
	require.EqualValues(t, defaultCacheTtl, vol.CacheTTL)
	require.EqualValues(t, defaultCacheHighWater, vol.CacheHighWater)
	require.EqualValues(t, defaultCacheLowWater, vol.CacheLowWater)
	require.EqualValues(t, defaultCacheLruInterval, vol.CacheLRUInterval)

	delVol(volName2, t)

	req[nameKey] = volName3
	req[cacheRuleKey] = "cacheRule"

	blkSize := 7 * 1024 * 1024
	cacheCap := 10
	threshold := 10 * 1024 * 24
	ttl := 10
	high := 77
	low := 40
	lru := 7

	// check with illegal args
	checkCreateVolParam(ebsBlkSizeKey, req, -1, blkSize, t)
	checkCreateVolParam(cacheCapacity, req, -1, cacheCap, t)
	checkCreateVolParam(cacheActionKey, req, "3", proto.NoCache, t)
	checkCreateVolParam(cacheThresholdKey, req, -1, threshold, t)
	checkCreateVolParam(cacheTTLKey, req, "ttl", ttl, t)
	checkCreateVolParam(cacheHighWaterKey, req, -1, high, t)
	checkCreateVolParam(cacheHighWaterKey, req, 92, high, t)
	checkCreateVolParam(cacheLowWaterKey, req, 80, low, t)
	checkCreateVolParam(cacheLRUIntervalKey, req, -1, lru, t)
	checkCreateVolParam(followerReadKey, req, -1, true, t)

	processWithFatalV2(proto.AdminCreateVol, true, req, t)

	view := getSimpleVol(volName3, true, t)
	assert.True(t, view.ObjBlockSize == blkSize)
	assert.True(t, view.CacheCapacity == uint64(cacheCap))
	assert.True(t, view.CacheThreshold == threshold)
	assert.True(t, view.CacheAction == proto.NoCache)
	assert.True(t, view.CacheTtl == ttl)
	assert.True(t, view.CacheHighWater == high)
	assert.True(t, view.CacheLowWater == low)
	assert.True(t, view.CacheLruInterval == lru)

	delVol(volName3, t)

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

	switch kv[volTypeKey].(int) {
	case proto.VolumeTypeHot:
		checkWithDefault(kv, replicaNumKey, 3)
		break
	case proto.VolumeTypeCold:
		checkWithDefault(kv, cacheCapacity, 80)
		checkWithDefault(kv, replicaNumKey, 1)
		break
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

	maxPartitionID := vol.maxPartitionID()
	maxMp := vol.MetaPartitions[maxPartitionID]
	// after check meta partitions ,the status must be writable
	maxMp.checkStatus(server.cluster.Name, false, int(vol.mpReplicaNum), maxPartitionID, 4194304, vol.Forbidden)
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
		dp := newDataPartition(uint64(id), 3, name, volID, 0, 0)
		vol.dataPartitions.put(dp)
	}
	go func() {
		var id uint64 = 30000
		for {
			id++
			dp := newDataPartition(id, 3, name, volID, 0, 0)
			vol.dataPartitions.put(dp)
			time.Sleep(time.Second)
		}
	}()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		vol.updateViewCache(server.cluster)
	}
}
