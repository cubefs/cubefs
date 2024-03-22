package master

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/log"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	_, ctx := proto.SpanContextPrefix("vol-test-auto-create-data-partition-")
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

	commonVol.checkAutoDataPartitionCreation(ctx, server.cluster)
	newDpCount := len(commonVol.dataPartitions.partitions)
	if dpCount == newDpCount {
		t.Errorf("autoCreateDataPartitions failed,expand 0 data partitions,oldCount[%v],curCount[%v]", dpCount, newDpCount)
		return
	}
}

func TestCheckVol(t *testing.T) {
	_, ctx := proto.SpanContextPrefix("vol-test-check-vol-")
	commonVol.checkStatus(ctx, server.cluster)
	commonVol.checkMetaPartitions(ctx, server.cluster)
	commonVol.checkDataPartitions(ctx, server.cluster)
	// log.Flush()
	t.Logf("writable data partitions[%v]\n", commonVol.dataPartitions.readableAndWritableCnt)
}

func TestVol(t *testing.T) {
	_, ctx := proto.SpanContextPrefix("vol-test-")
	name := "test1"
	createVol(ctx, map[string]interface{}{nameKey: name}, t)
	// report mp/dp info to master
	server.cluster.checkDataNodeHeartbeat(ctx)
	server.cluster.checkDataNodeHeartbeat(ctx)
	time.Sleep(5 * time.Second)
	// check status
	server.cluster.checkMetaPartitions(ctx)
	server.cluster.checkDataPartitions(ctx)
	server.cluster.checkLoadMetaPartitions(ctx)
	server.cluster.doLoadDataPartitions(ctx)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}

	vol.checkStatus(ctx, server.cluster)
	getVol(name, t)
	statVol(name, t)
	delVol(name, t)
	getSimpleVol(name, true, t)
	vol.checkStatus(ctx, server.cluster)
	err = vol.deleteVolFromStore(ctx, server.cluster)
	if err != nil {
		panic(err)
	}
}

func TestCreateColdVol(t *testing.T) {
	volName := "coldVol"

	req := map[string]interface{}{}
	// name can't be empty
	checkCreateVolParam(nameKey, req, "", volName, t)
	// name regex is illegal
	checkCreateVolParam(nameKey, req, "_vol", volName, t)
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
	vol, err := server.cluster.getVol(volName)
	assert.Nil(t, err)
	assert.True(t, vol.dataPartitionSize == 120*util.GB)
	assert.True(t, len(vol.MetaPartitions) == defaultInitMetaPartitionCount)
	assert.False(t, vol.FollowerRead)
	assert.False(t, vol.authenticate)
	assert.False(t, vol.crossZone)
	assert.True(t, vol.capacity() == 100)
	assert.True(t, vol.VolType == proto.VolumeTypeHot)
	assert.True(t, vol.dpReplicaNum == defaultReplicaNum)
	assert.True(t, vol.domainId == 0)

	delVol(volName, t)
	time.Sleep(11 * time.Second)

	req[volTypeKey] = proto.VolumeTypeCold

	processWithFatalV2(proto.AdminCreateVol, true, req, t)

	// check default val of LF vol
	vol, err = server.cluster.getVol(volName)
	assert.Nil(t, err)
	assert.True(t, vol.CacheRule == "")
	assert.True(t, vol.EbsBlkSize == defaultEbsBlkSize)
	assert.True(t, vol.CacheCapacity == 0)
	assert.True(t, vol.CacheAction == proto.NoCache)
	assert.True(t, vol.CacheThreshold == defaultCacheThreshold)
	assert.True(t, vol.dpReplicaNum == 0)
	assert.True(t, vol.FollowerRead)
	assert.True(t, vol.CacheTTL == defaultCacheTtl)
	assert.True(t, vol.CacheHighWater == defaultCacheHighWater)
	assert.True(t, vol.CacheLowWater == defaultCacheLowWater)
	assert.True(t, vol.CacheLRUInterval == defaultCacheLruInterval)

	delVol(volName, t)

	volName = "coldVol2"
	req[nameKey] = volName
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

	view := getSimpleVol(volName, true, t)
	assert.True(t, view.ObjBlockSize == blkSize)
	assert.True(t, view.CacheCapacity == uint64(cacheCap))
	assert.True(t, view.CacheThreshold == threshold)
	assert.True(t, view.CacheAction == proto.NoCache)
	assert.True(t, view.CacheTtl == ttl)
	assert.True(t, view.CacheHighWater == high)
	assert.True(t, view.CacheLowWater == low)
	assert.True(t, view.CacheLruInterval == lru)

	delVol(volName, t)
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

func createVol(ctx context.Context, kv map[string]interface{}, t *testing.T) {
	checkWithDefault(kv, volTypeKey, proto.VolumeTypeHot)
	checkWithDefault(kv, volOwnerKey, testOwner)
	checkWithDefault(kv, zoneNameKey, testZone2)
	checkWithDefault(kv, volCapacityKey, 300)

	switch kv[volTypeKey].(int) {
	case proto.VolumeTypeHot:
		checkWithDefault(kv, replicaNumKey, 3)
	case proto.VolumeTypeCold:
		checkWithDefault(kv, cacheCapacity, 80)
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

	checkDataPartitionsWritableTest(ctx, vol, t)
	checkMetaPartitionsWritableTest(ctx, vol, t)
}

func checkDataPartitionsWritableTest(ctx context.Context, vol *Vol, t *testing.T) {
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
	vol.checkDataPartitions(ctx, server.cluster)
	partition = vol.dataPartitions.partitions[0]
	if partition.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
		return
	}
}

func checkMetaPartitionsWritableTest(ctx context.Context, vol *Vol, t *testing.T) {
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
	maxMp.checkStatus(ctx, server.cluster.Name, false, int(vol.mpReplicaNum), maxPartitionID, 4194304, vol.Forbidden)
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
	_, ctx := proto.SpanContextPrefix("vol-test-mps-lock-")
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
	vol := newVol(ctx, vv)
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
		log.Warnf("vol %v mpsLock hang more than %v since time %v stack(%v)",
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
	_, ctx := proto.SpanContextPrefix("vol-test-concurrent-rw-dp-map-")
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

	vol := newVol(ctx, vv)
	// unavailable mp
	mp1 := newMetaPartition(1, 1, defaultMaxMetaPartitionInodeID, 3, name, volID, 0)
	vol.addMetaPartition(ctx, mp1)
	// readonly mp
	mp2 := newMetaPartition(2, 1, defaultMaxMetaPartitionInodeID, 3, name, volID, 0)
	mp2.Status = proto.ReadOnly
	vol.addMetaPartition(ctx, mp2)
	vol.updateViewCache(ctx, server.cluster)
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
		vol.updateViewCache(ctx, server.cluster)
	}
}
