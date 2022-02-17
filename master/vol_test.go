package master

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

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
	fmt.Printf("writable data partitions[%v]\n", commonVol.dataPartitions.readableAndWritableCnt)
}

func TestVol(t *testing.T) {
	name := "test1"
	createVol(map[string]interface{}{nameKey: name}, t)
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
	statVol(name, t)
	delVol(name, t)
	getSimpleVol(name, true, t)
	vol.checkStatus(server.cluster)
	err = vol.deleteVolFromStore(server.cluster)
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
	assert.True(t, vol.dpReplicaNum == 1)
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

	fmt.Println(url)

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
	checkWithDefault(kv, volCapacityKey, 100)

	switch kv[volTypeKey].(int) {
	case proto.VolumeTypeHot:
		checkWithDefault(kv, replicaNumKey, 3)
		break
	case proto.VolumeTypeCold:
		checkWithDefault(kv, cacheCapacity, 80)
		checkWithDefault(kv, replicaNumKey, 1)
		break
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

	partition := vol.dataPartitions.partitions[0]
	if partition.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
		return
	}

	//after check data partitions ,the status must be writable
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
	//after check meta partitions ,the status must be writable
	maxMp.checkStatus(server.cluster.Name, false, int(vol.mpReplicaNum), maxPartitionID)
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
	fmt.Println(reqURL)
	process(reqURL, t)
}

func statVol(name string, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v",
		hostAddr, proto.ClientVolStat, name)
	fmt.Println(reqURL)
	process(reqURL, t)
}

func TestConcurrentReadWriteDataPartitionMap(t *testing.T) {
	name := "TestConcurrentReadWriteDataPartitionMap"
	var volID uint64 = 1
	var createTime = time.Now().Unix()

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
	mp1 := newMetaPartition(1, 1, defaultMaxMetaPartitionInodeID, 3, name, volID)
	vol.addMetaPartition(mp1)
	//readonly mp
	mp2 := newMetaPartition(2, 1, defaultMaxMetaPartitionInodeID, 3, name, volID)
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
