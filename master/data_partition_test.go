package master

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/unit"
)

func TestDataPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkDataPartitions()
	count := 20
	commonVol.createTime = time.Now().Unix() - defaultAutoCreateDPAfterVolCreateSecond*2
	createDataPartition(commonVol, count, t)
	if !assert.Greater(t, len(commonVol.dataPartitions.partitions), 0, "getDataPartition no dp") {
		return
	}
	partition := commonVol.dataPartitions.partitions[0]
	partition.Lock()
	partition.isRecover = false
	partition.Unlock()
	getDataPartition(partition.PartitionID, t)
	loadDataPartitionTest(partition, t)
	decommissionDataPartition(partition, t)
	updateDataPartition(partition, true, server.cluster, commonVol, t)
	updateDataPartition(partition, false, server.cluster, commonVol, t)
	setDataPartitionIsRecover(partition, true, t)
	setDataPartitionIsRecover(partition, false, t)
	allDataNodes := make([]string, 0)
	server.cluster.dataNodes.Range(func(key, _ interface{}) bool {
		if addr, ok := key.(string); ok {
			allDataNodes = append(allDataNodes, addr)
		}
		return true
	})
	partition2 := commonVol.dataPartitions.partitions[1]
	partition2.Lock()
	partition2.isRecover = false
	partition2.Unlock()
	decommissionDataPartitionToDestAddr(partition2, allDataNodes, t)
	delDataReplicaTest(partition2, t)
}

func createDataPartition(vol *Vol, count int, t *testing.T) {
	oldCount := len(vol.dataPartitions.partitions)
	reqURL := fmt.Sprintf("%v%v?count=%v&name=%v&type=extent",
		hostAddr, proto.AdminCreateDataPartition, count, vol.Name)
	process(reqURL, t)
	newCount := len(vol.dataPartitions.partitions)
	total := oldCount + count
	if !assert.Equalf(t, total, newCount, "createDataPartition failed,newCount[%v],total=%v,count[%v],oldCount[%v]", newCount, total, count, oldCount) {
		return
	}
}

func getDataPartition(id uint64, t *testing.T) {

	reqURL := fmt.Sprintf("%v%v?id=%v",
		hostAddr, proto.AdminGetDataPartition, id)
	process(reqURL, t)
}

func decommissionDataPartition(dp *DataPartition, t *testing.T) {
	offlineAddr := dp.Hosts[0]
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v&force=true",
		hostAddr, proto.AdminDecommissionDataPartition, dp.VolName, dp.PartitionID, offlineAddr)
	process(reqURL, t)
	if !assert.NotContainsf(t, dp.Hosts, offlineAddr, "decommissionDataPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, dp.Hosts) {
		return
	}
	dp.isRecover = false
}

func decommissionDataPartitionToDestAddr(dp *DataPartition, allDataNodes []string, t *testing.T) {
	var destAddr string
	for _, addr := range allDataNodes {
		if !contains(dp.Hosts, addr) {
			destAddr = addr
			break
		}
	}
	dp.isRecover = false
	offlineAddr := dp.Hosts[0]
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v&destAddr=%v&force=true",
		hostAddr, proto.AdminDecommissionDataPartition, dp.VolName, dp.PartitionID, offlineAddr, destAddr)
	process(reqURL, t)
	if !assert.NotContainsf(t, dp.Hosts, offlineAddr, "decommissionDataPartitionToDestAddr failed,offlineAddr[%v],destAddr[%v],hosts[%v]", offlineAddr, destAddr, dp.Hosts) ||
		!assert.Containsf(t, dp.Hosts, destAddr, "decommissionDataPartitionToDestAddr failed,offlineAddr[%v],destAddr[%v],hosts[%v]", offlineAddr, destAddr, dp.Hosts) {
		return
	}
	dp.isRecover = false
}

func loadDataPartitionTest(dp *DataPartition, t *testing.T) {
	dps := make([]*DataPartition, 0)
	dps = append(dps, dp)
	server.cluster.waitForResponseToLoadDataPartition(dps)
	time.Sleep(5 * time.Second)
	dp.RLock()
	tinyFile := &FileInCore{}
	tinyFile.Name = "50000011"
	tinyFile.LastModify = 1562507765
	extentFile := &FileInCore{}
	extentFile.Name = "10"
	extentFile.LastModify = 1562507765
	for index, host := range dp.Hosts {
		fm := newFileMetadata(uint32(404551221)+uint32(index), host, index, 2*unit.MB)
		tinyFile.MetadataArray = append(tinyFile.MetadataArray, fm)
		extentFile.MetadataArray = append(extentFile.MetadataArray, fm)
	}

	dp.FileInCoreMap[tinyFile.Name] = tinyFile
	dp.FileInCoreMap[extentFile.Name] = extentFile
	dp.RUnlock()
	dp.getFileCount()
	dp.validateCRC(server.cluster.Name)
	dp.setToNormal()
}
func delDataReplicaTest(dp *DataPartition, t *testing.T) {
	testAddr := mds9Addr
	extraReplica := proto.DataReplica{
		Status: 2,
		Addr:   testAddr,
	}
	addDataServer(testAddr, testZone1)
	dn, _ := server.cluster.dataNode(testAddr)
	extraDataReplica := &DataReplica{
		DataReplica: extraReplica,
		dataNode:    dn,
	}
	dp.Replicas = append(dp.Replicas, extraDataReplica)
	err := server.cluster.deleteDataReplica(dp, dn, false)
	assert.NoErrorf(t, err, "delete replica failed, err[%v]", err)
	server.cluster.checkDataPartitions()
	assert.Equalf(t, 3, len(dp.Replicas), "delete replica failed, expect replica length[%v], but is[%v]", 3, len(dp.Replicas))
	for _, r := range dp.Replicas {
		if !assert.NotEqualf(t, r.Addr, testAddr, "delete replica [%v] failed", testAddr) {
			return
		}
	}
}

func updateDataPartition(dp *DataPartition, isManual bool, c *Cluster, vol *Vol, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v&isManual=%v",
		hostAddr, proto.AdminDataPartitionUpdate, dp.VolName, dp.PartitionID, isManual)
	process(reqURL, t)
	if !assert.Equalf(t, isManual, dp.IsManual, "expect isManual[%v],dp.IsManual[%v],not equal", isManual, dp.IsManual) {
		return
	}
	dp.isRecover = false
	dp.checkStatus(c.Name, true, c.cfg.DataPartitionTimeOutSec, 0, vol.CrossRegionHAType, c, vol.getDataPartitionQuorum())
	if dp.IsManual {
		if !assert.Equalf(t, proto.ReadOnly, int(dp.Status), "dp.IsManual[%v] expect Status ReadOnly, but get Status[%v]", dp.IsManual, dp.Status) {
			return
		}
	}
}

func setDataPartitionIsRecover(dp *DataPartition, isRecover bool, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?id=%v&isRecover=%v",
		hostAddr, proto.AdminDataPartitionSetIsRecover, dp.PartitionID, isRecover)
	process(reqURL, t)
	if !assert.Equalf(t, isRecover, dp.isRecover, "expect isRecover[%v],dp.isRecover[%v],not equal", isRecover, dp.isRecover) {
		return
	}
}
