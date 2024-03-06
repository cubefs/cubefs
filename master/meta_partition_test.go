package master

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

func TestMetaPartition(t *testing.T) {
	_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "meta-partition-test")
	server.cluster.checkDataNodeHeartbeat(ctx)
	server.cluster.checkMetaNodeHeartbeat(ctx)
	time.Sleep(5 * time.Second)
	server.cluster.checkMetaPartitions(ctx)
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	createMetaPartition(ctx, commonVol, t)
	maxPartitionID := commonVol.maxPartitionID()
	getMetaPartition(commonVol.Name, maxPartitionID, t)
	loadMetaPartitionTest(commonVol, maxPartitionID, t)
	server.cluster.checkMetaNodeHeartbeat(ctx)
	time.Sleep(5 * time.Second)
	decommissionMetaPartition(ctx, commonVol, maxPartitionID, t)
}

func createMetaPartition(ctx context.Context, vol *Vol, t *testing.T) {
	count := 3
	vol.mpsLock.RLock()
	oldPartitionCount := len(vol.MetaPartitions)
	vol.mpsLock.RUnlock()

	reqURL := fmt.Sprintf("%v%v?name=%v&count=%v",
		hostAddr, proto.AdminCreateMetaPartition, vol.Name, count)
	process(reqURL, t)

	vol, err := server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}

	vol.mpsLock.RLock()
	newPartitionCount := len(vol.MetaPartitions)
	newMaxPartitionID := vol.maxPartitionID()
	newMaxMetaPartition, err := vol.metaPartition(newMaxPartitionID)
	if err != nil {
		vol.mpsLock.RUnlock()
		t.Errorf("createMetaPartition,err [%v]", err)
		return
	}

	assert.Equal(t, oldPartitionCount+count, newPartitionCount)

	if defaultMaxMetaPartitionInodeID != newMaxMetaPartition.End {
		t.Errorf("createMetaPartition,err expected MaxMetaPartitionEnd [%v] , actual MaxMetaPartitionEnd [%v]", defaultMaxMetaPartitionInodeID, newMaxMetaPartition.End)
	}
	vol.mpsLock.RUnlock()

	server.cluster.checkMetaNodeHeartbeat(ctx)
}

func getMetaPartition(volName string, id uint64, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v",
		hostAddr, proto.ClientMetaPartition, volName, id)
	process(reqURL, t)
}

func loadMetaPartitionTest(vol *Vol, id uint64, t *testing.T) {
	reqURL := fmt.Sprintf("%v%v?name=%v&id=%v", hostAddr, proto.AdminLoadMetaPartition, vol.Name, id)
	process(reqURL, t)
}

func decommissionMetaPartition(ctx context.Context, vol *Vol, id uint64, t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat(ctx)
	time.Sleep(5 * time.Second)
	reqURL := fmt.Sprintf("%v%v", hostAddr, proto.AdminGetCluster)
	process(reqURL, t)
	vol, err := server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}
	mp, err := vol.metaPartition(id)
	if err != nil {
		t.Errorf("decommissionMetaPartition,err [%v]", err)
		return
	}
	offlineAddr := mp.Hosts[0]
	reqURL = fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, proto.AdminDecommissionMetaPartition, vol.Name, id, offlineAddr)
	process(reqURL, t)
	mp, err = server.cluster.getMetaPartitionByID(id)
	if err != nil {
		t.Errorf("decommissionMetaPartition,err [%v]", err)
		return
	}
	if contains(mp.Hosts, offlineAddr) {
		t.Errorf("decommissionMetaPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, mp.Hosts)
		return
	}
}
