package ecnode

import (
	"fmt"
	"github.com/cubefs/cubefs/ecstorage"
	"github.com/cubefs/cubefs/proto"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func TestEcPartition_scrubPartitionData(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll(fakeNode.path)
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	fakeNode.EcNode.maxScrubExtents = 3
	if err := ep.scrubPartitionData(&fakeNode.EcNode); err != nil {
		t.Fatalf("scrub partitionData err[%v]", err)
	}

}

func TestEcPartition_repairPartitionData(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll(fakeNode.path)
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	AdjustEcHostsForTest(ep)
	ep.repairPartitionData(true)
}

func TestEcPartition_notifyRepairTinyDelInfo(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer os.RemoveAll(fakeNode.path)
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	handleEcBatchTinyExtentDelete(t, ep)
	var needRepairTinyDel repairTinyDelInfo
	needRepairTinyDel.needRepairHosts = append(needRepairTinyDel.needRepairHosts, ep.Hosts[0])
	needRepairTinyDel.maxTinyDelCount, _ = ep.extentStore.GetEcTinyDeleteCount()
	needRepairTinyDel.normalHost = ep.Hosts[0]
	err := ep.notifyRepairTinyDelInfo(&needRepairTinyDel, false)
	if err != nil {
		t.Fatal(err)
	}
	err = ep.repairTinyDelete(needRepairTinyDel.normalHost, needRepairTinyDel.maxTinyDelCount)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEcPartition_repairExtentData(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	var repairInfo repairExtentInfo
	originExtentSize := ep.EcMaxUnitSize * uint64(ep.EcDataNum)
	ep.fillRepairExtentInfo(ep.EcMaxUnitSize, 0, ep.EcMaxUnitSize, originExtentSize, ep.Hosts[2], &repairInfo)
	err := ep.repairExtentData(fakeNode.fakeNormalExtentId, &repairInfo, proto.DataRepair)
	errStr := fmt.Sprintf("%v", err)
	if !strings.Contains(errStr, "no enough data to reconstruct") {
		t.Fatalf("err[%v]", err)
	}

}

func TestEcPartition_writeBackReconstructData(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	ecNodeNum := ep.EcDataNum + ep.EcParityNum
	needRepairDataBitmap := make([]byte, ecNodeNum)
	needRepairDataBitmap[0] = 1
	data := make([][]byte, ecNodeNum)
	size := int(ep.EcMaxUnitSize)
	originExtentSize := ep.EcMaxUnitSize * uint64(ep.EcDataNum)
	data[0] = make([]byte, size)
	for i := 1; i < len(ep.Hosts); i++ {
		ep.Hosts[i] = ep.Hosts[0]
	}
	ecStripe, _ := NewEcStripe(ep, ep.EcMaxUnitSize, fakeNode.fakeNormalExtentId)
	var repairInfo repairExtentInfo
	ep.fillRepairExtentInfo(ep.EcMaxUnitSize, 0, ep.EcMaxUnitSize, originExtentSize, ep.Hosts[0], &repairInfo)
	err := ecStripe.writeBackReconstructData(data, 0, &repairInfo, proto.DataRepair)
	if err != nil {
		t.Fatalf("writeBack err[%v]", err)
	}
}

func TestEcPartition_statusUpdate(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	ep.statusUpdate()
	if ep.partitionStatus != proto.ReadWrite {
		t.Fatalf("update err partition status [%v]", ep.partitionStatus)
	}
}

func TestEcPartition_evictFileDescriptor(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(true, true, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	ep.disk.evictExpiredFileDescriptor()
	atomic.AddInt64(&ep.disk.fdCount, DiskMaxFDLimit+1)
	ep.disk.forceEvictFileDescriptor()
	//no result check, just check no panic
}

func TestEcPartition_compareEcReplicas(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	hosts1 := []string{
		"127.0.0.1:11310",
		"127.0.0.1:11311",
		"127.0.0.1:11312",
		"127.0.0.1:11313",
		"127.0.0.1:11314",
		"127.0.0.1:11315",
	}
	hosts2 := []string{
		"127.0.0.1:11310",
		"127.0.0.1:11311",
		"127.0.0.1:11312",
		"127.0.0.1:11313",
		"127.0.0.1:11314",
		"127.0.0.1:11316",
	}
	if isTrue := ep.compareEcReplicas(hosts1, hosts2); isTrue {
		t.Fatalf("compare ecReplicas err")
	}

}
func fakeExtentInfo(i uint64) (nodeExtentsInfoMap map[uint64]*ecstorage.ExtentInfo) {
	nodeExtentsInfoMap = make(map[uint64]*ecstorage.ExtentInfo, 0)
	var extentInfo ecstorage.ExtentInfo
	extentInfo.OriginExtentSize = i
	extentInfo.FileID = fakeNode.fakeNormalExtentId
	nodeExtentsInfoMap[fakeNode.fakeNormalExtentId] = &extentInfo
	return
}

func TestEcPartition_getMaxExtentsInfo(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	nodeNum := ep.EcDataNum + ep.EcParityNum
	extentsInfoNodeList := make([]map[uint64]*ecstorage.ExtentInfo, nodeNum)
	i := uint64(0)
	for ; i < uint64(nodeNum); i++ {
		extentsInfoNodeList[i] = fakeExtentInfo(i)
	}
	maxExtentInfo := ep.getMaxExtentsInfo(extentsInfoNodeList)
	if maxExtentInfo[fakeNode.fakeNormalExtentId].OriginExtentSize != (i - 1) {
		t.Fatalf("getMaxExtentsInfo err")
	}
}

func TestEcPartition_fillNeedRepairExtents(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	needRepairExtents := make(map[uint64]*repairExtentInfo, 0)
	originExtentSize := ep.EcMaxUnitSize * uint64(ep.EcDataNum)
	extentInfo := &ecstorage.ExtentInfo{
		FileID: fakeNode.fakeNormalExtentId,
		Size:   ep.EcMaxUnitSize,
	}
	ep.fillNeedRepairExtents(extentInfo, ep.EcMaxUnitSize, originExtentSize, ep.Hosts[1], needRepairExtents)
	ep.fillNeedRepairExtents(extentInfo, ep.EcMaxUnitSize, originExtentSize, ep.Hosts[2], needRepairExtents)
	if len(needRepairExtents) == 0 {
		t.Fatalf("fillNeedRepairExtents err")
	}
}

func TestEcPartition_LoadEcPartition(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	newEp, err := LoadEcPartition(ep.path, ep.disk)
	if err != nil {
		t.Fatalf("loadPartition err[%v]", err)
	}

	if newEp.PartitionID != partitionIdNum {
		t.Fatalf("loadPartition partitionId err")
	}
}

func TestEcPartition_getTinyExtentOffset(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)

	holes := []*proto.TinyExtentHole{}
	offset := uint64(4096)
	ep.originTinyExtentDeleteMap[fakeNode.fakeTinyExtentId] = holes
	newOffset := ep.getTinyExtentOffset(fakeNode.fakeTinyExtentId, offset)
	if newOffset != 4096 {
		t.Fatalf("getTinyExtentOffset err newOffset(%v) ", newOffset)
	}

	holes = []*proto.TinyExtentHole{
		{Offset: 1, Size: 1, PreAllSize: 1},
		{Offset: 3, Size: 1, PreAllSize: 1},
	}
	ep.originTinyExtentDeleteMap[fakeNode.fakeTinyExtentId] = holes
	newOffset = ep.getTinyExtentOffset(fakeNode.fakeTinyExtentId, 2)
	if newOffset != 1 {
		t.Fatalf("getTinyExtentOffset err newOffset(%v)", newOffset)
	}

}

func TestEcPartition_ecCheckScrub(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	ep.ecMigrateStatus = proto.FinishEC
	for _, disk := range fakeNode.space.disks {
		disk.ecCheckScrub()
	}
}

func TestEcPartition_getExtentsFromRemoteNode(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)

	if err := ep.remoteCreateExtent(fakeNode.fakeNormalExtentId, 1024, ep.Hosts[0]); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ep.getExtentsFromRemoteNode(ep.Hosts[0], &fakeNode.EcNode); err != nil {
		t.Fatal(err)
	}

}

func TestEcPartition_repairCreateExtent(t *testing.T) {
	if err := FakeNodePrepare(t); err != nil {
		t.Fatal(err)
		return
	}
	defer func() {
		os.RemoveAll(fakeNode.path)
	}()
	partitionIdNum++
	ep := PrepareEcPartition(false, false, t, partitionIdNum)
	if ep == nil {
		t.Fatalf("prepare ec partition failed")
		return
	}
	defer fakeNode.fakeDeleteECPartition(t, partitionIdNum)
	createInfo := &createExtentInfo{
		hosts:            ep.Hosts,
		originExtentSize: 0,
	}
	if err := ep.repairCreateExtent(fakeNode.fakeNormalExtentId, createInfo); err != nil {
		t.Fatal(err)
	}

}
