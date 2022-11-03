package datanode

import (
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/datanode/mock"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/statistics"
	"hash/crc32"
	"os"
	"path"
	"sync"
	"testing"
	"time"
)

const (
	tcpPort  = 17019
	pageSize = 4096
)

func TestGetLocalExtentInfo(t *testing.T) {
	var (
		err         error
		dp          *DataPartition
		count       uint64 = 100
		testBaseDir        = path.Join(os.TempDir(), t.Name())
	)
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, count, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()

	tinyExtents := []uint64{1, 2, 3, 10}
	var extents []storage.ExtentInfoBlock
	var leaderTinyDeleteRecordFileSize int64
	if extents, leaderTinyDeleteRecordFileSize, err = dp.getLocalExtentInfo(proto.TinyExtentType, tinyExtents); err != nil {
		t.Fatalf("get local extent info, extentType:%v err:%v", proto.TinyExtentType, err)
	}
	if len(extents) != len(tinyExtents) {
		t.Fatalf("tiny extent count expect:%v, actual:%v", len(tinyExtents), len(extents))
	}
	if leaderTinyDeleteRecordFileSize != int64(24) {
		t.Fatalf("leaderTinyDeleteRecordFileSize expect:%v, actual:%v", 24, leaderTinyDeleteRecordFileSize)
	}
	if extents, leaderTinyDeleteRecordFileSize, err = dp.getLocalExtentInfo(proto.NormalExtentType, tinyExtents); err != nil {
		t.Fatalf("get local extent info, extentType:%v err:%v", proto.TinyExtentType, err)
	}
	if len(extents) != int(count) {
		t.Fatalf("tiny extent count expect:%v, actual:%v", count, len(extents))
	}

	if dp, err = initDataPartition(testBaseDir, 2, false); err != nil {
		t.Fatalf("init data partition err:%v", err)
	}
	if _, _, err = dp.getLocalExtentInfo(proto.NormalExtentType, tinyExtents); err == nil {
		t.Fatalf("get local extent info, extentType:%v err equal nil", proto.TinyExtentType)
	}
}

func TestGetRemoteExtentInfo(t *testing.T) {
	ctx := context.Background()
	tcp := mock.NewMockTcp(tcpPort)
	err := tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	// use v2
	mock.ReplyGetAllWatermarksV2Err = false
	dp := &DataPartition{
		partitionID: 10,
	}
	tinyExtents := []uint64{1, 2, 3, 10}
	targetHost := fmt.Sprintf(":%v", tcpPort)
	extentFiles, err := dp.getRemoteExtentInfo(ctx, proto.TinyExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v2 type:%v failed:%v", proto.TinyExtentType, err)
	}
	if len(extentFiles) != len(tinyExtents) {
		t.Fatalf("get remote extent info by v2 type:%v tiny extent count expect:%v actual:%v", proto.TinyExtentType, len(tinyExtents), len(extentFiles))
	}
	extentFiles, err = dp.getRemoteExtentInfo(ctx, proto.NormalExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v2 type:%v failed:%v", proto.NormalExtentType, err)
	}
	if len(extentFiles) != mock.RemoteNormalExtentCount {
		t.Fatalf("get remote extent info by v2 type:%v normal extent count expect:%v actual:%v", proto.NormalExtentType, mock.RemoteNormalExtentCount, len(extentFiles))
	}
	// use v1
	mock.ReplyGetAllWatermarksV2Err = true
	extentFiles, err = dp.getRemoteExtentInfo(ctx, proto.TinyExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v1 type:%v failed:%v", proto.TinyExtentType, err)
	}
	if len(extentFiles) != len(tinyExtents) {
		t.Fatalf("get remote extent info by v1 type:%v tiny extent count expect:%v actual:%v", proto.TinyExtentType, len(tinyExtents), len(extentFiles))
	}
	extentFiles, err = dp.getRemoteExtentInfo(ctx, proto.NormalExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v1 type:%v failed:%v", proto.NormalExtentType, err)
	}
	if len(extentFiles) != mock.RemoteNormalExtentCount {
		t.Fatalf("get remote extent info by v1 type:%v normal extent count expect:%v actual:%v", proto.NormalExtentType, mock.RemoteNormalExtentCount, len(extentFiles))
	}
}

func TestBuildDataPartitionRepairTask_TinyExtent(t *testing.T) {
	var (
		err                  error
		dp                   *DataPartition
		localExtentFileCount uint64 = 100
		testBaseDir                 = path.Join(os.TempDir(), t.Name())
		ctx                         = context.Background()
	)
	tcp := mock.NewMockTcp(tcpPort)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, localExtentFileCount, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	repairTasks := make([]*DataPartitionRepairTask, len(dp.replicas))
	tinyExtents := []uint64{1, 2, 3, 10}
	if err = dp.buildDataPartitionRepairTask(ctx, repairTasks, dp.replicas, proto.TinyExtentType, tinyExtents); err != nil {
		t.Fatalf("build data partition repair task err:%v", err)
	}
	if len(repairTasks[0].extents) != len(tinyExtents) {
		t.Fatalf("repairTasks[0] tiny extent count expect:%v, actual:%v", len(tinyExtents), len(repairTasks[0].extents))
	}
	if repairTasks[0].LeaderTinyDeleteRecordFileSize != int64(24) {
		t.Fatalf("repairTasks[0] leaderTinyDeleteRecordFileSize expect:%v, actual:%v", 24, repairTasks[0].LeaderTinyDeleteRecordFileSize)
	}
	if len(repairTasks[1].extents) != len(tinyExtents) {
		t.Fatalf("repairTasks[1] tiny extent info type:%v tiny extent count expect:%v actual:%v", proto.TinyExtentType, len(tinyExtents), len(repairTasks[1].extents))
	}
	_, brokenTinyExtents := dp.prepareRepairTasks(repairTasks)
	results := [][3]uint64{
		{uint64(len(tinyExtents)), 0, uint64(len(tinyExtents))},
		{0, 0, 0},
		{0, 0, 0},
	}
	for i, task := range repairTasks {
		if results[i][0] != uint64(len(task.ExtentsToBeRepairedSource)) || results[i][1] != uint64(len(task.ExtentsToBeCreated)) || results[i][2] != uint64(len(task.ExtentsToBeRepaired)) {
			t.Fatalf("repairTasks[%v] result not match, expect:%v actual:%v", i, results[i],
				[3]int{len(task.ExtentsToBeRepairedSource), len(task.ExtentsToBeCreated), len(task.ExtentsToBeRepaired)})
		}
	}
	if len(brokenTinyExtents) != len(tinyExtents) {
		t.Fatalf("brokenTinyExtents count expect:%v, actual:%v", len(tinyExtents), len(brokenTinyExtents))
	}
	if len(repairTasks[0].ExtentsToBeRepairedSource) != len(tinyExtents) {
		t.Fatalf("repairTasks[0] ExtentsToBeRepairedSource count expect:%v, actual:%v", len(tinyExtents), len(repairTasks[0].ExtentsToBeRepairedSource))
	}
	if len(repairTasks[0].ExtentsToBeCreated) != 0 {
		t.Fatalf("repairTasks[0] v count expect:%v, actual:%v", 0, len(repairTasks[0].ExtentsToBeCreated))
	}
	if len(repairTasks[0].ExtentsToBeRepaired) != len(tinyExtents) {
		t.Fatalf("repairTasks[0] ExtentsToBeRepaired count expect:%v, actual:%v", len(tinyExtents), len(repairTasks[0].ExtentsToBeRepaired))
	}
	dp.DoRepairOnLeaderDisk(ctx, repairTasks)
	for _, extentId := range tinyExtents {
		if !dp.ExtentStore().HasExtent(extentId) {
			t.Fatalf("tiny extent(%v) should exist", extentId)
		}
		size, _ := dp.ExtentStore().LoadExtentWaterMark(extentId)
		expectSize := extentId * 1024
		if expectSize%pageSize != 0 {
			expectSize = expectSize + (pageSize - expectSize%pageSize)
		}
		if size != int64(expectSize) {
			t.Fatalf("repaired tiny extent(%v) size expect:%v actual:%v", extentId, expectSize, size)
		}
	}
	if err = dp.NotifyExtentRepair(ctx, repairTasks); err != nil {
		t.Fatalf("notify extent repair should not have err, but there are err:%v", err)
	}
}

func TestBuildDataPartitionRepairTask_NormalExtent(t *testing.T) {
	var (
		err                  error
		dp                   *DataPartition
		localExtentFileCount uint64 = 100
		testBaseDir                 = path.Join(os.TempDir(), t.Name())
		ctx                         = context.Background()
	)
	tcp := mock.NewMockTcp(tcpPort)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, localExtentFileCount, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	repairTasks := make([]*DataPartitionRepairTask, len(dp.replicas))
	var tinyExtents []uint64
	if err = dp.buildDataPartitionRepairTask(ctx, repairTasks, dp.replicas, proto.NormalExtentType, tinyExtents); err != nil {
		t.Fatalf("build data partition repair task err:%v", err)
	}
	if len(repairTasks[0].extents) != int(localExtentFileCount) {
		t.Fatalf("repairTasks[0] noraml extent count expect:%v, actual:%v", localExtentFileCount, len(repairTasks[0].extents))
	}
	if repairTasks[0].LeaderTinyDeleteRecordFileSize != int64(24) {
		t.Fatalf("repairTasks[0] leaderTinyDeleteRecordFileSize expect:%v, actual:%v", 24, repairTasks[0].LeaderTinyDeleteRecordFileSize)
	}
	_, _ = dp.prepareRepairTasks(repairTasks)
	remoteRepairCount := localExtentFileCount - (mock.RemoteNormalExtentCount - 1)
	results := [][3]uint64{
		{mock.RemoteNormalExtentCount, 1, mock.RemoteNormalExtentCount},
		{remoteRepairCount, remoteRepairCount, remoteRepairCount},
		{remoteRepairCount, remoteRepairCount, remoteRepairCount},
	}
	for i, task := range repairTasks {
		if results[i][0] != uint64(len(task.ExtentsToBeRepairedSource)) || results[i][1] != uint64(len(task.ExtentsToBeCreated)) || results[i][2] != uint64(len(task.ExtentsToBeRepaired)) {
			t.Fatalf("repairTasks[%v] result not match, expect:%v actual:%v", i, results[i],
				[3]int{len(task.ExtentsToBeRepairedSource), len(task.ExtentsToBeCreated), len(task.ExtentsToBeRepaired)})
		}
	}
	dp.DoRepairOnLeaderDisk(ctx, repairTasks)
	if !dp.ExtentStore().HasExtent(mock.LocalCreateExtentId) {
		t.Fatalf("normal extent(%v) should exist", mock.LocalCreateExtentId)
	}
	// 比较创建并修复后extent水位
	size, _ := dp.ExtentStore().LoadExtentWaterMark(mock.LocalCreateExtentId)
	if size != int64(mock.LocalCreateExtentId*1024) {
		t.Fatalf("created normal extent(%v) size expect:%v actual:%v", mock.LocalCreateExtentId, mock.LocalCreateExtentId*1024, size)
	}
	// 比较修复后extent水位
	for i := 0; i < mock.RemoteNormalExtentCount-1; i++ {
		extentId := uint64(i + 1 + proto.TinyExtentCount)
		size, _ = dp.ExtentStore().LoadExtentWaterMark(extentId)
		if size != int64(extentId*1024) {
			t.Fatalf("repaired normal extent(%v) size expect:%v actual:%v", extentId, extentId*1024, size)
		}
	}
	if err = dp.NotifyExtentRepair(ctx, repairTasks); err != nil {
		t.Fatalf("notify extent repair should not have err, but there are err:%v", err)
	}
}

func TestRepair(t *testing.T) {
	var (
		err                  error
		dp                   *DataPartition
		localExtentFileCount uint64 = 100
		testBaseDir                 = path.Join(os.TempDir(), t.Name())
		ctx                         = context.Background()
	)
	tcp := mock.NewMockTcp(tcpPort)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, localExtentFileCount, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	var brokenTinyExtents []uint64
	for i := 1; i <= proto.TinyExtentCount; i++ {
		brokenTinyExtents = append(brokenTinyExtents, uint64(i))
	}
	dp.repair(ctx, proto.TinyExtentType)
	for _, extentId := range brokenTinyExtents {
		if !dp.ExtentStore().HasExtent(extentId) {
			t.Fatalf("tiny extent(%v) should exist", extentId)
		}
		size, _ := dp.ExtentStore().LoadExtentWaterMark(extentId)
		expectSize := extentId * 1024
		if expectSize%pageSize != 0 {
			expectSize = expectSize + (pageSize - expectSize%pageSize)
		}
		if size != int64(expectSize) {
			t.Fatalf("repaired tiny extent(%v) size expect:%v actual:%v", extentId, expectSize, size)
		}
	}
	dp.repair(ctx, proto.NormalExtentType)
	if !dp.ExtentStore().HasExtent(mock.LocalCreateExtentId) {
		t.Fatalf("normal extent(%v) should exist", mock.LocalCreateExtentId)
	}
	// 比较创建并修复后extent水位
	size, _ := dp.ExtentStore().LoadExtentWaterMark(mock.LocalCreateExtentId)
	if size != int64(mock.LocalCreateExtentId*1024) {
		t.Fatalf("created normal extent(%v) size expect:%v actual:%v", mock.LocalCreateExtentId, mock.LocalCreateExtentId*1024, size)
	}
	// 比较修复后extent水位
	for i := 0; i < mock.RemoteNormalExtentCount-1; i++ {
		extentId := uint64(i + 1 + proto.TinyExtentCount)
		size, _ = dp.ExtentStore().LoadExtentWaterMark(extentId)
		if size != int64(extentId*1024) {
			t.Fatalf("repaired normal extent(%v) size expect:%v actual:%v", extentId, extentId*1024, size)
		}
	}
}

func TestDoStreamExtentFixRepairOnFollowerDisk(t *testing.T) {
	var (
		err                  error
		dp                   *DataPartition
		localExtentFileCount uint64 = 100
		testBaseDir                 = path.Join(os.TempDir(), t.Name())
		ctx                         = context.Background()
	)
	tcp := mock.NewMockTcp(tcpPort)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, localExtentFileCount, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	repairTasks := make([]*DataPartitionRepairTask, len(dp.replicas))
	var tinyExtents []uint64
	if err = dp.buildDataPartitionRepairTask(ctx, repairTasks, dp.replicas, proto.NormalExtentType, tinyExtents); err != nil {
		t.Fatalf("build data partition repair task err:%v", err)
	}
	_, _ = dp.prepareRepairTasks(repairTasks)
	wg := new(sync.WaitGroup)
	for _, extentInfo := range repairTasks[0].ExtentsToBeRepaired {
		wg.Add(1)
		source := repairTasks[0].ExtentsToBeRepairedSource[extentInfo[storage.FileID]]
		go dp.doStreamExtentFixRepairOnFollowerDisk(ctx, wg, extentInfo, source)
	}
	wg.Wait()
	// 比较修复后extent水位
	for i := 0; i < mock.RemoteNormalExtentCount-1; i++ {
		extentId := uint64(i + 1 + proto.TinyExtentCount)
		size, _ := dp.ExtentStore().LoadExtentWaterMark(extentId)
		if size != int64(extentId*1024) {
			t.Fatalf("repaired normal extent(%v) size expect:%v actual:%v", extentId, extentId*1024, size)
		}
	}
}

func createDataPartition(partitionId, normalExtentCount uint64, baseDir string, t *testing.T) (dp *DataPartition) {
	var (
		ctx = context.Background()
		err error
	)
	if err = os.MkdirAll(baseDir, os.ModePerm); err != nil {
		t.Fatalf("prepare test base dir:%v err: %v", baseDir, err)
	}

	if dp, err = initDataPartition(baseDir, partitionId, true); err != nil {
		t.Fatalf("init data partition err:%v", err)
	}
	// create normal extent
	var (
		data                      = []byte{1, 2, 3, 4, 5, 6}
		size                      = int64(len(data))
		crc                       = crc32.ChecksumIEEE(data)
		extentID           uint64 = proto.TinyExtentCount + 1
		deleteTinyExtentId uint64 = 1
	)
	for ; extentID <= proto.TinyExtentCount+normalExtentCount; extentID++ {
		if err = dp.extentStore.Create(extentID, true); err != nil {
			t.Fatalf("extent store create normal extent err:%v", err)
		}
		if err = dp.extentStore.Write(ctx, extentID, 0, size, data, crc, storage.AppendWriteType, false); err != nil {
			t.Fatalf("extent store write normal extent err:%v", err)
		}
	}
	if uint64(dp.GetExtentCount()) != proto.TinyExtentCount+normalExtentCount {
		t.Fatalf("get extent count expect:%v, actual:%v", proto.TinyExtentCount+normalExtentCount, dp.GetExtentCount())
	}
	// waiting time for modification is greater than 10s
	time.Sleep(time.Second * 11)
	dp.extentStore.RecordTinyDelete(deleteTinyExtentId, 1, 2)
	return
}

func initDataPartition(rootDir string, partitionID uint64, isCreatePartition bool) (partition *DataPartition, err error) {
	var (
		partitionSize = 128849018880
	)
	dataPath := path.Join(rootDir, fmt.Sprintf(DataPartitionPrefix+"_%v_%v", partitionID, partitionSize))
	host := fmt.Sprintf(":%v", tcpPort)
	partition = &DataPartition{
		volumeID:                "test-vol",
		clusterID:               "test-cluster",
		isLeader:                true,
		partitionID:             partitionID,
		path:                    dataPath,
		partitionSize:           partitionSize,
		replicas:                []string{host, host, host},
		repairC:                 make(chan struct{}, 1),
		fetchVolHATypeC:         make(chan struct{}, 1),
		stopC:                   make(chan bool, 0),
		stopRaftC:               make(chan uint64, 0),
		storeC:                  make(chan uint64, 128),
		snapshot:                make([]*proto.File, 0),
		partitionStatus:         proto.ReadWrite,
		DataPartitionCreateType: 0,
		monitorData:             statistics.InitMonitorData(statistics.ModelDataNode),
		persistSync:             make(chan struct{}, 1),
		inRepairExtents:         make(map[uint64]struct{}),
		applyStatus:             NewWALApplyStatus(),
	}
	d := new(Disk)
	d.repairTaskLimit = 10
	partition.disk = d
	partition.extentStore, err = storage.NewExtentStore(partition.path, partitionID, partitionSize, CacheCapacityPerPartition, nil, isCreatePartition)
	return
}