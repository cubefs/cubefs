package datanode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"os"
	"path"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
)
var disk *Disk

func init() {
	var (
		diskFDLimit = FDLimit{
			MaxFDLimit:      DiskMaxFDLimit,
			ForceEvictRatio: DiskForceEvictFDRatio,
		}
		spaceManager = &SpaceManager{
			repairTaskLimitOnDisk: 0,
			fixTinyDeleteRecordLimitOnDisk: 0,
			dataNode: &DataNode{
				localServerAddr: localNodeAddress,
			},
			partitions: make(map[uint64]*DataPartition),
		}
		reservedSpace uint64 = DefaultDiskReservedSpace
		stateFS              = new(syscall.Statfs_t)
	)
	fmt.Println("init disk")
	if err := syscall.Statfs(testDiskPath, stateFS); err == nil {
		reservedSpace = uint64(float64(stateFS.Blocks*uint64(stateFS.Bsize)) * DefaultDiskReservedRatio)
	}
	disk = new(Disk)
	disk.Path = testDiskPath
	disk.ReservedSpace = reservedSpace
	disk.MaxErrCnt = DefaultDiskMaxErr
	disk.fdLimit = diskFDLimit
	disk.space = spaceManager
	disk.partitionMap = make(map[uint64]*DataPartition)
	disk.fixTinyDeleteRecordLimit = spaceManager.fixTinyDeleteRecordLimitOnDisk
	disk.repairTaskLimit = spaceManager.repairTaskLimitOnDisk
	disk.RejectWrite = false
}
func TestFDCount(t *testing.T) {
	var (
		incParallelNum = 10
		incNum         = 10000
		decParallelNum = 10
		decNum         = 100
		wg             sync.WaitGroup
	)
	wg.Add(incParallelNum)
	wg.Add(decParallelNum)
	for i := 0; i < incParallelNum; i++ {
		go func() {
			defer func() {
				wg.Done()
			}()
			for j := 0; j < incNum; j++ {
				disk.IncreaseFDCount()
			}
		}()
	}
	for i := 0; i < decParallelNum; i++ {
		go func() {
			defer func() {
				wg.Done()
			}()
			for j := 0; j < decNum; j++ {
				disk.DecreaseFDCount()
			}
		}()
	}
	wg.Wait()
	total := incParallelNum*incNum - decParallelNum*decNum
	if disk.fdCount != int64(total) {
		t.Fatalf("disk fdCount mismatch, except:%v, actual:%v", total, disk.fdCount)
	}
}

func TestPartitionCount(t *testing.T) {
	var (
		attachParallelNum = 10
		attachNum         = 10000
		detachParallelNum = 10
		detachNum         = 100
		attachWg          sync.WaitGroup
		detachWg          sync.WaitGroup
	)
	attachWg.Add(attachParallelNum)
	for i := 0; i < attachParallelNum; i++ {
		go func() {
			defer func() {
				attachWg.Done()
			}()
			for j := 0; j < attachNum; j++ {
				dp := &DataPartition{partitionID: uint64(j)}
				disk.AttachDataPartition(dp)
				disk.PartitionCount()
			}
		}()
	}
	attachWg.Wait()
	detachWg.Add(detachParallelNum)
	detachWg.Add(detachParallelNum)
	for i := 0; i < detachParallelNum; i++ {
		go func() {
			defer func() {
				detachWg.Done()
			}()
			for j := 0; j < detachNum; j++ {
				dp := &DataPartition{partitionID: uint64(j)}
				disk.DetachDataPartition(dp)
				disk.PartitionCount()
			}
		}()
	}
	for i := 0; i < detachParallelNum; i++ {
		go func() {
			defer func() {
				detachWg.Done()
			}()
			for j := 0; j < detachNum; j++ {
				dp := &DataPartition{partitionID: uint64(j)}
				disk.DetachDataPartition(dp)
				disk.PartitionCount()
			}
		}()
	}
	detachWg.Wait()
	actualDpCount := disk.PartitionCount()
	total := attachNum - detachNum
	if actualDpCount != total {
		t.Fatalf("disk partitionCount mismatch, except:%v, actual:%v", total, actualDpCount)
	}
}

func TestCanRepairOnDisk(t *testing.T) {
	if disk.canRepairOnDisk() {
		t.Fatal("This should be able to repair on disk")
	}
	disk.repairTaskLimit = 10
	wantRepairCount := 20
	for i := 0; i < wantRepairCount; i++ {
		if uint64(i) < disk.repairTaskLimit {
			if !disk.canRepairOnDisk() {
				t.Fatal("This should not repair on disk")
			}
		} else {
			if disk.canRepairOnDisk() {
				t.Fatal("This should be able to repair on disk")
			}
		}
	}
	if disk.executingRepairTask != disk.repairTaskLimit {
		t.Fatalf("disk executingRepairTask mismatch, except:%v, actual:%v", disk.repairTaskLimit, disk.executingRepairTask)
	}
}

func TestFinishRepairTask(t *testing.T) {
	num := 100
	disk.executingRepairTask = uint64(num)
	for i := 0; i < num/2; i++ {
		disk.finishRepairTask()
	}
	if disk.executingRepairTask != uint64(num)/2 {
		t.Fatalf("disk executingRepairTask mismatch, except:%v, actual:%v", uint64(num)/2, disk.executingRepairTask)
	}
}

func TestComputeUsage(t *testing.T) {
	if err := disk.computeUsage(); err != nil {
		t.Fatalf("Disk compute usage failed, err:%v", err)
	}
	t.Logf("disk Total:%v, Available:%v, Used:%v, RejectWrite:%v, Unallocated:%v", disk.Total, disk.Available, disk.Used, disk.RejectWrite, disk.Unallocated)
}

func TestIsPartitionDir(t *testing.T) {
	fileNames := make(map[string]bool)
	for i := 0; i < 100; i++ {
		name := fmt.Sprintf("datapartition_%v_%v", i+1, i+123)
		fileNames[name] = true
	}
	fileNames["Adatapartition_1_123"] = false
	fileNames["atapartition_1_123"] = false
	fileNames["atapartition_1_123Ad"] = false

	for fileName, b := range fileNames {
		if disk.isPartitionDir(fileName) != b {
			t.Fatalf("disk isPartitionDir mismatch, fileName:%v, except:%v, actual:%v", fileName, b, disk.isPartitionDir(fileName))
		}
	}
}

func TestDataPartitionList(t *testing.T) {
	var partitionIds []uint64
	disk.partitionMap = make(map[uint64]*DataPartition)
	for i := 0; i < 100; i++ {
		pId:= uint64(i)+1
		partitionIds = append(partitionIds, pId)
		dp := &DataPartition{
			partitionID: pId,
		}
		disk.AttachDataPartition(dp)
	}
	partitionList := disk.DataPartitionList()
	sort.Slice(partitionList, func(i, j int) bool {
		return partitionList[i] < partitionList[j]
	})
	if !reflect.DeepEqual(partitionIds, partitionList) {
		t.Fatalf("disk DataPartitionList mismatch, partitionIds except:%v, actual:%v", partitionIds, partitionList)
	}
}

func TestGetDataPartition(t *testing.T) {
	var pid uint64 = 100
	disk.partitionMap = make(map[uint64]*DataPartition)
	dp := &DataPartition{partitionID: pid}
	disk.AttachDataPartition(dp)
	partition := disk.GetDataPartition(pid)
	if dp != partition {
		t.Fatalf("disk GetDataPartition mismatch, except:%v, actual:%v", dp, partition)
	}
}

func TestUnmarshalPartitionName(t *testing.T) {
	var pId uint64 = 100
	var pSize int = 536870912000
	fileName := fmt.Sprintf("datapartition_%v_%v", pId, pSize)
	partitionID, partitionSize, err := unmarshalPartitionName(fileName)
	if err != nil {
		t.Fatalf("unmarshalPartitionName, err:%v", err)
	}
	if partitionID != pId {
		t.Fatalf("unmarshalPartitionName mismatch, partitionID except:%v, actual:%v", pId, partitionID)
	}
	if partitionSize != pSize {
		t.Fatalf("unmarshalPartitionName mismatch, partitionSize except:%v, actual:%v", pSize, partitionSize)
	}
	fileName = "datapartition"
	partitionID, partitionSize, err = unmarshalPartitionName(fileName)
	if err == nil {
		t.Fatalf("unmarshalPartitionName fileName:%v is not standard", fileName)
	}
}

func TestUpdateSpaceInfo(t *testing.T) {
	err := disk.updateSpaceInfo()
	if err != nil {
		t.Fatalf("updateSpaceInfo err:%v", err)
	}
	t.Logf("action[updateSpaceInfo] disk(%v) total(%v) available(%v) remain(%v) "+
		"restSize(%v) maxErrs(%v) readErrs(%v) writeErrs(%v) status(%v)", disk.Path,
		disk.Total, disk.Available, disk.Unallocated, disk.ReservedSpace, disk.MaxErrCnt, disk.ReadErrCnt, disk.WriteErrCnt, disk.Status)
}

func TestCheckDiskStatus(t *testing.T) {
	disk.checkDiskStatus()
	filePath := path.Join(disk.Path, DiskStatusFile)
	fp, _ := os.OpenFile(filePath, os.O_RDONLY, 0755)
	if _, err := fp.ReadAt([]byte(DiskStatusFile), 0); err != nil {
		t.Fatalf("checkDiskStatus err:%v", err)
	}
	_ = os.Remove(filePath)
}

func TestRestoreOnePartition(t *testing.T) {
	var dpId uint64
	dpId = 10
	MasterClient.AdminAPI().MockCreateDataPartition(localNodeAddress, dpId)

	visitor := func(dp *DataPartition) {}
	var applyId uint64 = 1234
	dpDir := fmt.Sprintf("datapartition_%v_128849018880", dpId)
	initDataPartitionUnderFile(dpId, applyId, dpDir)

	defer removeUnderFile(dpDir)
	err := disk.RestoreOnePartition(visitor, dpDir)
	if err != nil {
		t.Fatalf("RestoreOnePartition err:%v", err)
	}
	partition := disk.GetDataPartition(dpId)
	if partition.applyStatus != nil {
		applied := partition.applyStatus.Applied()
		if applied != applyId {
			t.Fatalf("disk restoreOnePartition mismatch, applyId except:%v, actual:%v", applyId, applied)
		}
	}
	err = disk.RestoreOnePartition(visitor, "")
	if err == nil || !strings.Contains(err.Error(), "partition path is empty") {
		t.Fatalf("err:%v", err)
	}
	err = disk.RestoreOnePartition(visitor, "lopsd")
	if err == nil || !strings.Contains(err.Error(), "read dir") {
		t.Fatalf("err:%v", err)
	}
	dpDir = fmt.Sprintf("datapartition_%v", dpId)
	initDataPartitionUnderFile(dpId, applyId, dpDir)
	defer func() {
		removeUnderFile(dpDir)
		MasterClient.AdminAPI().MockDeleteDataReplica(localNodeAddress, dpId)
	}()
	err = disk.RestoreOnePartition(visitor, dpDir)
	if err == nil || !strings.Contains(err.Error(), "invalid partition path") {
		t.Fatalf("err:%v", err)
	}
	dpId = 11
	applyId = 1234
	dpDir = fmt.Sprintf("datapartition_%v_128849018880", dpId)
	MasterClient.AdminAPI().MockCreateDataPartition(localNodeAddress, dpId)
	initDataPartitionUnderFile(uint64(dpId), applyId, dpDir)
	defer func() {
		removeUnderFile(ExpiredPartitionPrefix+dpDir)
		MasterClient.AdminAPI().MockDeleteDataReplica(localNodeAddress, dpId)
	}()
	_ = disk.RestoreOnePartition(visitor, dpDir)
}

func TestRestorePartition(t *testing.T) {
	var dpId uint64 = 10
	var applyId uint64 = 1234
	dpDir := fmt.Sprintf("datapartition_%v_128849018880", dpId)
	initDataPartitionUnderFile(dpId, applyId, dpDir)
	MasterClient.AdminAPI().MockCreateDataPartition(localNodeAddress, dpId)
	defer func() {
		removeUnderFile(dpDir)
		MasterClient.AdminAPI().MockDeleteDataReplica(localNodeAddress, dpId)
	}()
	disk.RestorePartition(nil, 1)
}

func TestGetPersistPartitionsFromMaster(t *testing.T) {
	dInfo, err := disk.getPersistPartitionsFromMaster()
	if err != nil {
		t.Fatalf("getPersistPartitionsFromMaster err:%v", err)
	}
	fmt.Println(dInfo)
}

func TestForceExitRaftStore(t *testing.T) {
	disk.ForceExitRaftStore()
	for _, partition := range disk.partitionMap {
		if partition.partitionStatus != proto.Unavailable {
			t.Fatalf("disk ForceExitRaftStore mismatch, pId:%v partitionStatus except:%v, actual:%v", partition.partitionID, proto.Unavailable, partition.partitionStatus)
		}
	}
}

func TestIncReadErrCnt(t *testing.T) {
	var (
		incParallelNum = 10
		incNum         = 10000
		wg             sync.WaitGroup
	)
	wg.Add(incParallelNum)
	for i := 0; i < incParallelNum; i++ {
		go func() {
			defer func() {
				wg.Done()
			}()
			for j := 0; j < incNum; j++ {
				disk.incReadErrCnt()
			}
		}()
	}
	wg.Wait()
	total := incParallelNum*incNum
	if disk.ReadErrCnt != uint64(total) {
		t.Fatalf("disk ReadErrCnt mismatch, ReadErrCnt except:%v, actual:%v", total, disk.ReadErrCnt)
	}
}

func TestIncWriteErrCnt(t *testing.T) {
	var (
		incParallelNum = 10
		incNum         = 10000
		wg             sync.WaitGroup
	)
	wg.Add(incParallelNum)
	for i := 0; i < incParallelNum; i++ {
		go func() {
			defer func() {
				wg.Done()
			}()
			for j := 0; j < incNum; j++ {
				disk.incWriteErrCnt()
			}
		}()
	}
	wg.Wait()
	total := incParallelNum*incNum
	if disk.WriteErrCnt != uint64(total) {
		t.Fatalf("disk WriteErrCnt mismatch, WriteErrCnt except:%v, actual:%v", total, disk.WriteErrCnt)
	}
}

func TestTriggerDiskError(t *testing.T) {
	disk.triggerDiskError(syscall.EIO)
	if disk.Status != proto.Unavailable {
		t.Fatalf("disk triggerDiskError mismatch, disk status except:%v, actual:%v", proto.Unavailable, disk.Status)
	}
}

func TestCanFinTinyDeleteRecord(t *testing.T) {
	var limit uint64 = 10
	disk.fixTinyDeleteRecordLimit = limit
	var i uint64
	for ; i < limit * 2; i++ {
		if i < limit {
			if !disk.canFinTinyDeleteRecord() {
				t.Fatal("")
			}
		} else {
			if disk.canFinTinyDeleteRecord() {
				t.Fatal("")
			}
		}
	}
}

func initDataPartitionUnderFile(partitionId uint64, applyId uint64, dpDir string) {
	partitionPath := path.Join(disk.Path, dpDir)
	_ = os.Mkdir(partitionPath, 0755)
	fpApply, _ := os.OpenFile(path.Join(partitionPath, ApplyIndexFile), os.O_CREATE|os.O_RDWR, 0666)
	_, _ = fpApply.WriteString(fmt.Sprintf("%v", applyId))
	fp, _ := os.OpenFile(path.Join(partitionPath, DataPartitionMetadataFileName), os.O_CREATE|os.O_RDWR, 0666)
	defer func() {
		fp.Close()
		fpApply.Close()
	}()
	_, _ = fp.WriteString(fmt.Sprintf(`{
		"VolumeID":"ltptest",
		"PartitionID":%v,
		"PartitionSize":128849018880,
		"CreateTime":"2022-11-02 17:02:23",
		"Peers":[
			{
				"id":2,
				"addr":"192.168.0.33:17310"
			},
			{
				"id":4,
				"addr":"192.168.0.31:17310"
			},
			{
				"id":9,
				"addr":"192.168.0.32:17310"
			}
		],
		"Hosts":[
			"192.168.0.33:17310",
			"192.168.0.32:17310",
			"192.168.0.31:17310"
		],
		"Learners":[
	
		],
		"DataPartitionCreateType":0,
		"LastTruncateID":0,
		"LastUpdateTime":1667464155,
		"VolumeHAType":0,
		"IsCatchUp":false
	}`, partitionId))
}

func removeUnderFile(dpDir string) {
	partitionPath := path.Join(disk.Path, dpDir)
	_ = os.RemoveAll(partitionPath)
}
