package data

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	datanodeAddr = "192.168.0.31:17320"
)

var (
	dataClient = NewDataHttpClient(datanodeAddr, false)
	partitionIds []uint64
)

func init() {
	partitions, err := dataClient.GetPartitionsFromNode()
	if err != nil {
		fmt.Errorf("action[init] GetPartitionsFromNode err:%v", err)
	}
	for _, partition := range partitions.Partitions {
		partitionIds = append(partitionIds, partition.ID)
	}
	sort.Slice(partitionIds, func(i, j int) bool {
		return partitionIds[i] < partitionIds[j]
	})
}

func TestComputeExtentMd5(t *testing.T) {
	var (
		partitionID uint64 = 1
		extentID uint64 = 1
		offset uint64 = 0
		size uint64 = 0
	)
	for _, pId := range partitionIds {
		md5Remote, err := dataClient.ComputeExtentMd5(pId, extentID, offset, size)
		if err != nil {
			t.Fatalf("ComputeExtentMd5 failed, err:%v", err)
		}
		md5Writer := md5.New()
		md5local := hex.EncodeToString(md5Writer.Sum(nil))
		if md5local != md5Remote {
			t.Fatalf("cal md5 err, expect:%v actual:%v", md5local, md5Remote)
		}
	}
	dataClient.ComputeExtentMd5(partitionID, extentID, offset, 10000)
}

func TestGetDisks(t *testing.T) {
	_, err := dataClient.GetDisks()
	if err != nil {
		t.Fatalf("GetDisks failed, err:%v", err)
	}
}

func TestGetPartitionsFromNode(t *testing.T) {
	_, err := dataClient.GetPartitionsFromNode()
	if err != nil {
		t.Fatalf("GetPartitionsFromNode failed, err:%v", err)
	}
}

func TestGetPartitionFromNode(t *testing.T) {
	for _, pId := range partitionIds {
		pInfo, err := dataClient.GetPartitionFromNode(pId)
		if err != nil {
			t.Fatalf("GetPartitionFromNode id:%v failed, err:%v", pId, err)
		}
		if pInfo.ID != pId {
			t.Fatalf("GetPartitionFromNode id expect:%v actual:%v", pId, pInfo.ID)
		}
	}
	var noExistPid uint64
	idLength := len(partitionIds)
	if idLength > 0 {
		noExistPid = partitionIds[idLength-1] + 1
	} else {
		noExistPid = 1
	}
	_, err := dataClient.GetPartitionFromNode(noExistPid)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("GetPartitionFromNode id:%v failed, err:%v", noExistPid, err)
	}
}

func TestGetExtentHoles(t *testing.T) {
	for _, pId := range partitionIds {
		for extId := 1; extId <= proto.TinyExtentCount; extId++ {
			_, err := dataClient.GetExtentHoles(pId, uint64(extId))
			if err != nil {
				t.Fatalf("GetExtentHoles err:%v", err)
			}
		}
	}
}

func TestStopAndReLoadPartition(t *testing.T) {
	if len(partitionIds) == 0 {
		return
	}
	pId := partitionIds[0]
	err := dataClient.StopPartition(pId)
	if err != nil {
		t.Fatalf("StopPartition err:%v", err)
	}
	err = dataClient.StopPartition(math.MaxUint64)
	if err == nil {
		t.Fatalf("StopPartition expect err, but success")
	}
	partitionDirName, dirPath := fmt.Sprintf("datapartition_%v_128849018880", pId), "/cfs/disk"
	wrongDirPath := "/cfs/wrongDisk"

	err = dataClient.ReLoadPartition(partitionDirName, dirPath)
	if err != nil {
		t.Fatalf("ReLoadPartition err:%v", err)
	}
	err = dataClient.ReLoadPartition(partitionDirName, wrongDirPath)
	if err == nil {
		t.Fatalf("ReLoadPartition expect err, but success")
	}
}

func TestGetExtentBlockCrc(t *testing.T) {
	for _, pId := range partitionIds {
		_, _ = dataClient.GetExtentBlockCrc(pId, 1)
		_, _ = dataClient.GetExtentBlockCrc(pId, proto.TinyExtentCount+1)
	}
}

func TestGetExtentInfo(t *testing.T) {
	for _, pId := range partitionIds {
		_, err := dataClient.GetExtentInfo(pId, 1)
		if err != nil {
			t.Fatalf("GetExtentInfo err:%v", err)
		}
	}
}

func TestRepairExtent(t *testing.T) {
	for _, pId := range partitionIds {
		partitionPath := fmt.Sprintf("/cfs/disk/datapartition_%v_128849018880", pId)
		err := dataClient.RepairExtent(1, partitionPath, pId)
		if err != nil {
			t.Fatalf("RepairExtent partition:%v extent:%v partitionPath:%v, err:%v", pId, 1, partitionPath,  err)
		}
	}
}

func TestRepairExtentBatch(t *testing.T) {
	extents := []string{"1", "2", "3", "4", "5"}
	extentsStr := strings.Join(extents, "-")
	for _, pId := range partitionIds {
		partitionPath := fmt.Sprintf("/cfs/disk/datapartition_%v_128849018880", pId)
		exts, err := dataClient.RepairExtentBatch(extentsStr, partitionPath, pId)
		if err != nil {
			t.Fatalf("RepairExtentBatch partition:%v extents:%v partitionPath:%v, err:%v", pId, extentsStr, partitionPath, err)
		}
		if len(exts) != len(extents) {
			t.Fatalf("RepairExtentBatch partition:%v extents:%v partitionPath:%v exts count expect:%v actual:%v", pId, extentsStr, partitionPath, len(extents), len(exts))
		}
	}
}

func TestGetDatanodeStats(t *testing.T) {
	stats, err := dataClient.GetDatanodeStats()
	if err != nil {
		t.Fatalf("GetDatanodeStats, err:%v", err)
	}
	fmt.Printf("stats:%v\n", stats)
}

func TestGetRaftStatus(t *testing.T) {
	var raftId uint64 = 10
	raftStatus, err := dataClient.GetRaftStatus(raftId)
	if err != nil {
		t.Fatalf("GetRaftStatus, raftId:%v err:%v", raftId, err)
	}
	fmt.Printf("raftStatus:%v\n", raftStatus)
}

func TestSetAutoRepairStatus(t *testing.T) {
	autoRepairResult, err := dataClient.SetAutoRepairStatus(true)
	if err != nil {
		t.Fatalf("SetAutoRepairStatus, err:%v", err)
	}
	if autoRepairResult != true {
		t.Fatalf("SetAutoRepairStatus, autoRepairResult expect:%v actual:%v", true, autoRepairResult)
	}
	autoRepairResult, err = dataClient.SetAutoRepairStatus(false)
	if err != nil {
		t.Fatalf("SetAutoRepairStatus, err:%v", err)
	}
	if autoRepairResult != false {
		t.Fatalf("SetAutoRepairStatus, autoRepairResult expect:%v actual:%v", false, autoRepairResult)
	}
}

func TestReleasePartitions(t *testing.T) {
	wrongKey := "abc"
	_, err := dataClient.ReleasePartitions(wrongKey)
	if err == nil || !strings.Contains(err.Error(), "auth key not match") {
		t.Fatalf("ReleasePartitions failed, err:%v", err)
	}
	//key := generateAuthKey()
	//_, err = dataClient.ReleasePartitions(key)
	//if err != nil {
	//	t.Fatalf("ReleasePartitions failed, err:%v", err)
	//}
}

func TestGetStatInfo(t *testing.T) {
	_, err := dataClient.GetStatInfo()
	if err != nil {
		t.Fatalf("GetStatInfo, err:%v", err)
	}
}

func TestGetReplProtocolDetail(t *testing.T) {
	_, err := dataClient.GetReplProtocolDetail()
	if err != nil {
		t.Fatalf("GetReplProtocolDetail, err:%v", err)
	}
}

func TestMoveExtentFile(t *testing.T) {
	for _, pId := range partitionIds {
		partitionPath := fmt.Sprintf("/cfs/disk/datapartition_%v_128849018880", pId)
		err := dataClient.MoveExtentFile(1, partitionPath, pId)
		if err != nil {
			t.Fatalf("MoveExtentFile partition:%v extent:%v partitionPath:%v, err:%v", pId, 1, partitionPath,  err)
		}
	}
}

func TestMoveExtentFileBatch(t *testing.T) {
	extents := []string{"1", "2", "3", "4", "5"}
	extentsStr := strings.Join(extents, "-")
	for _, pId := range partitionIds {
		partitionPath := fmt.Sprintf("/cfs/disk/datapartition_%v_128849018880", pId)
		exts, err := dataClient.MoveExtentFileBatch(extentsStr, partitionPath, pId)
		if err != nil {
			t.Fatalf("MoveExtentFileBatch partition:%v extents:%v partitionPath:%v, err:%v", pId, extentsStr, partitionPath, err)
		}
		if len(exts) != len(extents) {
			t.Fatalf("MoveExtentFileBatch partition:%v extents:%v partitionPath:%v exts count expect:%v actual:%v", pId, extentsStr, partitionPath, len(extents), len(exts))
		}
	}
}

func TestGetExtentCrc(t *testing.T) {
	for _, pId := range partitionIds {
		_, err := dataClient.GetExtentCrc(pId, 1)
		if err != nil {
			t.Fatalf("GetExtentCrc partition:%v extent:%v, err:%v", pId, 1, err)
		}
	}
}

func TestPlaybackPartitionTinyDelete(t *testing.T) {
	for _, pId := range partitionIds {
		err := dataClient.PlaybackPartitionTinyDelete(pId)
		if err != nil {
			t.Fatalf("PlaybackPartitionTinyDelete err:%v", err)
		}
	}
}

func generateAuthKey() string {
	date := time.Now().Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

