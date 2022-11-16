package data

import (
	"strings"
	"testing"
)

const (
	ltptestVolume = "ltptest"
	ltptestMaster = "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
)

func TestWrapper_getDataPartitionByPid(t *testing.T) {
	dataWrapper, err := NewDataPartitionWrapper(ltptestVolume, strings.Split(ltptestMaster, ","))
	if err != nil {
		t.Fatalf("NewDataPartitionWrapper failed, err %v", err)
	}

	dataWrapper.InitFollowerRead(true)
	dataWrapper.SetNearRead(true)

	close(dataWrapper.stopC)

	var validPids []uint64
	dataWrapper.partitions.Range(func(key, value interface{}) bool {
		pid := key.(uint64)
		validPids = append(validPids, pid)
		return true
	})
	if len(validPids) == 0 {
		t.Fatalf("no valid data partition for test")
	}

	var invalidPid uint64
	for _, pid := range validPids {
		if invalidPid <= pid {
			invalidPid = pid + 1
		}
		if err = dataWrapper.getDataPartitionByPid(pid); err != nil {
			t.Fatalf("getDataPartitionByPid failed, pid %v, err %v", pid, err)
		}
	}

	if err = dataWrapper.getDataPartitionByPid(invalidPid); err == nil {
		t.Fatalf("getDataPartitionByPid use invalidPid %v, expect failed but success", invalidPid)
	}
}
