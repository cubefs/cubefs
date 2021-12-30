package datanode

import (
	"github.com/chubaofs/chubaofs/storage"
	"reflect"
	"testing"
)

func TestValidateCRC(t *testing.T) {
	dataNodeAddr1 := "192.168.0.31"
	dataNodeAddr2 := "192.168.0.32"
	dataNodeAddr3 := "192.168.0.33"

	dataNode1Extents := make([]storage.ExtentInfoBlock, 0)
	dataNode1Extents = append(dataNode1Extents, newExtentInfoForTest(1, 11028, 1776334861))
	dataNode1Extents = append(dataNode1Extents, newExtentInfoForTest(1028, 11028, 1776334861))
	dataNode2Extents := make([]storage.ExtentInfoBlock, 0)
	dataNode2Extents = append(dataNode2Extents, newExtentInfoForTest(1, 11028, 1776334861))
	dataNode2Extents = append(dataNode2Extents, newExtentInfoForTest(1028, 11028, 1776334861))
	dataNode3Extents := make([]storage.ExtentInfoBlock, 0)
	dataNode3Extents = append(dataNode3Extents, newExtentInfoForTest(1, 11028, 1776334861))
	dataNode3Extents = append(dataNode3Extents, newExtentInfoForTest(1028, 11028, 1776334861))

	validateCRCTasks := make([]*DataPartitionValidateCRCTask, 0, 3)
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode1Extents, dataNodeAddr1, dataNodeAddr1))
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode2Extents, dataNodeAddr2, dataNodeAddr1))
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode3Extents, dataNodeAddr3, dataNodeAddr1))

	dp := &DataPartition{
		partitionID: 1,
	}
	dp.validateCRC(validateCRCTasks)
}

func TestCheckNormalExtentFile_differentCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334861))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334862))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334863))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	crcLocAddrMap := make(map[uint64][]string)
	for i, extentInfo := range extentInfos {
		crcLocAddrMap[extentInfo[storage.Crc]] = append(crcLocAddrMap[extentInfo[storage.Crc]], extentReplicaSource[i])
	}
	extentCrcInfo, crcNotEqual := dp.checkNormalExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != true {
		t.Errorf("action[TestCheckNormalExtentFile_differentCrc] failed, result[%v] expect[%v]", crcNotEqual, true)
	}
	if reflect.DeepEqual(crcLocAddrMap, extentCrcInfo.CrcLocAddrMap) == false {
		t.Errorf("action[TestCheckNormalExtentFile_differentCrc] failed, result[%v] expect[%v]", extentCrcInfo.CrcLocAddrMap, crcLocAddrMap)
	}
}

func TestCheckNormalExtentFile_SameCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	_, crcNotEqual := dp.checkNormalExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckNormalExtentFile_SameCrc] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckNormalExtentFile_OnlyOneReplica(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	_, crcNotEqual := dp.checkNormalExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckNormalExtentFile_OnlyOneReplica] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_DifferentCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334861))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334862))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334863))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != true {
		t.Errorf("action[TestCheckTinyExtentFile_DifferentCrc] failed, result[%v] expect[%v]", crcNotEqual, true)
	}
}

func TestCheckTinyExtentFile_SameCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_SameCrc] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_OnlyOneReplica(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_OnlyOneReplica] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_DiffSize(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11301, 1776334861))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11302, 1776334862))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11303, 1776334863))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_DiffSize] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func newExtentInfoForTest(fileID, size, crc uint64) storage.ExtentInfoBlock {
	return storage.ExtentInfoBlock{
		storage.FileID: fileID,
		storage.Size:   size,
		storage.Crc:    crc,
	}
}
