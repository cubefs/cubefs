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
	dataNode1Extents = append(dataNode1Extents, newExtentInfoForTest(1, 11028, 1776334861, dataNodeAddr1))
	dataNode1Extents = append(dataNode1Extents, newExtentInfoForTest(1028, 11028, 1776334861, dataNodeAddr1))
	dataNode2Extents := make([]storage.ExtentInfoBlock, 0)
	dataNode2Extents = append(dataNode2Extents, newExtentInfoForTest(1, 11028, 1776334861, dataNodeAddr2))
	dataNode2Extents = append(dataNode2Extents, newExtentInfoForTest(1028, 11028, 1776334861, dataNodeAddr2))
	dataNode3Extents := make([]storage.ExtentInfoBlock, 0)
	dataNode3Extents = append(dataNode3Extents, newExtentInfoForTest(1, 11028, 1776334861, dataNodeAddr3))
	dataNode3Extents = append(dataNode3Extents, newExtentInfoForTest(1028, 11028, 1776334861, dataNodeAddr3))

	validateCRCTasks := make([]*DataPartitionValidateCRCTask, 0, 3)
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode1Extents, dataNodeAddr1, dataNodeAddr1))
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode2Extents, dataNodeAddr2, dataNodeAddr1))
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode3Extents, dataNodeAddr3, dataNodeAddr1))

	for _, task := range validateCRCTasks {
		task.extents[9] = storage.EmptyExtentBlock
	}
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
	replicas := make([]string, 3)
	replicas[0] = "192.168.0.31"
	replicas[1] = "192.168.0.32"
	replicas[2] = "192.168.0.33"

	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334861, replicas[0]))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334862, replicas[1]))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334863, replicas[2]))

	crcLocAddrMap := make(map[uint32][]string)
	for index, einfo := range extentInfos {
		crcLocAddrMap[uint32(einfo[storage.Crc])] = append(crcLocAddrMap[uint32(einfo[storage.Crc])], replicas[index])
	}
	extentCrcInfo, crcNotEqual := dp.checkNormalExtentFile(extentInfos, replicas)
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
	replicas := make([]string, 3)
	replicas[0] = "192.168.0.31"
	replicas[1] = "192.168.0.32"
	replicas[2] = "192.168.0.33"
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865, replicas[0]))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865, replicas[1]))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865, replicas[2]))

	_, crcNotEqual := dp.checkNormalExtentFile(extentInfos, replicas)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckNormalExtentFile_SameCrc] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckNormalExtentFile_OnlyOneReplica(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	replicas := make([]string, 3)
	replicas[0] = "192.168.0.31"
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865, "192.168.0.31"))
	_, crcNotEqual := dp.checkNormalExtentFile(extentInfos, replicas)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckNormalExtentFile_OnlyOneReplica] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_DifferentCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	replicas := make([]string, 3)
	replicas[0] = "192.168.0.31"
	replicas[1] = "192.168.0.32"
	replicas[2] = "192.168.0.33"
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334861, replicas[0]))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334862, replicas[1]))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334863, replicas[2]))

	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, replicas)
	if crcNotEqual != true {
		t.Errorf("action[TestCheckTinyExtentFile_DifferentCrc] failed, result[%v] expect[%v]", crcNotEqual, true)
	}
}

func TestCheckTinyExtentFile_SameCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	replicas := make([]string, 3)
	replicas[0] = "192.168.0.31"
	replicas[1] = "192.168.0.32"
	replicas[2] = "192.168.0.33"
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865, replicas[0]))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865, replicas[1]))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865, replicas[2]))

	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, replicas)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_SameCrc] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_OnlyOneReplica(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	replicas := make([]string, 3)
	replicas[0] = "192.168.0.31"
	replicas[1] = "192.168.0.32"
	replicas[2] = "192.168.0.33"
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865, replicas[0]))
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, replicas)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_OnlyOneReplica] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_DiffSize(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	replicas := make([]string, 3)
	replicas[0] = "192.168.0.31"
	replicas[1] = "192.168.0.32"
	replicas[2] = "192.168.0.33"
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11301, 1776334861, replicas[0]))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11302, 1776334862, replicas[1]))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11303, 1776334863, replicas[2]))
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, replicas)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_DiffSize] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func newExtentInfoForTest(fileID, size uint64, crc uint32, source string) (einfo storage.ExtentInfoBlock) {
	einfo[storage.FileID] = fileID
	einfo[storage.Size] = size
	einfo[storage.Crc] = uint64(crc)
	return
}
