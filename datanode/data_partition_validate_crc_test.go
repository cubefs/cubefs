package datanode

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/datanode/mock"
	"github.com/cubefs/cubefs/storage"
	"os"
	"path"
	"reflect"
	"testing"
	"time"
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

func TestIsGetConnectError(t *testing.T) {
	err := fmt.Errorf("rand str %v op", errorGetConnectMsg)
	connectError := isGetConnectError(err)
	if !connectError {
		t.Errorf("action[TestIsGetConnectError] failed, isGetConnectError expect[%v] actual[%v]", true, connectError)
	}
	err = fmt.Errorf("rand str abc op")
	connectError = isGetConnectError(err)
	if connectError {
		t.Errorf("action[TestIsGetConnectError] failed, isGetConnectError expect[%v] actual[%v]", false, connectError)
	}
}

func TestIsConnectionRefusedFailure(t *testing.T) {
	err := fmt.Errorf("rand str %v op", errorConnRefusedMsg)
	connectRefusedFailure := isConnectionRefusedFailure(err)
	if !connectRefusedFailure {
		t.Errorf("action[TestIsConnectionRefusedFailure] failed, isConnectionRefusedFailure expect[%v] actual[%v]", true, connectRefusedFailure)
	}
	err = fmt.Errorf("rand str abc op")
	connectRefusedFailure = isConnectionRefusedFailure(err)
	if connectRefusedFailure {
		t.Errorf("action[TestIsConnectionRefusedFailure] failed, isGetConnectError expect[%v] actual[%v]", false, connectRefusedFailure)
	}
}

func TestIsIOTimeoutFailure(t *testing.T) {
	err := fmt.Errorf("rand str %v op", errorIOTimeoutMsg)
	ioTimeoutFailure := isIOTimeoutFailure(err)
	if !ioTimeoutFailure {
		t.Errorf("action[TestIsIOTimeoutFailure] failed, isIOTimeoutFailure expect[%v] actual[%v]", true, ioTimeoutFailure)
	}
	err = fmt.Errorf("rand str abc op")
	ioTimeoutFailure = isIOTimeoutFailure(err)
	if ioTimeoutFailure {
		t.Errorf("action[TestIsIOTimeoutFailure] failed, isIOTimeoutFailure expect[%v] actual[%v]", false, ioTimeoutFailure)
	}
}

func TestGetRemoteExtentInfoForValidateCRCWithRetry(t *testing.T) {
	var (
		ctx         = context.Background()
		tcp         = mock.NewMockTcp(mockDataTcpPort1)
		err         error
		extentFiles []storage.ExtentInfoBlock
	)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	dp := &DataPartition{
		partitionID: 10,
	}
	mock.ReplyGetRemoteExtentInfoForValidateCRCCount = GetRemoteExtentInfoForValidateCRCRetryTimes
	targetHost := fmt.Sprintf(":%v", mockDataTcpPort1)
	if extentFiles, err = dp.getRemoteExtentInfoForValidateCRCWithRetry(ctx, targetHost); err == nil {
		t.Error("action[getRemoteExtentInfoForValidateCRCWithRetry] err should not be nil")
	}
	if extentFiles, err = dp.getRemoteExtentInfoForValidateCRCWithRetry(ctx, targetHost); err != nil {
		t.Errorf("action[getRemoteExtentInfoForValidateCRCWithRetry] err:%v", err)
	}
	if uint64(len(extentFiles)) != mock.LocalCreateExtentId {
		t.Errorf("action[getRemoteExtentInfoForValidateCRCWithRetry] extents length expect[%v] actual[%v]", mock.LocalCreateExtentId, len(extentFiles))
	}
}

func TestValidateCrc(t *testing.T) {
	var (
		err         error
		dp          *DataPartition
		count       uint64 = 100
		testBaseDir        = path.Join(os.TempDir(), t.Name())
		extents     []storage.ExtentInfoBlock
	)
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, count, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	storage.ValidateCrcInterval = 5
	time.Sleep(time.Second*time.Duration(storage.ValidateCrcInterval) + 1)
	defer func() {
		storage.ValidateCrcInterval = int64(20 * storage.RepairInterval)
	}()
	testCases := []struct {
		name  string
		class int
	}{
		{name: "getLocalExtentInfoForValidateCRC", class: 0},
		{name: "runValidateCRC", class: 1},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			switch testCase.class {
			case 0:
				if extents, err = dp.getLocalExtentInfoForValidateCRC(); err != nil {
					t.Errorf("action[getLocalExtentInfoForValidateCRC] err:%v", err)
				}
				if uint64(len(extents)) != count {
					t.Errorf("action[getLocalExtentInfoForValidateCRC] extents length expect[%v] actual[%v]", count, len(extents))
				}
				var dp2 *DataPartition
				if dp2, err = initDataPartition(testBaseDir, 2, false); err != nil {
					t.Errorf("init data partition err:%v", err)
				}
				if extents, err = dp2.getLocalExtentInfoForValidateCRC(); err == nil || err.Error() != "partition is loadding" {
					t.Errorf("action[getLocalExtentInfoForValidateCRC], err should not be equal to nil or err[%v]", err)
				}
			case 1:
				tcp := mock.NewMockTcp(mockDataTcpPort1)
				err = tcp.Start()
				if err != nil {
					t.Fatalf("start mock tcp server failed: %v", err)
				}
				defer tcp.Stop()
				mock.ReplyGetRemoteExtentInfoForValidateCRCCount = 0
				dp.runValidateCRC(context.Background())
			}
		})
	}
}
