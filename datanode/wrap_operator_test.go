// Copyright 2024 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package datanode

import (
	"encoding/json"
	"net"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/datanode/repl"
	"github.com/cubefs/cubefs/datanode/storage"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/atomicutil"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func newExtentStoreForOperatorTest(t *testing.T) (store *storage.ExtentStore) {
	path, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	store, err = storage.NewExtentStore(path, 0, 1*util.GB, proto.PartitionTypeNormal, 0, true)
	require.NoError(t, err)
	return
}

func newDiskForOperatorTest(t *testing.T, dn *DataNode) (d *Disk) {
	var _ interface{} = t
	d = &Disk{
		Status:    proto.ReadWrite,
		Total:     1 * util.TB,
		Available: 1 * util.TB,
		Used:      0,
		dataNode:  dn,
	}
	d.limitFactor = make(map[uint32]*rate.Limiter)
	d.limitFactor[proto.FlowReadType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxFLowLimit), proto.QosDefaultBurst)
	d.limitFactor[proto.FlowWriteType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxFLowLimit), proto.QosDefaultBurst)
	d.limitFactor[proto.IopsReadType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxIoLimit), defaultIOLimitBurst)
	d.limitFactor[proto.IopsWriteType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxIoLimit), defaultIOLimitBurst)
	d.limitFactor[proto.IopsDeleteType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxIoLimit), defaultIOLimitBurst)
	d.limitRead = util.NewIOLimiter(1*util.MB, 10)
	d.limitWrite = util.NewIOLimiter(1*util.MB, 10)
	d.limitAsyncRead = util.NewIOLimiter(1*util.MB, 10)
	d.limitDelete = util.NewIOLimiter(1*util.MB, 10)
	return
}

func newDpForOperatorTest(t *testing.T, dn *DataNode) (dp *DataPartition) {
	dp = &DataPartition{
		disk:        newDiskForOperatorTest(t, dn),
		extentStore: newExtentStoreForOperatorTest(t),
		config: &dataPartitionCfg{
			Forbidden:                false,
			ForbidWriteOpOfProtoVer0: false,
		},
		partitionSize: 1 * util.TB,
		dataNode:      dn,
	}
	return
}

func newPacketForOperatorTest(t *testing.T, dp *DataPartition, extentId uint64) (p *repl.Packet) {
	var _ interface{} = t
	p = &repl.Packet{
		Object: dp,
		Packet: proto.Packet{
			ExtentID: extentId,
		},
	}
	return
}

func newDataNodeForOperatorTest(t *testing.T) (dn *DataNode) {
	var _ interface{} = t
	dn = &DataNode{
		metrics: &DataNodeMetrics{
			dataNode: dn,
		},
		diskQosEnable:      true,
		diskAsyncQosEnable: true,
	}
	return
}

func TestSkipAppendWrite(t *testing.T) {
	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)
	extentId := uint64(1000)
	p := newPacketForOperatorTest(t, dp, extentId)

	dataStr := "HelloWorld"

	p.Opcode = proto.OpCreateExtent
	dn.handlePacketToCreateExtent(p)
	t.Logf("handle create extent, result code(%v)", p.ResultCode)
	require.EqualValues(t, proto.OpOk, p.ResultCode)

	p = newPacketForOperatorTest(t, dp, extentId)
	p.Opcode = proto.OpWrite
	p.Data = []byte(dataStr)
	p.ExtentOffset = int64(len(p.Data))
	p.Size = uint32(len(p.Data))
	dn.handleWritePacket(p)
	t.Logf("handle write packet, result code(%v)", p.ResultCode)
	require.EqualValues(t, proto.OpArgMismatchErr, p.ResultCode)
}

func newPacketForTest(task *proto.AdminTask) *repl.Packet {
	data, _ := json.Marshal(task)
	return &repl.Packet{
		Packet: proto.Packet{
			Data: data,
		},
	}
}

func TestMarkDeleteIopsLimit(t *testing.T) {
	var (
		wg sync.WaitGroup
		c  net.Conn
	)

	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)

	for i := 100; i < 900; i++ {
		p := newPacketForOperatorTest(t, dp, uint64(i))
		p.Opcode = proto.OpCreateExtent
		dn.handlePacketToCreateExtent(p)
		require.EqualValues(t, proto.OpOk, p.ResultCode)
	}

	dp.dataNode.diskAsyncQosEnable = true
	dp.disk.limitFactor[proto.IopsDeleteType].SetLimit(rate.Limit(200))
	startTime := time.Now()
	for i := 100; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := newPacketForOperatorTest(t, dp, uint64(i))
			p.Opcode = proto.OpMarkDelete
			p.ExtentType = 1
			dn.handleMarkDeletePacket(p, c)
			require.EqualValues(t, proto.OpOk, p.ResultCode)
		}(i)
	}
	wg.Wait()
	costTime1 := time.Since(startTime)
	t.Logf("cost time1(%v)", costTime1)

	time.Sleep(time.Second)

	dp.disk.limitFactor[proto.IopsDeleteType].SetLimit(rate.Limit(50))
	startTime = time.Now()
	for i := 500; i < 900; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := newPacketForOperatorTest(t, dp, uint64(i))
			p.Opcode = proto.OpMarkDelete
			p.ExtentType = 1
			dn.handleMarkDeletePacket(p, c)
			require.EqualValues(t, proto.OpOk, p.ResultCode)
		}(i)
	}
	wg.Wait()
	costTime2 := time.Since(startTime)
	t.Logf("cost time2(%v)", costTime2)

	require.Greater(t, costTime2, costTime1)
}

func TestMarkDeleteIoccLimit(t *testing.T) {
	var (
		wg sync.WaitGroup
		c  net.Conn
	)

	dn := newDataNodeForOperatorTest(t)
	dp := newDpForOperatorTest(t, dn)

	for i := 100; i < 700; i++ {
		p := newPacketForOperatorTest(t, dp, uint64(i))
		p.Opcode = proto.OpCreateExtent
		dn.handlePacketToCreateExtent(p)
		require.EqualValues(t, proto.OpOk, p.ResultCode)
	}

	dp.disk.limitFactor[proto.IopsDeleteType].SetLimit(rate.Limit(100))
	startTime := time.Now()
	for i := 100; i < 400; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := newPacketForOperatorTest(t, dp, uint64(i))
			p.Opcode = proto.OpMarkDelete
			p.ExtentType = 1
			dn.handleMarkDeletePacket(p, c)
		}(i)
	}
	wg.Wait()

	costTime1 := time.Since(startTime)
	t.Logf("cost time1(%v)", costTime1)

	dp.disk.limitDelete.ResetIO(2, 0)
	startTime = time.Now()
	for i := 400; i < 700; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			p := newPacketForOperatorTest(t, dp, uint64(i))
			p.Opcode = proto.OpMarkDelete
			p.ExtentType = 1
			dn.handleMarkDeletePacket(p, c)
			require.EqualValues(t, proto.OpOk, p.ResultCode)
		}(i)
	}
	wg.Wait()
	costTime2 := time.Since(startTime)
	t.Logf("cost time2(%v)", costTime2)

	require.Greater(t, costTime2, costTime1)
}

func TestDeleteLostDisk(t *testing.T) {
	dn := &DataNode{
		space: &SpaceManager{
			disks:     make(map[string]*Disk),
			diskList:  []string{},
			diskUtils: make(map[string]*atomicutil.Float64),
		},
	}

	testDiskPath1 := "/test/disk1"
	testDiskPath2 := "/test/disk2"

	lostDisk1 := NewLostDisk(
		testDiskPath1,
		1*util.TB,
		0,
		3,
		dn.space,
		true,
	)
	lostDisk2 := NewLostDisk(
		testDiskPath2,
		1*util.TB,
		0,
		3,
		dn.space,
		true,
	)
	dn.space.putDisk(lostDisk1)
	dn.space.putDisk(lostDisk2)

	t.Run("normal delete disk", func(t *testing.T) {
		req := &proto.DeleteLostDiskRequest{DiskPath: testDiskPath1}
		task := &proto.AdminTask{
			OpCode:  proto.OpDeleteLostDisk,
			Request: req,
		}
		p := newPacketForTest(task)
		dn.handlePacketToDeleteLostDisk(p)
		require.Equal(t, proto.OpOk, p.ResultCode)
		_, err := dn.space.GetDisk(testDiskPath1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not exist")
	})

	t.Run("delete unexist disk", func(t *testing.T) {
		invalidReq := &proto.DeleteLostDiskRequest{DiskPath: "/invalid/path"}
		task := &proto.AdminTask{
			OpCode:  proto.OpDeleteLostDisk,
			Request: invalidReq,
		}
		p := newPacketForTest(task)
		dn.handlePacketToDeleteLostDisk(p)
		require.Equal(t, proto.OpIntraGroupNetErr, p.ResultCode)
		require.Contains(t, string(p.Data), "not exist")
	})

	t.Run("delete unlost disk", func(t *testing.T) {
		lostDisk2.isLost = false
		invalidReq := &proto.DeleteLostDiskRequest{DiskPath: testDiskPath2}
		task := &proto.AdminTask{
			OpCode:  proto.OpDeleteLostDisk,
			Request: invalidReq,
		}
		p := newPacketForTest(task)
		dn.handlePacketToDeleteLostDisk(p)
		require.Equal(t, proto.OpIntraGroupNetErr, p.ResultCode)
		t.Logf("%v", string(p.Data))
		require.Contains(t, string(p.Data), "not lost")
	})
}

func TestReloadDisk(t *testing.T) {
	tmpDir, err := os.MkdirTemp(".", "")
	defer os.RemoveAll(tmpDir)
	require.NoError(t, err)

	dn := &DataNode{
		diskReadFlow:      1 * util.MB,
		diskAsyncReadFlow: 1 * util.MB,
		diskWriteFlow:     1 * util.MB,
		diskReadIocc:      10,
		diskAsyncReadIocc: 10,
		diskWriteIocc:     10,
	}
	sm := &SpaceManager{
		disks:     make(map[string]*Disk),
		dataNode:  dn,
		diskList:  []string{},
		diskUtils: make(map[string]*atomicutil.Float64),
	}
	dn.space = sm

	testDiskPath := path.Join(tmpDir, "disk1")
	err = os.Mkdir(testDiskPath, 0o755)
	require.NoError(t, err)

	disk := NewLostDisk(
		testDiskPath,
		1*util.TB,
		0,
		3,
		dn.space,
		true,
	)
	dn.space.putDisk(disk)

	t.Run("normal reload disk", func(t *testing.T) {
		req := &proto.ReloadDiskRequest{DiskPath: testDiskPath}
		task := &proto.AdminTask{
			OpCode:  proto.OpReloadDisk,
			Request: req,
		}
		p := newPacketForTest(task)
		dn.handlePacketToReloadDisk(p)
		require.Equal(t, proto.OpOk, p.ResultCode)
		require.Eventually(t, func() bool {
			disk, _ := dn.space.GetDisk(testDiskPath)
			return disk != nil && !disk.isLost
		}, 3*time.Second, 100*time.Millisecond, "disk not loaded")
	})

	t.Run("reload unexist disk", func(t *testing.T) {
		req := &proto.ReloadDiskRequest{DiskPath: "/invalid/path"}
		task := &proto.AdminTask{
			OpCode:  proto.OpReloadDisk,
			Request: req,
		}
		p := newPacketForTest(task)
		dn.handlePacketToReloadDisk(p)
		require.Equal(t, proto.OpIntraGroupNetErr, p.ResultCode)
		require.Contains(t, string(p.Data), "not exist")
	})

	t.Run("reload unlost disk", func(t *testing.T) {
		disk.isLost = false
		req := &proto.ReloadDiskRequest{DiskPath: testDiskPath}
		task := &proto.AdminTask{
			OpCode:  proto.OpReloadDisk,
			Request: req,
		}
		p := newPacketForTest(task)
		dn.handlePacketToReloadDisk(p)
		require.Equal(t, proto.OpIntraGroupNetErr, p.ResultCode)
		require.Contains(t, string(p.Data), "not lost")
	})
}
