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
	"os"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func newExtentStoreForOperatorTest(t *testing.T) (store *storage.ExtentStore) {
	path, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	store, err = storage.NewExtentStore(path, 0, 1*util.GB, proto.PartitionTypeNormal, true)
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
	d.limitFactor = make(map[uint32]*rate.Limiter, 0)
	d.limitFactor[proto.FlowReadType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxFLowLimit), proto.QosDefaultBurst)
	d.limitFactor[proto.FlowWriteType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxFLowLimit), proto.QosDefaultBurst)
	d.limitFactor[proto.IopsReadType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxIoLimit), defaultIOLimitBurst)
	d.limitFactor[proto.IopsWriteType] = rate.NewLimiter(rate.Limit(proto.QosDefaultDiskMaxIoLimit), defaultIOLimitBurst)
	d.limitRead = newIOLimiter(1*util.MB, 10)
	d.limitWrite = newIOLimiter(1*util.MB, 10)
	return
}

func newDpForOperatorTest(t *testing.T, dn *DataNode) (dp *DataPartition) {
	dp = &DataPartition{
		disk:        newDiskForOperatorTest(t, dn),
		extentStore: newExtentStoreForOperatorTest(t),
		config: &dataPartitionCfg{
			Forbidden: false,
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
