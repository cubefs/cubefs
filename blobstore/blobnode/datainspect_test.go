// Copyright 2026 The CubeFS Authors.
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

package blobnode

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	bnapi "github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/api/proxy"
	"github.com/cubefs/cubefs/blobstore/blobnode/core"
	"github.com/cubefs/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/taskswitch"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
)

func newDataInspectMgr(t *testing.T, conf DataInspectConf, svr *Service) *DataInspectMgr {
	t.Helper()
	ctr := gomock.NewController(t)

	getter := mocks.NewMockAccessor(ctr)
	getter.EXPECT().GetConfig(any, any).AnyTimes().Return("", nil)
	getter.EXPECT().SetConfig(any, any, any).AnyTimes().Return(nil)
	switchMgr := taskswitch.NewSwitchMgr(getter)

	mgr, err := NewDataInspectMgr(svr, conf, switchMgr)
	require.NoError(t, err)
	require.NotNil(t, mgr)

	// mocker inspect record
	recorder := mocks.NewMockRecordLogEncoder(ctr)
	mgr.recorder = recorder

	// unit tests have no real cluster manager/proxy; disable the lazy builder so
	// trySendCrcRepair gets a nil sender and returns early (svr.Conf may be nil).
	mgr.repairSender.build = nil

	return mgr
}

func TestTrySendCrcRepair(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	vuid0, _ := proto.NewVuid(100, 0, 1)
	vuid3, _ := proto.NewVuid(100, 3, 1)

	ds := NewMockDiskAPI(ctr)
	cs := NewMockChunkAPI(ctr)
	ds.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).AnyTimes()
	cs.EXPECT().Disk().Return(ds).AnyTimes()

	// sender nil: no-op, no proxy calls.
	mgr := &DataInspectMgr{}
	mgr.trySendCrcRepair(ctx, cs, []bnapi.BadShard{{Vuid: vuid0, Bid: 1, Err: errMock}})

	// sender set: only a crc-bad shard is sent; deleted and other errors skipped.
	var sent []*proxy.ShardRepairArgs
	sender := mocks.NewMockProxyLbRpcClient(ctr)
	sender.EXPECT().SendShardRepairMsg(any, any).Times(1).DoAndReturn(
		func(_ context.Context, args *proxy.ShardRepairArgs) error {
			sent = append(sent, args)
			return nil
		},
	)
	// use a fresh mgr: lazyRepairSender builds the sender at most once, so the
	// nil-build get() above must not poison this sender path
	mgr2 := &DataInspectMgr{}
	mgr2.repairSender.build = func() proxy.LbMsgSender {
		return sender
	}

	mgr2.trySendCrcRepair(ctx, cs, []bnapi.BadShard{
		{Vuid: vuid0, Bid: 1, Err: errMock},
		{Vuid: vuid3, Bid: 2, Err: crc32block.ErrMismatchedCrc},
	})
	require.Len(t, sent, 1)
	require.Equal(t, vuid3.Vid(), sent[0].Vid)
}

// TestRunInspectRoundCallsInspectDisk verifies each round drives InspectDisk
// (manager method) for every writable disk only.
func TestRunInspectRoundCallsInspectDisk(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	ds1 := NewMockDiskAPI(ctr)
	ds2 := NewMockDiskAPI(ctr)
	svr := &Service{
		Disks:   map[proto.DiskID]core.DiskAPI{11: ds1, 22: ds2},
		ctx:     ctx,
		closeCh: make(chan struct{}),
	}

	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 10, RateLimit: 1024 * 1024}, svr)
	mgr.taskSwitch = taskswitch.NewEnabledTaskSwitch()

	ds1.EXPECT().ID().Return(proto.DiskID(11)).AnyTimes()
	ds2.EXPECT().ID().Return(proto.DiskID(22)).AnyTimes()
	ds1.EXPECT().IsWritable().Return(true).AnyTimes()
	ds2.EXPECT().IsWritable().Return(false).AnyTimes() // non-writable disk skipped
	ds1.EXPECT().IsClosing().Return(false).AnyTimes()
	ds1.EXPECT().LoadInspectDiskState(any).Return(core.InspectDiskState{DiskID: 11, CycleStartAt: time.Now().UnixNano(), CycleID: 1}, nil).Times(1)
	ds1.EXPECT().ListChunks(any).Return(nil, nil).Times(1)
	ds1.EXPECT().FlushInspectState(any).AnyTimes()
	ds1.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{}).AnyTimes()
	ds1.EXPECT().RangeInspectChunkState(any, any).Return(nil).AnyTimes()

	mgr.runInspectRound(ctx)
}

// TestListInspectDiskStatesSkipsClosing verifies listInspectDiskStates skips
// closing disks and returns a map keyed by disk id with the persisted states of
// open disks only, consistent with /qos/stat/diskid/*.
func TestListInspectDiskStatesSkipsClosing(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	closing := NewMockDiskAPI(ctr)
	open := NewMockDiskAPI(ctr)
	svr := &Service{
		Disks: map[proto.DiskID]core.DiskAPI{2: closing, 3: open},
	}

	closing.EXPECT().ID().Return(proto.DiskID(2)).AnyTimes()
	closing.EXPECT().IsClosing().Return(true).Times(1)
	open.EXPECT().ID().Return(proto.DiskID(3)).AnyTimes()
	open.EXPECT().IsClosing().Return(false).Times(1)
	open.EXPECT().LoadInspectDiskState(any).Return(
		core.InspectDiskState{DiskID: 3, CycleStartAt: 987654321, CycleID: 8}, nil,
	).Times(1)

	states, err := svr.listInspectDiskStates(ctx)
	require.NoError(t, err)
	require.Equal(t, map[proto.DiskID]core.InspectDiskState{
		3: {DiskID: 3, CycleStartAt: 987654321, CycleID: 8},
	}, states)
}

// TestListInspectChunkStates verifies listInspectChunkStates returns a map keyed
// by vuid, consistent with /qos/stat/diskid/*.
func TestListInspectChunkStates(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()

	ds := NewMockDiskAPI(ctr)
	st1 := core.InspectChunkState{
		Vuid:         proto.Vuid(1003),
		Cursor:       200,
		CycleMaxBid:  300,
		CycleID:      9,
		CycleCnt:     12,
		CycleScanned: 5,
		BadBids:      map[proto.BlobID]struct{}{9: {}},
	}
	st2 := core.InspectChunkState{
		Vuid:         proto.Vuid(1002),
		Cursor:       301,
		CycleMaxBid:  301,
		CycleID:      9,
		CycleCnt:     13,
		CycleScanned: 6,
		BadBids:      map[proto.BlobID]struct{}{8: {}},
	}
	ds.EXPECT().RangeInspectChunkState(any, any).DoAndReturn(
		func(_ context.Context, fn func(st *core.InspectChunkState) bool) error {
			fn(&st1)
			fn(&st2)
			return nil
		},
	)

	states, err := (&Service{}).listInspectChunkStates(ctx, ds)
	require.NoError(t, err)
	require.Equal(t, map[proto.Vuid]InspectChunkStateInfo{
		1002: {Vuid: proto.Vuid(1002), Cursor: 301, CycleMaxBid: 301, Counted: true, CycleID: 9, CycleCnt: 13, CycleScanned: 6, BadBids: []proto.BlobID{8}},
		1003: {Vuid: proto.Vuid(1003), Cursor: 200, CycleMaxBid: 300, Counted: true, CycleID: 9, CycleCnt: 12, CycleScanned: 5, BadBids: []proto.BlobID{9}},
	}, states)
}

// TestLoopDataInspectExit verifies the manager main goroutine exits and closes the
// record log when the service close channel is signalled.
func TestLoopDataInspectExit(t *testing.T) {
	ctx := context.Background()
	svr := &Service{ctx: ctx, closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{IntervalSec: 10, RateLimit: 1024 * 1024}, svr)
	mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Close().Times(1)

	close(svr.closeCh)
	mgr.loopDataInspect()
}

func TestDataInspectMetric(t *testing.T) {
	ctx := context.Background()
	info := clustermgr.BlobNodeDiskInfo{DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 11}}

	svr := &Service{
		ctx:     ctx,
		closeCh: make(chan struct{}),
	}
	mgr := newDataInspectMgr(t, DataInspectConf{RateLimit: 1024 * 1024}, svr)

	beforeCnt := testCounterValue(dataInspectBadVec, dataInspectDiskLabelValues(info))
	mgr.AddInspectBadMetric(info, 5)
	afterCnt := testCounterValue(dataInspectBadVec, dataInspectDiskLabelValues(info))
	require.Equal(t, beforeCnt+5, afterCnt)
}

func TestDataInspectRecord(t *testing.T) {
	ctx := context.Background()
	svr := &Service{ctx: ctx, closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{RateLimit: 1024 * 1024}, svr)

	info := clustermgr.BlobNodeDiskInfo{DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 11}}
	mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Times(1)
	mgr.RecordBadBids(ctx, info, proto.Vuid(1001), []string{"1", "2"}, "bad shard")
}

func TestReportBadShard(t *testing.T) {
	ctr := gomock.NewController(t)
	ctx := context.Background()
	svr := &Service{ctx: ctx, closeCh: make(chan struct{})}
	mgr := newDataInspectMgr(t, DataInspectConf{RateLimit: 1024 * 1024}, svr)

	cs := NewMockChunkAPI(ctr)
	ds := NewMockDiskAPI(ctr)
	cs.EXPECT().Vuid().Return(proto.Vuid(1001)).AnyTimes()
	cs.EXPECT().Disk().Return(ds).AnyTimes()
	ds.EXPECT().DiskInfo().Return(clustermgr.BlobNodeDiskInfo{DiskHeartBeatInfo: clustermgr.DiskHeartBeatInfo{DiskID: 11}}).AnyTimes()

	// deleted shard errors are ignored
	mgr.reportBadShard(ctx, cs, 1, os.ErrNotExist)

	// normal error is recorded
	mgr.recorder.(*mocks.MockRecordLogEncoder).EXPECT().Encode(any).Times(1)
	mgr.reportBadShard(ctx, cs, 2, errMock)
}

func testCounterValue(vec *prometheus.CounterVec, labels []string) float64 {
	m := &dto.Metric{}
	_ = vec.WithLabelValues(labels...).Write(m)
	return m.GetCounter().GetValue()
}

func badBidSet(bids ...proto.BlobID) map[proto.BlobID]struct{} {
	set := make(map[proto.BlobID]struct{}, len(bids))
	for _, bid := range bids {
		set[bid] = struct{}{}
	}
	return set
}
