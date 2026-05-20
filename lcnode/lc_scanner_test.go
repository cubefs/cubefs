// Copyright 2023 The CubeFS Authors.
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

package lcnode

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/routinepool"
	"github.com/cubefs/cubefs/util/unboundedchan"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

type inodeGetErrMW struct {
	*MockMetaWrapper
}

func (m *inodeGetErrMW) InodeGet_ll(inode uint64, isAsync bool) (*proto.InodeInfo, error) {
	return nil, fmt.Errorf("inode get failed")
}

type inodeGetTrackingMW struct {
	*MockMetaWrapper
	inodeGetCalls []uint64
}

func (m *inodeGetTrackingMW) InodeGet_ll(inode uint64, isAsync bool) (*proto.InodeInfo, error) {
	m.inodeGetCalls = append(m.inodeGetCalls, inode)
	return m.MockMetaWrapper.InodeGet_ll(inode, isAsync)
}

type scanInodeByPoolMW struct {
	*MockMetaWrapper
	resp *proto.ScanInodeByPoolResponse
}

func (m *scanInodeByPoolMW) ScanInodeByPool(req *proto.ScanInodeByPoolRequest) (*proto.ScanInodeByPoolResponse, error) {
	return m.resp, nil
}

func TestLcScanner(t *testing.T) {
	// log.InitLog("", "", log.InfoLevel, nil, 0)
	lcScanRoutineNumPerTask = 1
	maxDirChanNum = 0
	scanCheckInterval = 1
	days1, days3 := 1, 3
	scanner := &LcScanner{
		ID:     "test_id",
		Volume: "test_vol",
		mw:     NewMockMetaWrapper(),
		lcnode: &LcNode{},
		transitionMgr: &TransitionMgr{
			volume:    "test_vol",
			ec:        NewMockExtentClient(),
			ecForW:    NewMockExtentClient(),
			ebsClient: NewMockEbsClient(),
		},
		adminTask: &proto.AdminTask{
			Response: &proto.LcNodeRuleTaskResponse{},
		},
		rule: &proto.Rule{
			Transitions: []*proto.Transition{
				{
					StorageClass: proto.OpTypeStorageClassHDD,
					Days:         &days1,
					FromPoolId:   proto.DefaultSSDPoolId,
					ToPoolId:     proto.DefaultHDDPoolId,
				},
			},
			Expiration: &proto.Expiration{
				Days: &days3,
			},
		},
		dirChan:     unboundedchan.NewUnboundedChan(10),
		fileChan:    make(chan interface{}),
		dirRPool:    routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		fileRPool:   routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		currentStat: &proto.LcNodeRuleTaskStatistics{},
		limiter:     rate.NewLimiter(defaultLcScanLimitPerSecond, defaultLcScanLimitBurst),
		now:         time.Now(),
		stopC:       make(chan bool),
	}
	err := scanner.Start()
	require.NoError(t, err)
	time.Sleep(time.Second * 5)
	require.Equal(t, true, scanner.DoneScanning())
	require.Equal(t, int64(4), scanner.currentStat.TotalFileScannedNum)
	require.Equal(t, int64(3), scanner.currentStat.TotalFileExpiredNum)
	require.Equal(t, int64(4), scanner.currentStat.TotalDirScannedNum)
	require.Equal(t, int64(1), scanner.currentStat.ExpiredDeleteNum)
	require.Equal(t, int64(2), scanner.currentStat.ExpiredMToHddNum)
	require.Equal(t, int64(2), scanner.currentStat.ExpiredMNum)
	require.Equal(t, int64(300), scanner.currentStat.ExpiredMToHddBytes)
	require.Equal(t, int64(300), scanner.currentStat.ExpiredMBytes)
	require.Equal(t, int64(0), scanner.currentStat.ExpiredSkipNum)
	require.Equal(t, int64(0), scanner.currentStat.ErrorDeleteNum)
	require.Equal(t, int64(0), scanner.currentStat.ErrorMToHddNum)
	require.Equal(t, int64(0), scanner.currentStat.ErrorReadDirNum)

	dentry := &proto.ScanDentry{
		Inode: 1,
	}
	scanner.rule = &proto.Rule{
		Filter: &proto.Filter{
			MinSize: 1024,
		},
	}
	scanner.handleFile(dentry)

	// expired(inode *proto.InodeInfo, now int64, days *int, date *time.Time)
	inode := &proto.InodeInfo{
		AccessTime: time.Now().Add(time.Second * -1),
		CreateTime: time.Now(),
	}
	days := 1
	now := time.Now().Unix() + 24*60*60 + 1
	res := expired(inode, now, &days, nil)
	require.True(t, res)

	inode.AccessTime = time.Now().Add(time.Second * -12)
	res = expired(inode, now, &days, nil)
	require.False(t, res)
}

func TestInodeExpiredNilInfoReturnsEmptyOp(t *testing.T) {
	scanner := &LcScanner{now: time.Now()}
	op := scanner.inodeExpired(nil, nil, nil, &proto.ScanDentry{Inode: 1})
	require.Empty(t, op)
}

type ebsUpdateExtentErrMW struct {
	*ebsInodeMW
}

func (m *ebsUpdateExtentErrMW) UpdateExtentKeyAfterMigration(inode uint64, storageType uint32,
	objExtentKeys []proto.ObjExtentKey, poolId uint8, leaseExpire uint64, delayDelMinute uint64, fullPath string,
) error {
	return fmt.Errorf("statusLeaseGenerationNotMatch: mock")
}

func TestHandleFileEbsMigrationUpdateExtentKeyError(t *testing.T) {
	lcScanRoutineNumPerTask = 1
	days := 1
	scanner := &LcScanner{
		ID:     "test_ebs_update_err",
		Volume: "test_vol",
		mw:     &ebsUpdateExtentErrMW{ebsInodeMW: &ebsInodeMW{MockMetaWrapper: NewMockMetaWrapper()}},
		lcnode: &LcNode{},
		transitionMgr: &TransitionMgr{
			volume:    "test_vol",
			ec:        NewMockExtentClient(),
			ecForW:    NewMockExtentClient(),
			ebsClient: NewMockEbsClient(),
		},
		adminTask: &proto.AdminTask{Response: &proto.LcNodeRuleTaskResponse{}},
		rule: &proto.Rule{Transitions: []*proto.Transition{{
			StorageClass: proto.OpTypeStorageClassEBS,
			Days:         &days,
			FromPoolId:   proto.DefaultSSDPoolId,
			ToPoolId:     proto.DefaultHDDPoolId,
		}}},
		currentStat: &proto.LcNodeRuleTaskStatistics{},
		limiter:     rate.NewLimiter(defaultLcScanLimitPerSecond, defaultLcScanLimitBurst),
		now:         time.Now(),
		stopC:       make(chan bool),
	}
	scanner.handleFile(&proto.ScanDentry{Inode: 99, Path: "/ebs-err", Type: 0})
}

func TestLcScannerInodeExpiredSetsLeaseExpire(t *testing.T) {
	scanner := &LcScanner{
		now: time.Now(),
	}

	info := &proto.InodeInfo{
		Inode:           1001,
		Size:            4096,
		StorageClass:    proto.StorageClass_Replica_SSD,
		LeaseExpireTime: 200,
		HasMigrationEk:  true,
	}
	dentry := &proto.ScanDentry{}

	op := scanner.inodeExpired(info, nil, nil, dentry)
	require.Equal(t, "", op)
	require.Equal(t, info.LeaseExpireTime, dentry.LeaseExpire)
	require.Equal(t, info.StorageClass, dentry.StorageClass)
	require.Equal(t, info.Size, dentry.Size)
	require.True(t, dentry.HasMek)
}

type ebsInodeMW struct {
	*MockMetaWrapper
}

func (m *ebsInodeMW) InodeGet_ll(inode uint64, isAsync bool) (*proto.InodeInfo, error) {
	if inode == 99 {
		return &proto.InodeInfo{
			Inode:           99,
			Size:            128,
			PoolId:          proto.DefaultSSDPoolId,
			StorageClass:    proto.StorageClass_Replica_SSD,
			AccessTime:      time.Now().AddDate(0, 0, -5),
			CreateTime:      time.Now().AddDate(0, 0, -10),
			LeaseExpireTime: 50,
		}, nil
	}
	return m.MockMetaWrapper.InodeGet_ll(inode, isAsync)
}

func TestLcScannerHandleFilePassesLeaseExpireToMetaWrapper(t *testing.T) {
	lcScanRoutineNumPerTask = 1
	maxDirChanNum = 0
	scanCheckInterval = 1
	days := 1
	mockMw := NewMockMetaWrapper()
	scanner := &LcScanner{
		ID:     "test_id_generation",
		Volume: "test_vol",
		mw:     mockMw,
		lcnode: &LcNode{},
		transitionMgr: &TransitionMgr{
			volume:    "test_vol",
			ec:        NewMockExtentClient(),
			ecForW:    NewMockExtentClient(),
			ebsClient: NewMockEbsClient(),
		},
		adminTask: &proto.AdminTask{
			Response: &proto.LcNodeRuleTaskResponse{},
		},
		rule: &proto.Rule{
			Transitions: []*proto.Transition{
				{
					StorageClass: proto.OpTypeStorageClassHDD,
					Days:         &days,
					FromPoolId:   proto.DefaultSSDPoolId,
					ToPoolId:     proto.DefaultHDDPoolId,
				},
			},
		},
		dirChan:     unboundedchan.NewUnboundedChan(10),
		fileChan:    make(chan interface{}),
		dirRPool:    routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		fileRPool:   routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		currentStat: &proto.LcNodeRuleTaskStatistics{},
		limiter:     rate.NewLimiter(defaultLcScanLimitPerSecond, defaultLcScanLimitBurst),
		now:         time.Now(),
		stopC:       make(chan bool),
	}

	scanner.handleFile(&proto.ScanDentry{
		ParentId: 1,
		Name:     "f1",
		Path:     "/f1",
		Inode:    1,
		Type:     0,
	})

	lastLeaseExpire, ok := mockMw.LastUpdateLeaseExpire()
	require.True(t, ok, "expected UpdateExtentKeyAfterMigration to be called")
	require.Equal(t, uint64(101), lastLeaseExpire)
}

func TestBatchGetFileInodeInfoBuildsScanDentryWithoutInodeInfo(t *testing.T) {
	scanner := &LcScanner{}
	parentID := uint64(100)
	dentries := []proto.Dentry{
		{Inode: 11, Name: "a", Type: uint32(0o644)},
		{Inode: 12, Name: "dir", Type: uint32(os.ModeDir)},
	}

	result := scanner.batchGetFileInodeInfo(parentID, dentries, "/parent")
	require.Len(t, result, 2)
	require.Equal(t, parentID, result[0].ParentId)
	require.Equal(t, uint64(11), result[0].Inode)
	require.Equal(t, "parent/a", result[0].Path)
	require.Nil(t, result[0].InodeInfo)
	require.Equal(t, "parent/dir", result[1].Path)
}

func TestHandleFileInodeGetErrorReturnsEarly(t *testing.T) {
	scanner := &LcScanner{
		mw:          &inodeGetErrMW{MockMetaWrapper: NewMockMetaWrapper()},
		rule:        &proto.Rule{},
		currentStat: &proto.LcNodeRuleTaskStatistics{},
		limiter:     rate.NewLimiter(defaultLcScanLimitPerSecond, defaultLcScanLimitBurst),
		stopC:       make(chan bool),
	}
	before := scanner.currentStat.TotalFileExpiredNum
	scanner.handleFile(&proto.ScanDentry{Inode: 99, Path: "/x"})
	require.Equal(t, before, scanner.currentStat.TotalFileExpiredNum)
}

func TestHandleFileAlwaysCallsInodeGet(t *testing.T) {
	lcScanRoutineNumPerTask = 1
	days := 1
	trackMw := &inodeGetTrackingMW{MockMetaWrapper: NewMockMetaWrapper()}
	scanner := &LcScanner{
		ID:     "test_handle_file_inode_get",
		Volume: "test_vol",
		mw:     trackMw,
		lcnode: &LcNode{},
		transitionMgr: &TransitionMgr{
			volume:    "test_vol",
			ec:        NewMockExtentClient(),
			ecForW:    NewMockExtentClient(),
			ebsClient: NewMockEbsClient(),
		},
		adminTask: &proto.AdminTask{Response: &proto.LcNodeRuleTaskResponse{}},
		rule: &proto.Rule{
			Transitions: []*proto.Transition{{
				StorageClass: proto.OpTypeStorageClassHDD,
				Days:         &days,
				FromPoolId:   proto.DefaultSSDPoolId,
				ToPoolId:     proto.DefaultHDDPoolId,
			}},
		},
		dirChan:     unboundedchan.NewUnboundedChan(10),
		fileChan:    make(chan interface{}),
		dirRPool:    routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		fileRPool:   routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		currentStat: &proto.LcNodeRuleTaskStatistics{},
		limiter:     rate.NewLimiter(defaultLcScanLimitPerSecond, defaultLcScanLimitBurst),
		now:         time.Now(),
		stopC:       make(chan bool),
	}

	scanner.handleFile(&proto.ScanDentry{
		ParentId: 1,
		Name:     "f1",
		Path:     "/f1",
		Inode:    1,
		Type:     0,
	})
	require.Equal(t, []uint64{1}, trackMw.inodeGetCalls)
}

func TestHandleFileEbsMigrationCallsUpdateExtentKey(t *testing.T) {
	lcScanRoutineNumPerTask = 1
	days := 1
	mockMw := &ebsInodeMW{MockMetaWrapper: NewMockMetaWrapper()}
	scanner := &LcScanner{
		ID:     "test_handle_file_ebs",
		Volume: "test_vol",
		mw:     mockMw,
		lcnode: &LcNode{},
		transitionMgr: &TransitionMgr{
			volume:    "test_vol",
			ec:        NewMockExtentClient(),
			ecForW:    NewMockExtentClient(),
			ebsClient: NewMockEbsClient(),
		},
		adminTask: &proto.AdminTask{Response: &proto.LcNodeRuleTaskResponse{}},
		rule: &proto.Rule{
			Transitions: []*proto.Transition{{
				StorageClass: proto.OpTypeStorageClassEBS,
				Days:         &days,
				FromPoolId:   proto.DefaultSSDPoolId,
				ToPoolId:     proto.DefaultHDDPoolId,
			}},
		},
		dirChan:     unboundedchan.NewUnboundedChan(10),
		fileChan:    make(chan interface{}),
		dirRPool:    routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		fileRPool:   routinepool.NewRoutinePool(lcScanRoutineNumPerTask),
		currentStat: &proto.LcNodeRuleTaskStatistics{},
		limiter:     rate.NewLimiter(defaultLcScanLimitPerSecond, defaultLcScanLimitBurst),
		now:         time.Now(),
		stopC:       make(chan bool),
	}

	scanner.handleFile(&proto.ScanDentry{
		ParentId: 1,
		Name:     "ebs-file",
		Path:     "/ebs-file",
		Inode:    99,
		Type:     0,
	})

	lastLeaseExpire, ok := mockMw.LastUpdateLeaseExpire()
	require.True(t, ok, "expected UpdateExtentKeyAfterMigration after EBS migration")
	require.Equal(t, uint64(50), lastLeaseExpire)
}

type pagingScanInodeByPoolMW struct {
	*MockMetaWrapper
	pages []*proto.ScanInodeByPoolResponse
	calls int
}

func (m *pagingScanInodeByPoolMW) ScanInodeByPool(req *proto.ScanInodeByPoolRequest) (*proto.ScanInodeByPoolResponse, error) {
	if m.calls >= len(m.pages) {
		return &proto.ScanInodeByPoolResponse{}, nil
	}
	resp := m.pages[m.calls]
	m.calls++
	return resp, nil
}

func TestScanInodesByMpAndPoolPagination(t *testing.T) {
	scanner := &LcScanner{
		mw: &pagingScanInodeByPoolMW{
			MockMetaWrapper: NewMockMetaWrapper(),
			pages: []*proto.ScanInodeByPoolResponse{
				{Inodes: []uint64{10}, TotalScanned: 1, HasMore: true, NextInode: 11},
				{Inodes: []uint64{11}, TotalScanned: 2, HasMore: false},
			},
		},
		fileChan:    make(chan interface{}, 4),
		stopC:       make(chan bool),
		currentStat: &proto.LcNodeRuleTaskStatistics{},
	}
	scanner.scanInodesByMpAndPool(6002, proto.DefaultSSDPoolId, 0)

	var got []uint64
	for len(got) < 2 {
		select {
		case v := <-scanner.fileChan:
			d, ok := v.(*proto.ScanDentry)
			require.True(t, ok)
			got = append(got, d.Inode)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for paginated scan dentry")
		}
	}
	require.ElementsMatch(t, []uint64{10, 11}, got)
}

func TestScanInodesByMpAndPoolStopsOnSignal(t *testing.T) {
	scanner := &LcScanner{
		mw: &scanInodeByPoolMW{
			MockMetaWrapper: NewMockMetaWrapper(),
			resp: &proto.ScanInodeByPoolResponse{
				Inodes:       []uint64{1, 2, 3},
				TotalScanned: 3,
			},
		},
		fileChan:    make(chan interface{}, 1),
		stopC:       make(chan bool),
		currentStat: &proto.LcNodeRuleTaskStatistics{},
	}
	close(scanner.stopC)
	scanner.scanInodesByMpAndPool(6003, proto.DefaultSSDPoolId, 0)
}

func TestScanInodesByMpAndPoolEnqueuesInodeOnly(t *testing.T) {
	mockMw := NewMockMetaWrapper()
	scanner := &LcScanner{
		mw: &scanInodeByPoolMW{
			MockMetaWrapper: mockMw,
			resp: &proto.ScanInodeByPoolResponse{
				Inodes:       []uint64{42, 43},
				TotalScanned: 2,
			},
		},
		fileChan:    make(chan interface{}, 4),
		stopC:       make(chan bool),
		currentStat: &proto.LcNodeRuleTaskStatistics{},
	}

	scanner.scanInodesByMpAndPool(6001, proto.DefaultSSDPoolId, 0)

	var got []uint64
	for len(got) < 2 {
		select {
		case v := <-scanner.fileChan:
			d, ok := v.(*proto.ScanDentry)
			require.True(t, ok)
			got = append(got, d.Inode)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for scan dentry")
		}
	}
	require.ElementsMatch(t, []uint64{42, 43}, got)
	for _, d := range got {
		require.NotZero(t, d)
	}
}
