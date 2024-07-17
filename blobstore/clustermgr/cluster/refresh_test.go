package cluster

import (
	"context"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/testing/mocks"
	"github.com/golang/mock/gomock"
)

func TestWritableSpace(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()

	spaceInfo := &clustermgr.SpaceStatInfo{}
	idcBlobNodeStgs := make(map[string][]*nodeAllocator)
	for i := range testDiskMgr.cfg.IDC {
		for j := 0; j < 16; j++ {
			idcBlobNodeStgs[testDiskMgr.cfg.IDC[i]] = append(idcBlobNodeStgs[testDiskMgr.cfg.IDC[i]], &nodeAllocator{free: 100 * testDiskMgr.cfg.ChunkSize})
		}
	}
	testDiskMgr.calculateWritable(idcBlobNodeStgs)
	t.Log("writable space: ", spaceInfo.WritableSpace)
}

func TestReadonlySpace(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	initTestDiskMgrNodes(t, testDiskMgr, 1, 1, testIdcs...)
	initTestDiskMgrDisksWithReadonly(t, testDiskMgr, 1, 4, testIdcs...)
	testDiskMgr.refresh(ctx)
}

func TestCheckDroppingNode(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()

	ctr := gomock.NewController(t)
	mockRaftServer := mocks.NewMockRaftServer(ctr)
	mockRaftServer.EXPECT().IsLeader().AnyTimes().Return(true)
	mockRaftServer.EXPECT().Propose(gomock.Any(), gomock.Any()).MaxTimes(1).Return(nil)
	testDiskMgr.SetRaftServer(mockRaftServer)

	_, ctx := trace.StartSpanFromContext(context.Background(), "")
	initTestDiskMgrNodes(t, testDiskMgr, 1, 1, testIdcs...)
	initTestDiskMgrDisksWithReadonly(t, testDiskMgr, 1, 4, testIdcs...)
	testDiskMgr.applyDroppingNode(ctx, 1, true)

	testDiskMgr.checkDroppingNode(ctx)
}
