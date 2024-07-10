package cluster

import (
	"context"
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

func TestWritableSpace(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()

	spaceInfo := &clustermgr.SpaceStatInfo{}
	idcBlobNodeStgs := make(map[string][]*nodeAllocator)
	for i := range testDiskMgr.IDC {
		for j := 0; j < 16; j++ {
			idcBlobNodeStgs[testDiskMgr.IDC[i]] = append(idcBlobNodeStgs[testDiskMgr.IDC[i]], &nodeAllocator{free: 100 * testDiskMgr.ChunkSize})
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
