package diskmgr

import (
	"testing"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
)

func TestWritableSpace(t *testing.T) {
	testDiskMgr, closeTestDiskMgr := initTestDiskMgr(t)
	defer closeTestDiskMgr()

	spaceInfo := &clustermgr.SpaceStatInfo{}
	idcBlobNodeStgs := make(map[string][]*blobNodeStorage)
	for i := range testDiskMgr.IDC {
		for j := 0; j < 16; j++ {
			idcBlobNodeStgs[testDiskMgr.IDC[i]] = append(idcBlobNodeStgs[testDiskMgr.IDC[i]], &blobNodeStorage{free: 100 * testDiskMgr.ChunkSize})
		}
	}
	testDiskMgr.calculateWritable(spaceInfo, idcBlobNodeStgs)
	t.Log("writable space: ", spaceInfo.WritableSpace)
}
