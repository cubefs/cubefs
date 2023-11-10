package metanode

import (
    "github.com/cubefs/cubefs/proto"
    "testing"
)

func TestFileMerget_Success(t *testing.T) {
    mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./extent_test", ApplyMock)
    if mp == nil {
        t.Errorf("mock mp failed:%v", err)
        t.FailNow()
    }
    defer releaseMetaPartition(mp)

    reqCreateInode := &proto.CreateInodeRequest{
        PartitionID: 1,
        Gid:         0,
        Uid:         0,
        Mode:        470,
    }
    resp := &Packet{}

    mp.config.Cursor = 100

    err = mp.CreateInode(reqCreateInode, resp)
    if err != nil {
        t.Errorf("create indoe failed:%s\n", err.Error())
        return
    }

    reqBatchAdd := &proto.AppendExtentKeysRequest {
        VolName: "test",
        PartitionId: 1,
        Inode: 101,
        Extents:         []proto.ExtentKey{
            {FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 300, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 400, PartitionId: 4, ExtentId: 2, ExtentOffset: 0, Size: 20},
        },
    }

    err = mp.BatchExtentAppend(reqBatchAdd, resp)
    mergeReq := &proto.InodeMergeExtentsRequest{
        VolName: "test",
        PartitionId: 1,
        Inode: 101,
        OldExtents: []proto.ExtentKey{
            {FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 300, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 400, PartitionId: 4, ExtentId: 2, ExtentOffset: 0, Size: 20},
        },
        NewExtents: []proto.ExtentKey{
            {FileOffset: 0, PartitionId: 5, ExtentId: 1, ExtentOffset: 0, Size: 420},
        },
    }
    mp.FileMigMergeExtents(mergeReq, resp)

    inode, _ := mp.inodeTree.Get(101)
    if inode == nil {
        t.Errorf("get inode 101 failed, err:%s", err.Error())
        return
    }
    if inode.Extents.Len() != 1 {
        t.Errorf("inode 101 ek failed, expect len 1, but now:%d\n%v\n", inode.Extents.Len(), inode.Extents)
        return
    }
    inode.Extents.Range2(func(index int, ek proto.ExtentKey) bool {
        if index != 0 {
            t.Errorf("err ek, index:%d ek:%v\n", index, ek)
            return false
        }

        if ek.PartitionId != 5 || ek.ExtentId != 1 || ek.Size != 420 {
            t.Errorf("err ek, index:%d ek:%v\n", index, ek)
            return false
        }
        return true
    })

    return
}

func TestFileMerget_Failed(t *testing.T) {

}

func TestFileMerget_Dup(t *testing.T) {

}

func TestFileMerget_AlreadyCommit(t *testing.T) {
    mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./extent_test", ApplyMock)
    if mp == nil {
        t.Errorf("mock mp failed:%v", err)
        t.FailNow()
    }
    defer releaseMetaPartition(mp)

    reqCreateInode := &proto.CreateInodeRequest{
        PartitionID: 1,
        Gid:         0,
        Uid:         0,
        Mode:        470,
    }
    resp := &Packet{}

    mp.config.Cursor = 100

    err = mp.CreateInode(reqCreateInode, resp)
    if err != nil {
        t.Errorf("create indoe failed:%s\n", err.Error())
        return
    }

    reqBatchAdd := &proto.AppendExtentKeysRequest {
        VolName: "test",
        PartitionId: 1,
        Inode: 101,
        Extents:         []proto.ExtentKey{
            {FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 300, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 400, PartitionId: 4, ExtentId: 2, ExtentOffset: 0, Size: 20},
        },
    }

    err = mp.BatchExtentAppend(reqBatchAdd, resp)
    mergeReq := &proto.InodeMergeExtentsRequest{
        VolName: "test",
        PartitionId: 1,
        Inode: 101,
        OldExtents: []proto.ExtentKey{
            {FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 100, PartitionId: 2, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 200, PartitionId: 3, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 300, PartitionId: 4, ExtentId: 1, ExtentOffset: 0, Size: 100},
            {FileOffset: 400, PartitionId: 4, ExtentId: 2, ExtentOffset: 0, Size: 20},
        },
        NewExtents: []proto.ExtentKey{
            {FileOffset: 0, PartitionId: 5, ExtentId: 1, ExtentOffset: 0, Size: 420},
        },
    }
    mp.FileMigMergeExtents(mergeReq, resp)

    mp.FileMigMergeExtents(mergeReq, resp)

    if resp.ResultCode != proto.OpOk {
        t.Errorf("expect ok, but now:%d\n", resp.ResultCode)
    }
}