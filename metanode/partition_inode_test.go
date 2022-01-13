package metanode

import (
	"context"
	"github.com/chubaofs/chubaofs/proto"
	"testing"
	"time"
)

func TestResetCursor_WriteStatus(t *testing.T) {
	mp, _ := newTestMetapartition(1)

	req := &proto.CursorResetRequest{
		PartitionId: 1,
		Inode: 0,
		Force: true,
	}

	configTotalMem = 100 * GB
	status, _ := mp.calcMPStatus()
	cursor, err := mp.CursorReset(context.Background(), req)
	if cursor != mp.config.Cursor {
		t.Errorf("reset cursor test failed, err:%s", err.Error())
		return
	}
	configTotalMem = 0
	t.Logf("reset cursor:%d, status:%d, err:%v", cursor, status, err)

	releaseTestMetapartition(mp)
	return
}

func TestResetCursor_OutOfMaxEnd(t *testing.T) {
	mp, _ := newTestMetapartition(1)

	req := &proto.CursorResetRequest{
		PartitionId: 1,
		Inode: 10000,
		Force: true,
	}

	status, _ := mp.calcMPStatus()
	cursor, err := mp.CursorReset(context.Background(), req)
	if cursor != mp.config.Cursor {
		t.Errorf("reset cursor test failed, err:%s", err.Error())
		return
	}
	t.Logf("reset cursor:%d, status:%d, err:%v", cursor, status, err)

	for i := 1; i <100; i++ {
		mp.inodeTree.ReplaceOrInsert(NewInode(uint64(i), 0), false)
	}
	req.Inode = 90
	cursor, err = mp.CursorReset(context.Background(), req)
	if cursor != mp.config.Cursor {
		t.Errorf("reset cursor test failed, err:%s", err.Error())
		return
	}
	t.Logf("reset cursor:%d, status:%d, err:%v", cursor, status, err)
	releaseTestMetapartition(mp)
	return
}

func TestResetCursor_LimitedAndForce(t *testing.T) {
	mp, _ := newTestMetapartition(1)

	req := &proto.CursorResetRequest{
		PartitionId: 1,
		Inode: 9900,
		Force: false,
	}

	for i := 1; i <100; i++ {
		mp.inodeTree.ReplaceOrInsert(NewInode(uint64(i), 0), false)
	}

	status, _ := mp.calcMPStatus()
	cursor, err := mp.CursorReset(context.Background(), req)
	if cursor != mp.config.Cursor {
		t.Errorf("reset cursor test failed, err:%s", err.Error())
		return
	}
	t.Logf("reset cursor:%d, status:%d, err:%v", cursor, status, err)

	req.Force = true
	cursor, err = mp.CursorReset(context.Background(), req)
	if cursor != req.Inode {
		t.Errorf("reset cursor:%d test failed, err:%v", cursor, err)
		return
	}
	t.Logf("reset cursor:%d, status:%d, err:%v", cursor, status, err)

	releaseTestMetapartition(mp)
	return
}

func TestResetCursor_CursorChange(t *testing.T) {
	mp, _ := newTestMetapartition(1)

	req := &proto.CursorResetRequest{
		PartitionId: 1,
		Inode: 8000,
		Force: false,
	}

	for i := 1; i <100; i++ {
		mp.inodeTree.ReplaceOrInsert(NewInode(uint64(i), 0), false)
	}

	go func() {
		for i := 0; i < 100; i++{
			mp.nextInodeID()
		}
	}()
	time.Sleep(time.Microsecond * 10)
	cursor, err := mp.CursorReset(context.Background(), req)
	if cursor != mp.config.Cursor {
		t.Errorf("reset cursor test failed, err:%s", err.Error())
		return
	}
	t.Logf("reset cursor:%d, err:%v", cursor,  err)

	releaseTestMetapartition(mp)
	return
}

func TestResetCursor_LeaderChange(t *testing.T) {
	mp, _ := newTestMetapartition(1)

	req := &proto.CursorResetRequest{
		PartitionId: 1,
		Inode: 8000,
		Force: false,
	}

	for i := 1; i <100; i++ {
		mp.inodeTree.ReplaceOrInsert(NewInode(uint64(i), 0), false)
	}

	mp.config.NodeId = 2
	cursor, err := mp.CursorReset(context.Background(), req)
	if cursor != mp.config.Cursor {
		t.Errorf("reset cursor test failed, err:%s", err.Error())
		return
	}
	t.Logf("reset cursor:%d, err:%v", cursor,  err)

	releaseTestMetapartition(mp)
	return
}