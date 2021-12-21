package metanode

import (
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/metanode/metamock"
	"github.com/chubaofs/chubaofs/proto"
	"os"
	"testing"
	"time"
)


func ApplyMock(elem interface{},command []byte, index uint64) (resp interface{}, err error) {
	mp := elem.(*metaPartition)
	resp, err = mp.Apply(command, index)
	return
}

func newTestMetapartition()(*metaPartition, error){
	node := &MetaNode{nodeId: 1}
	manager := &metadataManager{nodeId: 1, metaNode: node}
	conf := &MetaPartitionConfig{ PartitionId: 1,
		NodeId: 1,
		Start: 1, End: 10000,
		Peers: []proto.Peer{proto.Peer{ID: 1, Addr: "127.0.0.1"} },
		RootDir: "./partition_1"}
	tmp, err := CreateMetaPartition(conf, manager)
	if  err != nil {
		fmt.Printf("create meta partition failed:%s", err.Error())
		return nil, err
	}
	mp := tmp.(*metaPartition)
	mp.raftPartition = &metamock.MockPartition{Id: 1, Mp: mp, Apply: ApplyMock}
	mp.vol = NewVol()
	return mp, nil
}

func releaseTestMetapartition(mp *metaPartition) {
	close(mp.stopC)
	time.Sleep(time.Second)
	os.RemoveAll(mp.config.RootDir)
}

func TestResetCursor_WriteStatus(t *testing.T) {
	mp, _ := newTestMetapartition()

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
	mp, _ := newTestMetapartition()

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
	mp, _ := newTestMetapartition()

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
	mp, _ := newTestMetapartition()

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
	mp, _ := newTestMetapartition()

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