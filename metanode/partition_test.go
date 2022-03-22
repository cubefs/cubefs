package metanode

import (
	"context"
	"fmt"
	"github.com/chubaofs/chubaofs/metanode/metamock"
	"github.com/chubaofs/chubaofs/proto"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func ApplyMock(elem interface{},command []byte, index uint64) (resp interface{}, err error) {
	mp := elem.(*metaPartition)
	resp, err = mp.Apply(command, index)
	return
}

func newTestMetapartition(pid uint32)(*metaPartition, error){
	node := &MetaNode{nodeId: 1}
	manager := &metadataManager{nodeId: 1, metaNode: node, rocksDBDirs: []string{"./"}}
	conf := &MetaPartitionConfig{
		PartitionId: uint64(pid),
		NodeId:      1,
		Start:       1,
		End:         math.MaxUint64 - 100,
		Peers:       []proto.Peer{proto.Peer{ID: 1, Addr: "127.0.0.1"}},
		RootDir:     "./partition_" + strconv.Itoa(int(pid)),
		StoreMode:   proto.StoreModeMem,
	}
	tmp, err := CreateMetaPartition(conf, manager)
	if  err != nil {
		fmt.Printf("create meta partition failed:%s\n", err.Error())
		return nil, err
	}
	mp := tmp.(*metaPartition)
	mp.raftPartition = &metamock.MockPartition{Id: 1, Mp: []interface{}{mp}, Apply: ApplyMock}
	mp.vol = NewVol()
	return mp, nil
}

func releaseTestMetapartition(mp *metaPartition) {
	close(mp.stopC)
	time.Sleep(time.Second)
	mp.db.CloseDb()
	mp.db.ReleaseRocksDb()
	os.RemoveAll(mp.config.RootDir)
}

const (
	count = 10000
)

func genInode(t *testing.T, mp *metaPartition, cnt uint64) uint64 {

	maxInode := uint64(0)
	testTarget := []byte{'1', '2', '3', '4', '1', '2', '3', '4'}
	for i := uint64(0); i < cnt;  {
		ino := NewInode(rand.Uint64() % uint64(1000000000) + 1, 0)
		if ino.Inode > maxInode {
			maxInode = ino.Inode
		}
		if ino.Inode % 997 == 0 {
			ino.LinkTarget = append(ino.LinkTarget, testTarget...)
			ino.Type = rand.Uint32()
			for j := 0; j < 10; j++ {
				ino.Extents.Append(context.Background(), proto.ExtentKey{FileOffset: uint64(j) * 1024 * 4,
					PartitionId: rand.Uint64(), ExtentId: rand.Uint64(),
					ExtentOffset: rand.Uint64(), Size: rand.Uint32(), CRC:0})
			}
		}
		if err := mp.inodeTree.Create(ino, false); err != nil {
			continue
		}
		i++
	}
	return maxInode
}

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func genDentry(t *testing.T, mp *metaPartition, cnt, maxInode uint64) {
	for i := uint64(0); i < cnt;  {
		dentry := &Dentry{}
		dentry.ParentId = rand.Uint64() % uint64(1000000000) + 1
		dentry.Inode = rand.Uint64() % uint64(1000000000) + 1
		if dentry.ParentId > maxInode {
			continue
		}
		dentry.Type = rand.Uint32()
		dentry.Name = RandString(rand.Int() % 100 + 10)
		if err := mp.dentryTree.Create(dentry, false); err != nil {
			continue
		}
		i++
	}
}

func checkMPInodeAndDentry(t *testing.T, mp1, mp2 *metaPartition) {

	if mp1.inodeTree.Count() != mp2.inodeTree.Count() || mp1.inodeTree.Count() != count {
		t.Errorf("inode tree len expect [%d] actual [mp1:%d], [mp2:%d]",
			count, mp1.inodeTree.Count(), mp2.inodeTree.Count())
	}
	_ = mp1.inodeTree.Range(nil, nil, func(v []byte) (bool, error) {
		ino1 := NewInode(0, 0)
		_ = ino1.Unmarshal(context.Background(), v)
		ino2, _ := mp2.inodeTree.Get(ino1.Inode)
		if !reflect.DeepEqual(ino1, ino2) {
			t.Errorf("Failed to test, error: res=[%v] expectRes=[%v]\n",ino1, ino2)
		}
		return true, nil
	})

	if mp1.dentryTree.Count() != mp2.dentryTree.Count() || mp2.dentryTree.Count() != count {
		t.Errorf("dentry tree len expect[%d] actual [mp1:%d], [mp2:%d]",
			count, mp1.dentryTree.Count(), mp2.dentryTree.Count())
	}
	mp1.dentryTree.Range(nil, nil, func(v []byte) (bool, error) {
		dentry1 := new(Dentry)
		_ = dentry1.Unmarshal(v)
		dentry2, _ := mp2.dentryTree.Get(dentry1.ParentId, dentry1.Name)
		if !reflect.DeepEqual(dentry1, dentry2) {
			t.Errorf("Failed to test, error: res=[%v] expectRes=[%v]\n",dentry1, dentry2)
		}
		return true, nil
	})
}

func TestMetaPartition_Store(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	os.RemoveAll("./partition_1")
	os.RemoveAll("./partition_2")
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	if mp == nil || mp2 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		mp.db.CloseDb()
		mp2.db.CloseDb()
	}()
	mp.marshalVersion = MetaPartitionMarshVersion1
	mp2.marshalVersion = MetaPartitionMarshVersion2

	maxInode := genInode(t, mp, count)
	if maxInode <= 0 {
		fmt.Printf("error max inode id:%v\n", maxInode)
		t.FailNow()
	}
	genDentry(t, mp, count, maxInode)

	start := time.Now()
	mp.store(&storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID,
		snap:       NewSnapshot(mp),
	})
	storeV1Cost := time.Since(start)

	start = time.Now()
	mp2.store(&storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID,
		snap:       NewSnapshot(mp),
	})
	storeV2Cost := time.Since(start)
	t.Logf("Store %dW inodes and %dW dentry, V1 cost:%v, V2 cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, storeV1Cost, storeV2Cost)
}

func TestMetaPartition_Load(t *testing.T) {
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	if mp == nil || mp2 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		mp.db.CloseDb()
		mp2.db.CloseDb()
	}()

	mp.marshalVersion = MetaPartitionMarshVersion2
	mp2.marshalVersion = MetaPartitionMarshVersion1

	start := time.Now()
	err := mp.load(context.Background())
	if err != nil {
		t.Errorf("load failed:%v\n", err)
		return
	}
	loadV2Cost := time.Since(start)

	start = time.Now()
	err = mp2.load(context.Background())
	if err != nil {
		t.Errorf("load failed:%v\n", err)
		return
	}
	loadV1Cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("Load %dW inodes and %dW dentry, V1 cost:%v, V2 cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, loadV1Cost, loadV2Cost)
}

func dealChanel(mp *metaPartition) {
	for {
		select {
		case <-mp.extReset:
			return
		}
	}
}

func TestMetaPartition_GenSnap(t *testing.T) {
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	if mp == nil || mp2 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		mp.db.CloseDb()
		mp2.db.CloseDb()
	}()
	mp.marshalVersion = MetaPartitionMarshVersion2
	mp2.marshalVersion = MetaPartitionMarshVersion1
	mp.load(context.Background())

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	go dealChanel(mp2)
	mp2.ApplySnapshot(nil, snap)
	cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("%dW inodes %dW dentry, V2 gen snap, V1 aplly snnap success cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, cost)
}

func TestMetaPartition_ApplySnap(t *testing.T) {
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	if mp == nil || mp2 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		mp.db.CloseDb()
		mp2.db.CloseDb()
	}()
	mp.marshalVersion = MetaPartitionMarshVersion1
	mp2.marshalVersion = MetaPartitionMarshVersion2

	mp.load(context.Background())

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	go dealChanel(mp2)
	mp2.ApplySnapshot(nil, snap)
	cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("%dW inodes %dW dentry, V1 gen snap, V2 aplly snnap success cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, cost)
}

func TestMetaPartition_ApplySnapV2(t *testing.T) {
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	mp.marshalVersion = MetaPartitionMarshVersion1
	mp2.marshalVersion = MetaPartitionMarshVersion2

	mp.load(context.Background())

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	go dealChanel(mp2)
	mp2.ApplySnapshot(nil, snap)
	cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("%dW inodes %dW dentry, V1 gen snap, V2 aplly snnap success cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, cost)
	mp.db.CloseDb()
	mp2.db.CloseDb()
}

func TestMetaPartition_Snap(t *testing.T) {
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	mp3, _ := newTestMetapartition(3)
	mp4, _ := newTestMetapartition(4)
	if mp == nil || mp2 == nil || mp3 == nil || mp4 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}

	defer func() {
		releaseTestMetapartition(mp)
		releaseTestMetapartition(mp2)
		releaseTestMetapartition(mp3)
		releaseTestMetapartition(mp4)
	}()
	mp.marshalVersion = MetaPartitionMarshVersion1
	mp3.marshalVersion = MetaPartitionMarshVersion1
	mp2.marshalVersion = MetaPartitionMarshVersion2
	mp4.marshalVersion = MetaPartitionMarshVersion2

	mp.load(context.Background())
	mp2.load(context.Background())

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	go dealChanel(mp3)
	mp3.ApplySnapshot(nil, snap)
	v1Cost := time.Since(start)

	start = time.Now()
	snap2, _ := newMetaItemIterator(mp)
	go dealChanel(mp4)
	mp4.ApplySnapshot(nil, snap2)
	v2Cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp4)
	checkMPInodeAndDentry(t, mp2, mp3)
	t.Logf("V1 gen snap, V1 aplly snnap success cost:%v", v1Cost)
	t.Logf("V2 gen snap, V2 aplly snnap success cost:%v", v2Cost)
}

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
		_ = mp.inodeTree.Create(NewInode(uint64(i), 0), false)
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
		_ = mp.inodeTree.Create(NewInode(uint64(i), 0), false)
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
		_ = mp.inodeTree.Create(NewInode(uint64(i), 0), false)
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
		_ = mp.inodeTree.Create(NewInode(uint64(i), 0), false)
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

func TestMetaPartition_CleanDir(t *testing.T) {
	os.RemoveAll("./partition_1")
	os.RemoveAll("./partition_2")
}
