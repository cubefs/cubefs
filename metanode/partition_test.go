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
	manager := &metadataManager{nodeId: 1, metaNode: node}
	conf := &MetaPartitionConfig{ PartitionId: 1,
		NodeId: 1,
		Start: 1, End: math.MaxUint64 - 100,
		Peers: []proto.Peer{proto.Peer{ID: 1, Addr: "127.0.0.1"} },
		RootDir: "./partition_" + strconv.Itoa(int(pid))}
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

func genInode(t *testing.T, mp *metaPartition, cnt uint64) {

	testTarget := []byte{'1', '2', '3', '4', '1', '2', '3', '4'}
	for i := uint64(0); i < cnt;  {
		ino := NewInode(rand.Uint64(), 0)
		if ino.Inode % 997 == 0 {
			ino.LinkTarget = append(ino.LinkTarget, testTarget...)
			ino.Type = rand.Uint32()
			for j := 0; j < 10; j++ {
				ino.Extents.Append(context.Background(), proto.ExtentKey{FileOffset: uint64(j) * 1024 * 4,
					PartitionId: rand.Uint64(), ExtentId: rand.Uint64(),
					ExtentOffset: rand.Uint64(), Size: rand.Uint32(), CRC:0})
			}
		}
		if _, ok := mp.inodeTree.ReplaceOrInsert(ino, false); !ok {
			continue
		}
		i++
	}
}

func RandString(len int) string {
	bytes := make([]byte, len)
	for i := 0; i < len; i++ {
		b := rand.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func genDentry(t *testing.T, mp *metaPartition, cnt uint64) {
	for i := uint64(0); i < cnt;  {
		dentry := &Dentry{}
		dentry.ParentId = rand.Uint64()
		dentry.Inode = rand.Uint64()
		dentry.Type = rand.Uint32()
		dentry.Name = RandString(rand.Int() % 100 + 10)
		if _, ok := mp.dentryTree.ReplaceOrInsert(dentry, false); !ok {
			continue
		}
		i++
	}
}

func checkMPInodeAndDentry(t *testing.T, mp1, mp2 *metaPartition) {

	if mp1.inodeTree.Len() != mp2.inodeTree.Len() {
		t.Errorf("inode tree len is different [%d], [%d]",
			mp1.inodeTree.Len(), mp2.inodeTree.Len())
	}
	mp1.inodeTree.Ascend(func(item BtreeItem) bool {
		ino1 := item.(*Inode)
		item2 := mp2.inodeTree.Get(ino1)
		ino2 := item2.(*Inode)
		if !reflect.DeepEqual(ino1, ino2) {
			t.Errorf("Failed to test, error: res=[%v] expectRes=[%v]\n",ino1, ino2)
		}
		return true
	})

	if mp1.dentryTree.Len() != mp2.dentryTree.Len() {
		t.Errorf("dentry tree len is different [%d], [%d]",
			mp1.dentryTree.Len(), mp2.dentryTree.Len())
	}
	mp1.dentryTree.Ascend(func(item BtreeItem) bool {
		dentry1 := item.(*Dentry)
		item2 := mp2.dentryTree.Get(dentry1)
		dentry2 := item2.(*Dentry)
		if !reflect.DeepEqual(dentry1, dentry2) {
			t.Errorf("Failed to test, error: res=[%v] expectRes=[%v]\n",dentry1, dentry2)
		}
		return true
	})
}

func TestMetaPartition_Store(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	mp.marshalVersion = MetaPartitionMarshVersion1
	mp2.marshalVersion = MetaPartitionMarshVersion2

	genInode(t, mp, 1000000)
	genDentry(t, mp, 1000000)

	start := time.Now()
	mp.store(&storeMsg{
		command:       opFSMStoreTick,
		applyIndex:    mp.applyID,
		inodeTree:     mp.inodeTree,
		dentryTree:    mp.dentryTree,
		extendTree:    mp.extendTree,
		multipartTree: mp.multipartTree,
	})
	storeV1Cost := time.Since(start)

	start = time.Now()
	mp2.store(&storeMsg{
		command:       opFSMStoreTick,
		applyIndex:    mp.applyID,
		inodeTree:     mp.inodeTree,
		dentryTree:    mp.dentryTree,
		extendTree:    mp.extendTree,
		multipartTree: mp.multipartTree,
	})
	storeV2Cost := time.Since(start)

	t.Logf("Store %dW inodes and %dW dentry, V1 cost:%v, V2 cost:%v", mp.inodeTree.Len()/10000, mp.dentryTree.Len()/10000, storeV1Cost, storeV2Cost)
}

func TestMetaPartition_Load(t *testing.T) {
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	mp.marshalVersion = MetaPartitionMarshVersion2
	mp2.marshalVersion = MetaPartitionMarshVersion1

	start := time.Now()
	mp.load(context.Background())
	loadV2Cost := time.Since(start)

	start = time.Now()
	mp2.load(context.Background())
	loadV1Cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("Load %dW inodes and %dW dentry, V1 cost:%v, V2 cost:%v", mp.inodeTree.Len()/10000, mp.dentryTree.Len()/10000, loadV1Cost, loadV2Cost)
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
	mp.marshalVersion = MetaPartitionMarshVersion2
	mp2.marshalVersion = MetaPartitionMarshVersion1
	mp.load(context.Background())

	start := time.Now()
	snap, _ := newMetaItemIterator(mp)
	go dealChanel(mp2)
	mp2.ApplySnapshot(nil, snap)
	cost := time.Since(start)

	checkMPInodeAndDentry(t, mp, mp2)
	t.Logf("%dW inodes %dW dentry, V2 gen snap, V1 aplly snnap success cost:%v", mp.inodeTree.Len()/10000, mp.dentryTree.Len()/10000, cost)
}

func TestMetaPartition_ApplySnap(t *testing.T) {
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
	t.Logf("%dW inodes %dW dentry, V1 gen snap, V2 aplly snnap success cost:%v", mp.inodeTree.Len()/10000, mp.dentryTree.Len()/10000, cost)
}

func TestMetaPartition_Snap(t *testing.T) {
	mp, _ := newTestMetapartition(1)
	mp2, _ := newTestMetapartition(2)
	mp3, _ := newTestMetapartition(3)
	mp4, _ := newTestMetapartition(4)

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