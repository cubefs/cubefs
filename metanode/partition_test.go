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
	"testing"
	"time"
)

func ApplyMock(elem interface{},command []byte, index uint64) (resp interface{}, err error) {
	mp := elem.(*metaPartition)
	resp, err = mp.Apply(command, index)
	return
}

func mockMetaPartition(partitionID uint64, metaNodeID uint64, storeMode proto.StoreMode, rootDir string, applyFunc metamock.ApplyFunc) (*metaPartition, error) {
	_ = os.RemoveAll(rootDir)
	_ = os.MkdirAll(rootDir, 0666)
	node := &MetaNode{nodeId: metaNodeID, metadataDir: rootDir}
	manager := &metadataManager{nodeId: metaNodeID, rocksDBDirs: []string{rootDir}, metaNode: node}
	conf := &MetaPartitionConfig{
		RocksDBDir:  rootDir,
		PartitionId: partitionID,
		NodeId:      metaNodeID,
		Start:       1,
		End:         math.MaxUint64 - 100,
		Peers:       []proto.Peer{{ID: metaNodeID, Addr: "127.0.0.1"}},
		RootDir:     rootDir,
		StoreMode:   storeMode,
	}
	conf.VirtualMPs = append(conf.VirtualMPs, VirtualMetaPartitionConf{Start: conf.Start, End: conf.End, ID: partitionID})
	tmp, err := CreateMetaPartition(conf, manager)
	if err != nil {
		fmt.Printf("create meta partition failed:%s", err.Error())
		return nil, err
	}
	mp := tmp.(*metaPartition)
	mp.raftPartition = &metamock.MockPartition{Id: partitionID, Mp: []interface{}{mp}, Apply: applyFunc}
	mp.vol = NewVol()
	go metaPartitionSchedule(mp)
	return mp, nil
}

func metaPartitionSchedule(mp *metaPartition) {
	for{
		select{
		case sMsg := <- mp.storeChan:
			sMsg.snap.Close()
		case <- mp.extReset:
		case <- mp.stopC:
			return
		}
	}
}

func releaseMetaPartition(mp *metaPartition) {
	close(mp.stopC)
	time.Sleep(time.Second)
	_ = mp.db.CloseDb()
	_ = mp.db.ReleaseRocksDb()
	_ = os.RemoveAll(mp.config.RootDir)
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
					ExtentOffset: rand.Uint64(), Size: rand.Uint32(), CRC:0}, ino.Inode)
			}
		}
		if _, ok, err := inodeCreate(mp.inodeTree, ino, false); err != nil || !ok {
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
		if _, ok, err := dentryCreate(mp.dentryTree, dentry, false); err != nil || !ok {
			continue
		}
		i++
	}
}

func checkMPInodeAndDentry(t *testing.T, mp1, mp2 *metaPartition) {

	if mp1.inodeTree.Count() != mp2.inodeTree.Count() || mp1.inodeTree.Count() != count {
		t.Errorf("inode tree len expect [%d] actual [mp1:%d], [mp2:%d]",
			count, mp1.inodeTree.Count(), mp2.inodeTree.Count())
		t.FailNow()
	}
	_ = mp1.inodeTree.Range(nil, nil, func(ino1 *Inode) (bool, error) {
		ino2, _ := mp2.inodeTree.Get(ino1.Inode)
		if !reflect.DeepEqual(ino1, ino2) {
			t.Errorf("Failed to test, error:\n res=\n[%v]\n expectRes=\n[%v]\n",ino1, ino2)
		}
		return true, nil
	})

	if mp1.dentryTree.Count() != mp2.dentryTree.Count() || mp2.dentryTree.Count() != count {
		t.Errorf("dentry tree len expect[%d] actual [mp1:%d], [mp2:%d]",
			count, mp1.dentryTree.Count(), mp2.dentryTree.Count())
		t.FailNow()
	}
	mp1.dentryTree.Range(nil, nil, func(dentry1 *Dentry) (bool, error) {
		dentry2, _ := mp2.dentryTree.Get(dentry1.ParentId, dentry1.Name)
		if !reflect.DeepEqual(dentry1, dentry2) {
			t.Errorf("Failed to test, error:\n res=\n[%v]\n expectRes=\n[%v]\n",dentry1, dentry2)
			t.FailNow()
		}
		return true, nil
	})
}

func TestMetaPartition_StoreAndLoad(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	mp, _ := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMock)
	mp2, _ := mockMetaPartition(2, 1, proto.StoreModeMem, "./partition_2", ApplyMock)
	if mp == nil || mp2 == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}
	defer func() {
		releaseMetaPartition(mp)
		releaseMetaPartition(mp2)
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

	mp.marshalVersion = MetaPartitionMarshVersion2
	mp2.marshalVersion = MetaPartitionMarshVersion1

	start = time.Now()
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
//
//func TestMetaPartition_Load(t *testing.T) {
//	mp, _ := newTestMetapartition(1)
//	mp2, _ := newTestMetapartition(2)
//	if mp == nil || mp2 == nil {
//		fmt.Printf("new mock meta partition failed\n")
//		t.FailNow()
//	}
//	defer func() {
//		mp.db.CloseDb()
//		mp2.db.CloseDb()
//	}()
//
//	mp.marshalVersion = MetaPartitionMarshVersion2
//	mp2.marshalVersion = MetaPartitionMarshVersion1
//
//	start := time.Now()
//	err := mp.load(context.Background())
//	if err != nil {
//		t.Errorf("load failed:%v\n", err)
//		return
//	}
//	loadV2Cost := time.Since(start)
//
//	start = time.Now()
//	err = mp2.load(context.Background())
//	if err != nil {
//		t.Errorf("load failed:%v\n", err)
//		return
//	}
//	loadV1Cost := time.Since(start)
//
//	checkMPInodeAndDentry(t, mp, mp2)
//	t.Logf("Load %dW inodes and %dW dentry, V1 cost:%v, V2 cost:%v", mp.inodeTree.Count()/10000, mp.dentryTree.Count()/10000, loadV1Cost, loadV2Cost)
//}
//
//func TestMetaPartition_CleanDir(t *testing.T) {
//	os.RemoveAll("./partition_1")
//	os.RemoveAll("./partition_2")
//}

func Test_nextInodeID(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_next_inode_id", ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock metapartition failed:%v", err)
		return
	}
	defer releaseMetaPartition(mp)
	mp.config.VirtualMPs = []VirtualMetaPartitionConf{
		{
			Start: 0,
			End:   1024,
			ID:    1,
		},
		{
			Start: 10240,
			End:   defaultMaxMetaPartitionInodeID,
			ID:    10,
		},
	}
	mp.virtualMPs = InitVirtualMetaPartitionByConf(mp.config.VirtualMPs, true)
	mp.config.Cursor = 10240
	if _, err = mp.nextInodeID(1); err == nil {
		t.Errorf("nextInodeID test failed, error expect :inode ID out of range; actual: nil")
		return
	}

	if _, err = mp.nextInodeID(8); err == nil {
		t.Errorf("nextInodeID test failed, error expect :inode ID out of range; actual: nil")
		return
	}
}

func TestMetaPartition_updateVirtualMPByConf(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_next_inode_id", ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock metapartition failed:%v", err)
		return
	}
	defer releaseMetaPartition(mp)
	mp.config.VirtualMPs = []VirtualMetaPartitionConf{
		{
			Start: 0,
			End:   1024,
			ID:    1,
		},
	}
	mp.virtualMPs = []*VirtualMetaPartition{
		{
			VirtualMetaPartitionConf: VirtualMetaPartitionConf{ID: 1, Start: 0, End: 1024},
			Status:                   proto.ReadWrite,
			InodeIDAlloter:           nil,
		},
		{
			VirtualMetaPartitionConf: VirtualMetaPartitionConf{ID: 10, Start: 2048, End: proto.DefaultMetaPartitionInodeIDStep},
			Status:                   proto.ReadWrite,
			InodeIDAlloter:           nil,
		},
	}

	mp.updateVirtualMetaPartitionsByConf()
	if len(mp.virtualMPs) != len(mp.config.VirtualMPs) {
		t.Errorf("error virtual mp count:%v", len(mp.virtualMPs))
		return
	}
	for index, vMP := range mp.virtualMPs {
		t.Logf("conf:%v, virtual mp:(id:%v, start:%v, end:%v)", mp.config.VirtualMPs[index], vMP.ID, vMP.Start, vMP.End)
	}

	mp.config.VirtualMPs = []VirtualMetaPartitionConf{
		{
			Start: 0,
			End:   1024,
			ID:    1,
		},
		{
			Start: 10240,
			End:   defaultMaxMetaPartitionInodeID,
			ID:    10,
		},
	}

	mp.virtualMPs = []*VirtualMetaPartition{
		{
			VirtualMetaPartitionConf: VirtualMetaPartitionConf{ID: 1, Start: 0, End: 1024},
			Status:                   proto.ReadWrite,
			InodeIDAlloter:           NewInoAllocatorV1(0, 1024),
		},
	}
	inodeAllocator := mp.virtualMPs[0].InodeIDAlloter
	inodeAllocator.SetStatus(allocatorStatusInit)
	for index := inodeAllocator.Start; index <= inodeAllocator.Start + 1024; index++ {
		if index%10 == 5 {
			inodeAllocator.SetId(index)
		}
	}
	cnt := inodeAllocator.GetUsed()
	mp.updateVirtualMetaPartitionsByConf()
	if len(mp.virtualMPs) != len(mp.config.VirtualMPs) {
		t.Errorf("error virtual mp count:%v", len(mp.virtualMPs))
		return
	}
	inodeAllocator = mp.virtualMPs[0].InodeIDAlloter
	if inodeAllocator.GetUsed() != cnt {
		t.Errorf("inode allocator used count error, expect:%v, actual:%v", cnt, inodeAllocator.GetUsed())
	}

	inodeAllocator = mp.virtualMPs[1].InodeIDAlloter
	inodeAllocator.SetStatus(allocatorStatusInit)
	for index := inodeAllocator.Start; index <= inodeAllocator.Start + 1024; index++ {
		if index%10 == 6 {
			inodeAllocator.SetId(index)
		}
	}
	cnt = inodeAllocator.GetUsed()
	mp.updateVirtualMetaPartitionsByConf()
	if len(mp.virtualMPs) != len(mp.config.VirtualMPs) {
		t.Errorf("error virtual mp count:%v", len(mp.virtualMPs))
		return
	}
	inodeAllocator = mp.virtualMPs[1].InodeIDAlloter
	if inodeAllocator.GetUsed() != cnt {
		t.Errorf("inode allocator used count error, expect:%v, actual:%v", cnt, inodeAllocator.GetUsed())
	}
}
