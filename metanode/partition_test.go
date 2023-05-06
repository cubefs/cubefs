package metanode

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/metanode/metamock"
	"github.com/cubefs/cubefs/proto"
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

	if mp1.reqRecords == nil {
		return
	}

	if mp1.reqRecords.Count() != mp2.reqRecords.Count(){
		t.Errorf("req records count different [mp1:%d], [mp2:%d]",
			mp1.reqRecords.Count(), mp2.reqRecords.Count())
		t.FailNow()
	}
	mp1.reqRecords.reqTree.Ascend(func(i BtreeItem) bool {
		if item := mp1.reqRecords.reqTree.Get(i); item == nil {
			t.Errorf("req records test failed, reqInfo mp1(%v), mp2(nil)", i.(*RequestInfo))
			t.FailNow()
		}
		return true
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
	mp.reqRecords = InitRequestRecords(genBatchRequestInfo(128, false))

	start := time.Now()
	mp.store(&storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID,
		snap:       NewSnapshot(mp),
		reqTree:    mp.reqRecords.ReqBTreeSnap(),
	})
	storeV1Cost := time.Since(start)

	start = time.Now()
	mp2.store(&storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID,
		snap:       NewSnapshot(mp),
		reqTree:    mp.reqRecords.ReqBTreeSnap(),
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

//todo: add test case
func Test_nextInodeID(t *testing.T) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_next_inode_id", ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock metapartition failed:%v", err)
		return
	}
	defer releaseMetaPartition(mp)

}

func TestMetaPartition_storeAndLoadReqInfoInRocksDBStoreMode(t *testing.T) {
	mp, _ := mockMetaPartition(1, 1, proto.StoreModeRocksDb, "./test_reqRecordStore", ApplyMock)
	defer releaseMetaPartition(mp)
	mp.reqRecords = NewRequestRecords()
	dbWriteHandle, err := mp.db.CreateBatchHandler()

	for index:= 0; index < 128; index++ {
		reqInfo := genRequestInfo(true)
		if _, isDup := mp.reqRecords.IsDupReq(reqInfo); isDup {
			continue
		}
		mp.reqRecords.Update(reqInfo)
		mp.persistRequestInfoToRocksDB(dbWriteHandle, reqInfo)
		time.Sleep(time.Millisecond * 100)
	}

	_ = mp.db.CommitBatchAndRelease(dbWriteHandle)
	mp.db.CloseDb()

	mp2 := new(metaPartition)
	mp2.config = newDefaultMpConfig(1, 1, 0, math.MaxInt64, proto.StoreModeRocksDb)
	mp2.config.RootDir = "./test_reqRecordStore"
	mp2.config.RocksDBDir = "./test_reqRecordStore"
	mp2.config.PartitionId = 1
	mp2.db = NewRocksDb()
	if err = mp2.db.OpenDb(mp.getRocksDbRootDir(), mp.config.RocksWalFileSize, mp.config.RocksWalMemSize,
		mp.config.RocksLogFileSize, mp.config.RocksLogReversedTime, mp.config.RocksLogReVersedCnt, mp.config.RocksWalTTL); err != nil {
		t.Errorf("open db failed, dir:%v, error:%v", mp.getRocksDbRootDir(), err)
		return
	}
	defer mp2.db.CloseDb()


	if err = mp2.loadRequestRecordsInRocksDB(); err != nil {
		t.Errorf("load requset records in rocksDB failed:%v", err)
		return
	}

	if mp.reqRecords.Count() != mp2.reqRecords.Count() {
		t.Errorf("req records count mismatch, expect:%v, actual:%v", mp.reqRecords.Count(), mp2.reqRecords.Count())
		return
	}

	mp.reqRecords.reqTree.Ascend(func(i BtreeItem) bool {
		if ok := mp2.reqRecords.reqTree.Has(i); !ok {
			t.Errorf("result mismatch, req(%v) not exist in metapartition req records", i.(*RequestInfo))
			return false
		}
		return true
	})
}
