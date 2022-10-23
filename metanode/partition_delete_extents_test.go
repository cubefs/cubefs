package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/metanode/metamock"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	raftproto "github.com/tiglabs/raft/proto"
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func ApplyMockWithNull(elem interface{},command []byte, index uint64) (resp interface{}, err error) {
	return
}

func generateEk(num int) (eks []proto.MetaDelExtentKey){
	eks = make([]proto.MetaDelExtentKey, 0)
	for i:= 0; i < num; i++ {
		eks = append(eks,
			proto.MetaDelExtentKey{ExtentKey:proto.ExtentKey{
				FileOffset: uint64(i * 100),
				Size: 1000,
				PartitionId: uint64(i),
				ExtentId: uint64(i),
				ExtentOffset: uint64(i),
				CRC: uint32(i),
			},
									InodeId: uint64(i),
									TimeStamp: int64(i),
									SrcType: uint64(i % (delEkSrcTypeFromDelInode + 1))})
	}
	return
}
//
//func mockMP()(*metaPartition, error){
//	node := &MetaNode{nodeId: 1}
//	manager := &metadataManager{nodeId: 1, rocksDBDirs: []string{"./"}, metaNode: node}
//	conf := &MetaPartitionConfig{
//		RocksDBDir:  "./",
//		PartitionId: 1,
//		NodeId:      1,
//		Start:       1,
//		End:         100,
//		Peers:       []proto.Peer{{ID: 1, Addr: "127.0.0.1"}},
//		RootDir:     "./partition_1",
//		StoreMode:   proto.StoreModeMem,
//	}
//	tmp, err := CreateMetaPartition(conf, manager)
//	if  err != nil {
//		fmt.Printf("create meta partition failed:%s", err.Error())
//		return nil, err
//	}
//	mp := tmp.(*metaPartition)
//	mp.raftPartition = &metamock.MockPartition{Id: 1, Mp: []interface{}{mp}, Apply: ApplyMockWithNull}
//	mp.vol = NewVol()
//	return mp, nil
//}
//
//func releaseMP(mp *metaPartition) {
//	close(mp.stopC)
//	time.Sleep(time.Second)
//	mp.db.CloseDb()
//	mp.db.ReleaseRocksDb()
//	os.RemoveAll(mp.config.RootDir)
//}

func checkRocksDBEks(t *testing.T, mp *metaPartition, eks []proto.MetaDelExtentKey, date []byte)(int) {
	stKey   := make([]byte, 1)
	endKey  := make([]byte, 1)

	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)
	cnt := 0
	mp.db.Range(stKey, endKey, func(k, v []byte)(bool, error) {
		if k[0] != byte(ExtentDelTable) {
			return false, nil
		}
		ek := &proto.MetaDelExtentKey{}
		ek.UnmarshalDbKey(k[8:])
		ek.UnMarshDelEkValue(v)

		if date[dayKeyIndex] != k[dayKeyIndex] {
			t.Errorf("check rocks db failed: date prefix failed, want key:%v, but now:%v", date, k)
		}
		if ek.String() != eks[cnt].String() {
			t.Errorf("check rocks db failed: ek failed, want key:%v, but now:%v", eks[cnt], ek)
		}
		cnt++
		return true, nil
	})

	if cnt < len(eks) {
		t.Errorf("check rocks db failed: total count falied")
	}

	return cnt
}

func getRocksDbCnt(t *testing.T, mp *metaPartition)(int) {
	stKey   := make([]byte, 1)
	endKey  := make([]byte, 1)

	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)
	cnt := 0
	mp.db.Range(stKey, endKey, func(k, v []byte)(bool, error) {
		if k[0] != byte(ExtentDelTable) {
			return false, nil
		}
		cnt++
		return true, nil
	})

	return cnt
}

func AddExtentsToDB(t *testing.T, num int) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMockWithNull)
	//mp, err := mockMP()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
	defer releaseMetaPartition(mp)
	mp.startToDeleteExtents()

	//gen eks
	key := make([]byte, dbExtentKeySize)
	eks := generateEk(num)
	updateKeyToNow(key)

	//add eks to db
	mp.extDelCh <- eks
	time.Sleep(time.Second * 2)

	cnt := checkRocksDBEks(t, mp, eks, key)
	if cnt != num {
		t.Errorf("check cnt failed, want:%d, now:%d", num, cnt)
	}
}

func TestAddExtentsToDB(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		AddExtentsToDB(t, test)
		t.Logf("Add ek test case[%v] finished", test)
	}
}

func LeaderCleanExpiredEk(t *testing.T, num int) {
	fmt.Printf("start clean expired, ek number:%v\n", num)
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMockWithNull)
	//mp, err := mockMP()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
	defer releaseMetaPartition(mp)
	mp.startToDeleteExtents()

	//gen eks
	key := make([]byte, dbExtentKeySize)
	eks := generateEk(num)
	updateKeyToNow(key)

	//add eks to db
	mp.extDelCh <- eks
	time.Sleep(time.Second * 2)

	cnt := checkRocksDBEks(t, mp, eks, key)
	if cnt != num {
		t.Errorf("check cnt failed, want:%d, now:%d", num, cnt)
	}
	key[dayKeyIndex] += 1
	mp.addDelExtentToDb(key, eks)
	delCursor := getDateInKey(key)

	mp.extDelCursor<- delCursor

	time.Sleep(time.Second * 2)

	cnt = checkRocksDBEks(t, mp, eks, key)
	if cnt != num {
		t.Errorf("check cnt failed, want:%d, now:%d", num, cnt)
	}
}

func TestLeaderCleanExpiredEk(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		LeaderCleanExpiredEk(t, test)
		t.Logf("leader clean ek test case[%v] finished", test)
	}
}

func FollowerSyncExpiredEk(t *testing.T, num int) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMockWithNull)
	//mp, err := mockMP()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
	defer releaseMetaPartition(mp)

	mockPartition := mp.raftPartition.(*metamock.MockPartition)
	mockPartition.Id = 2
	mp.startToDeleteExtents()

	//gen eks
	key := make([]byte, dbExtentKeySize)
	eks := generateEk(num)
	updateKeyToNow(key)

	//add eks to db
	mp.extDelCh <- eks
	time.Sleep(time.Second * 2)

	checkRocksDBEks(t, mp, eks, key)
	key[hourKeyIndex] += 1

	delCursor := getDateInKey(key)
	buf := bytes.NewBuffer(make([]byte, 0, len(eks) * 24 + 8))


	if err = binary.Write(buf, binary.BigEndian, delCursor); err != nil {
		t.Fatalf("marsh failed: marsh date failed")
	}

	for _, ek := range eks {
		if err = binary.Write(buf, binary.BigEndian, ek.FileOffset); err != nil {
			return
		}
		if err = binary.Write(buf, binary.BigEndian, ek.PartitionId); err != nil {
			return
		}
		if err = binary.Write(buf, binary.BigEndian, ek.ExtentId); err != nil {
			return
		}
		if err = binary.Write(buf, binary.BigEndian, ek.ExtentOffset); err != nil {
			return
		}
		if err = binary.Write(buf, binary.BigEndian, ek.Size); err != nil {
			return
		}
		if err = binary.Write(buf, binary.BigEndian, ek.CRC); err != nil {
			return
		}
		if err = binary.Write(buf, binary.BigEndian, ek.InodeId); err != nil {
			return
		}
		if err = binary.Write(buf, binary.BigEndian, ek.TimeStamp); err != nil {
			return
		}
		if err = binary.Write(buf, binary.BigEndian, ek.SrcType); err != nil {
			return
		}
	}


	mp.fsmSyncDelExtentsV2(buf.Bytes())

	time.Sleep(time.Second * 2)

	key[dayKeyIndex] += 1
	cnt := checkRocksDBEks(t, mp, eks, key)
	if cnt != num {
		t.Errorf("check cnt failed, want:%d, now:%d", num, cnt)
	}
}

func TestFollowerSyncExpiredEk(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		FollowerSyncExpiredEk(t, test)
		t.Logf("follower clean ek test case[%v] finished", test)
	}
}

func SnapResetDb(t *testing.T, num int) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMockWithNull)
	//mp, err := mockMP()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
	defer releaseMetaPartition(mp)
	mp.initResouce()

	//gen eks
	key := make([]byte, dbExtentKeySize)
	updateKeyToNow(key)
	eks := generateEk(num)

	//add eks to db
	mp.addDelExtentToDb(key, eks)
	time.Sleep(time.Second)

	db := NewRocksDb()
	nowStr := strconv.FormatInt(time.Now().Unix(), 10)
	newDbDir := mp.getRocksDbRootDir() + "_" + nowStr

	os.MkdirAll(mp.getRocksDbRootDir() + "_" + strconv.FormatInt(time.Now().Unix() - 20000, 10), 0x755)

	if _, err = os.Stat(newDbDir); err == nil {
		os.RemoveAll(newDbDir)
	}

	os.MkdirAll(newDbDir, 0x755)
	if err = db.OpenDb(newDbDir, 0, 0, 0, 0, 0, 0); err != nil {
		return
	}
	key[dayKeyIndex] += 1

	for _, ek := range eks {
		valueBuff := make([]byte, proto.ExtentValueLen)
		ekInfo, _ := ek.MarshalDbKey()
		copy(key[8:], ekInfo)
		ek.MarshDelEkValue(valueBuff)
		db.Put(key, valueBuff)
	}
	db.CloseDb()

	mp.ResetDbByNewDir(newDbDir)
	cnt := checkRocksDBEks(t, mp, eks, key)
	if cnt != num {
		t.Errorf("check cnt failed, want:%d, now:%d", num, cnt)
	}
}

func TestSnapResetDb(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		SnapResetDb(t, test)
		t.Logf("snap reset db test case[%v] finished", test)
	}
}

func applySnapshot(t *testing.T, num int, rocksEnable bool) {
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./partition_1", ApplyMock)
	mp2, err := mockMetaPartition(2, 1, proto.StoreModeMem, "./partition_2", ApplyMock)
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
	defer func() {
		releaseMetaPartition(mp)
		releaseMetaPartition(mp2)
	}()
	mp.startToDeleteExtents()
	mp2.startToDeleteExtents()
	mockPartition := mp.raftPartition.(*metamock.MockPartition)
	//gen eks
	key := make([]byte, dbExtentKeySize)
	eks := generateEk(num)
	updateKeyToNow(key)

	mp.extDelCh<-eks
	//add eks to db
	time.Sleep(time.Second)
	checkRocksDBEks(t, mp, eks, key)
	mockPartition.Id = 2
	var snapV uint32 = 0
	var si raftproto.SnapIterator
	if rocksEnable {
		snapV = uint32(BatchSnapshotV1)
		si, _ = newBatchMetaItemIterator(mp, BatchSnapshotV1)
	} else {
		si, _ = newMetaItemIterator(mp)
	}

	mp2.ApplySnapshot(nil, si, snapV)
	cnt := getRocksDbCnt(t, mp2)

	if rocksEnable {
		if cnt != num {
			t.Logf("apply snap test case[%v] enablerocks :%v, failed, want:%d, now:%d", num, rocksEnable, num, cnt)
		} else {
			checkRocksDBEks(t, mp2, eks, key)
		}
		metaItem := si.(*BatchMetaItemIterator)
		metaItem.Close()
	} else {
		if cnt != 0 {
			t.Logf("apply snap test case[%v] enablerocks :%v, failed, want:%d, now:%d", num, rocksEnable, 0, cnt)
		}
		metaItem := si.(*MetaItemIterator)
		metaItem.Close()
	}
	time.Sleep(time.Second)
}

func TestApplySnapshot(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		applySnapshot(t, test, false)
		t.Logf("snap reset db test case[%v] finished", test)
	}

	for _, test := range addDelEk {
		applySnapshot(t, test, true)
		t.Logf("snap reset db test case[%v] finished", test)
	}
}

func TestRemoveOldDeleteEKRecordFileCase01(t *testing.T) {
	DeleteEKRecordFilesMaxTotalSize.Store(20 * util.MB)
	rootDir := "./test_remove_old_file"
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, rootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock metapartition failed:%v", err)
		return
	}

	if mp == nil {
		t.Errorf("mock mp is nil")
		return
	}
	defer releaseMetaPartition(mp)
	mp.manager.metaNode.disks = make(map[string]*util.FsCapMon, 0)
	mp.manager.metaNode.disks[rootDir] = &util.FsCapMon{
		Path:          rootDir,
		IsRocksDBDisk: false,
		ReservedSpace: 0,
		Total:         100,
		Used:          60,
		Available:     0,
		Status:        0,
		MPCount:       0,
	}

	//create delete record ek file
	err = createTestDeleteEKRecordsFile(2, mp.config.RootDir)
	if err != nil {
		t.Errorf("create test file failed:%v", err)
		return
	}

	mp.removeOldDeleteEKRecordFile(delExtentKeyList,  prefixDelExtentKeyListBackup,false)
	var files []fs.DirEntry
	files, err = os.ReadDir(mp.config.RootDir)
	if err != nil {
		t.Errorf("read dir failed:%v", err)
		return
	}
	cnt := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), prefixDelExtentKeyListBackup) && file.Name() != delExtentKeyList {
			cnt++
		}
	}
	if cnt != 2 {
		t.Errorf("expect file count:5, actual:%v", cnt)
		return
	}

	if _, err = os.Stat(path.Join(mp.config.RootDir, delExtentKeyList)); err == nil {
		return
	} else {
		if os.IsNotExist(err) {
			t.Errorf("%s has been deleted", delExtentKeyList)
			return
		}
		t.Errorf("stat %s error:%v", delExtentKeyList, err)
	}
}

func TestRemoveOldDeleteEKRecordFileCase02(t *testing.T) {
	DeleteEKRecordFilesMaxTotalSize.Store(20 * util.MB)
	rootDir := "./test_remove_old_file"
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, rootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock metapartition failed:%v", err)
		return
	}

	if mp == nil {
		t.Errorf("mock mp is nil")
		return
	}
	defer releaseMetaPartition(mp)
	mp.manager.metaNode.disks = make(map[string]*util.FsCapMon, 0)
	mp.manager.metaNode.disks[rootDir] = &util.FsCapMon{
		Path:          rootDir,
		IsRocksDBDisk: false,
		ReservedSpace: 0,
		Total:         100,
		Used:          40,
		Available:     0,
		Status:        0,
		MPCount:       0,
	}

	//create delete record ek file
	err = createTestDeleteEKRecordsFile(5, mp.config.RootDir)
	if err != nil {
		t.Errorf("create test file failed:%v", err)
		return
	}

	mp.removeOldDeleteEKRecordFile(delExtentKeyList,  prefixDelExtentKeyListBackup,false)
	var files []fs.DirEntry
	files, err = os.ReadDir(mp.config.RootDir)
	if err != nil {
		t.Errorf("read dir failed:%v", err)
		return
	}
	cnt := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), prefixDelExtentKeyListBackup) && file.Name() != delExtentKeyList {
			cnt++
		}
	}
	if cnt != 5 {
		t.Errorf("expect file count:5, actual:%v", cnt)
		return
	}

	if _, err = os.Stat(path.Join(mp.config.RootDir, delExtentKeyList)); err == nil {
		return
	} else {
		if os.IsNotExist(err) {
			t.Errorf("%s has been deleted", delExtentKeyList)
			return
		}
		t.Errorf("stat %s error:%v", delExtentKeyList, err)
	}
}

func TestRemoveOldDeleteEKRecordFileCase03(t *testing.T) {
	DeleteEKRecordFilesMaxTotalSize.Store(20 * util.MB)
	rootDir := "./test_remove_old_file"
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, rootDir, ApplyMockWithNull)
	if err != nil {
		t.Errorf("mock metapartition failed:%v", err)
		return
	}

	if mp == nil {
		t.Errorf("mock mp is nil")
		return
	}
	defer releaseMetaPartition(mp)
	mp.manager.metaNode.disks = make(map[string]*util.FsCapMon, 0)
	mp.manager.metaNode.disks[rootDir] = &util.FsCapMon{
		Path:          rootDir,
		IsRocksDBDisk: false,
		ReservedSpace: 0,
		Total:         100,
		Used:          60,
		Available:     0,
		Status:        0,
		MPCount:       0,
	}

	//create delete record ek file
	err = createTestDeleteEKRecordsFile(5, mp.config.RootDir)
	if err != nil {
		t.Errorf("create test file failed:%v", err)
		return
	}

	mp.removeOldDeleteEKRecordFile(delExtentKeyList,  prefixDelExtentKeyListBackup,false)
	var files []fs.DirEntry
	files, err = os.ReadDir(mp.config.RootDir)
	if err != nil {
		t.Errorf("read dir failed:%v", err)
		return
	}
	cnt := 0
	for _, file := range files {
		if strings.HasPrefix(file.Name(), prefixDelExtentKeyListBackup) && file.Name() != delExtentKeyList {
			cnt++
		}
	}
	if cnt != 3 {
		t.Errorf("expect file count:3, actual:%v", cnt)
		return
	}

	if _, err = os.Stat(path.Join(mp.config.RootDir, delExtentKeyList)); err == nil {
		return
	} else {
		if os.IsNotExist(err) {
			t.Errorf("%s has been deleted", delExtentKeyList)
			return
		}
		t.Errorf("stat %s error:%v", delExtentKeyList, err)
	}
}

func createTestDeleteEKRecordsFile(count int, dir string) (err error) {
	for count > 0 {
		var fp *os.File
		fileName := path.Join(dir, prefixDelExtentKeyListBackup + time.Now().Format(proto.TimeFormat2))
		fp, err = os.Create(fileName)
		if err != nil {
			return
		}
		err = syscall.Fallocate(int(fp.Fd()), 0, 0, 64 * defMaxDelEKRecord)
		if err != nil {
			fp.Close()
			return
		}
		fp.Close()
		time.Sleep(1 * time.Second)
		count--
	}
	time.Sleep(1 * time.Second)
	if _, err = os.Create(path.Join(dir, delExtentKeyList)); err != nil {
		return
	}
	return
}
