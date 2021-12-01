package metanode

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/chubaofs/chubaofs/metanode/metamock"
	"github.com/chubaofs/chubaofs/proto"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func generateEk(num int) (eks []proto.ExtentKey){
	eks = make([]proto.ExtentKey, 0)
	for i:= 0; i < num; i++ {
		eks = append(eks, proto.ExtentKey{FileOffset: 0, Size: 1000, PartitionId: uint64(i), ExtentId: uint64(i), ExtentOffset: uint64(i)})
	}
	return
}

func newTestMetapartition()(*metaPartition, error){
	node := &MetaNode{nodeId: 1}
	manager := &metadataManager{nodeId: 1, rocksDBDirs: []string{"./"}, metaNode: node}
	conf := &MetaPartitionConfig{RocksDBDir: "./", PartitionId: 1,
									NodeId: 1,
									Start: 1, End: 100,
									Peers: []proto.Peer{proto.Peer{ID: 1, Addr: "127.0.0.1"} },
									RootDir: "./partition_1"}
	tmp, err := CreateMetaPartition(conf, manager)
	if  err != nil {
		fmt.Printf("create meta partition failed:%s", err.Error())
		return nil, err
	}
	mp := tmp.(*metaPartition)
	mp.raftPartition = &metamock.MockPartition{Id: 1}
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

func checkRocksDBEks(t *testing.T, mp *metaPartition, eks []proto.ExtentKey, date []byte)(int) {
	stKey   := make([]byte, 1)
	endKey  := make([]byte, 1)

	stKey[0]  = byte(ExtentDelTable)
	endKey[0] = byte(ExtentDelTable + 1)
	cnt := 0
	mp.db.Range(stKey, endKey, func(k, v []byte)(bool, error) {
		if k[0] != byte(ExtentDelTable) {
			return false, nil
		}
		ek := &proto.ExtentKey{}
		ek.UnmarshalDbKey(k[8:])

		if date[dayKeyIndex] != k[dayKeyIndex] {
			t.Errorf("check rocks db failed: date prefix failed, want key:%v, but now:%v", date, k)
		}
		if ek.Marshal() != eks[cnt].Marshal() {
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
	mp, err := newTestMetapartition()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
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

	releaseTestMetapartition(mp)
}

func TestAddExtentsToDB(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		AddExtentsToDB(t, test)
		t.Logf("Add ek test case[%v] finished", test)
	}
}

func LeaderCleanExpiredEk(t *testing.T, num int) {
	mp, err := newTestMetapartition()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
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
	releaseTestMetapartition(mp)
}

func TestLeaderCleanExpiredEk(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		LeaderCleanExpiredEk(t, test)
		t.Logf("leader clean ek test case[%v] finished", test)
	}
}

func FollowerSyncExpiredEk(t *testing.T, num int) {
	mp, err := newTestMetapartition()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
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
		if err = binary.Write(buf, binary.BigEndian, ek.PartitionId); err != nil {
			t.Fatalf("marsh failed: marsh ek[%v] failed", ek)
		}
		if err = binary.Write(buf, binary.BigEndian, ek.ExtentId); err != nil {
			t.Fatalf("marsh failed: marsh ek[%v] failed", ek)
		}
		if err = binary.Write(buf, binary.BigEndian, (uint32)(ek.ExtentOffset)); err != nil {
			t.Fatalf("marsh failed: marsh ek[%v] failed", ek)
		}
		if err = binary.Write(buf, binary.BigEndian, ek.Size); err != nil {
			t.Fatalf("marsh failed: marsh ek[%v] failed", ek)
		}
	}


	mp.fsmSyncDelExtents(buf.Bytes())

	time.Sleep(time.Second * 2)

	key[dayKeyIndex] += 1
	cnt := checkRocksDBEks(t, mp, eks, key)
	if cnt != num {
		t.Errorf("check cnt failed, want:%d, now:%d", num, cnt)
	}
	releaseTestMetapartition(mp)
}

func TestFollowerSyncExpiredEk(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		FollowerSyncExpiredEk(t, test)
		t.Logf("follower clean ek test case[%v] finished", test)
	}
}

func SnapResetDb(t *testing.T, num int) {
	mp, err := newTestMetapartition()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
	mp.initResouce()

	//gen eks
	key := make([]byte, dbExtentKeySize)
	value := make ([]byte, 1)
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
	if err = db.OpenDb(newDbDir); err != nil {
		return
	}
	key[dayKeyIndex] += 1

	for _, ek := range eks {
		ekInfo, _ := ek.MarshalDbKey()
		copy(key[8:], ekInfo)
		db.Put(key, value)
	}
	db.CloseDb()

	mp.ResetDbByNewDir(newDbDir)
	cnt := checkRocksDBEks(t, mp, eks, key)
	if cnt != num {
		t.Errorf("check cnt failed, want:%d, now:%d", num, cnt)
	}
	releaseTestMetapartition(mp)
}

func TestSnapResetDb(t *testing.T) {
	var addDelEk []int = []int {0, 1, 10, 51, 100, 120, 1000}
	for _, test := range addDelEk {
		SnapResetDb(t, test)
		t.Logf("snap reset db test case[%v] finished", test)
	}
}

func applySnapshot(t *testing.T, num int, rocksEnable bool) {
	mp, err := newTestMetapartition()
	if err != nil {
		t.Fatalf("create mp failed:%s", err.Error())
	}
	mp.startToDeleteExtents()
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
	version := uint32(0)
	if rocksEnable {
		version = 1
	}
	atomic.StoreUint32(&mp.manager.metaNode.clusterMetaVersion, version)
	si, _ := newMetaItemIterator(mp)
	mp.ApplySnapshot(nil, si)

	cnt := getRocksDbCnt(t, mp)

	if rocksEnable {
		if cnt != num {
			t.Logf("apply snap test case[%v] enablerocks :%v, failed, want:%d, now:%d", num, rocksEnable, num, cnt)
		} else {
			checkRocksDBEks(t, mp, eks, key)
		}
	} else {
		if cnt != 0 {
			t.Logf("apply snap test case[%v] enablerocks :%v, failed, want:%d, now:%d", num, rocksEnable, 0, cnt)
		}

	}
	time.Sleep(time.Second)
	releaseTestMetapartition(mp)
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