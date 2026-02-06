// Copyright 2023 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	raftstoremock "github.com/cubefs/cubefs/util/mocktest/raftstore"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	partitionId uint64 = 10
	mp          *metaPartition
)

// PartitionId   uint64              `json:"partition_id"`
// VolName       string              `json:"vol_name"`
// PartitionType int                 `json:"partition_type"`
var metaConf = &MetaPartitionConfig{
	PartitionId:   10001,
	VolName:       VolNameForTest,
	PartitionType: proto.VolumeTypeHot,
	StoreMode:     proto.StoreModeMem,
}

const (
	ConfigKeyLogDir               = "logDir"
	ConfigKeyLogLevel             = "logLevel"
	DirModeType            uint32 = 2147484141
	RocksdbMultiVerTestDir        = "/tmp/cfs/mv_test"
)

var cfgJSON = `{
		"role": "meta",
		"logDir": "/tmp/cfs/logs",
		"logLevel":"debug",
		"walDir":"/tmp/cubefs/raft",
		"clusterName":"cubefs"
	}`

func newManager() *metadataManager {
	return &metadataManager{
		partitions:      make(map[uint64]MetaPartition),
		volUpdating:     new(sync.Map),
		rocksdbManager:  NewPerDiskRocksdbManager(&RocksdbManagerConfig{}),
		fileStatsConfig: &fileStatsConfig{},
	}
}

func newPartition(conf *MetaPartitionConfig, manager *metadataManager) (mp *metaPartition) {
	mp = &metaPartition{
		config:         conf,
		stopC:          make(chan bool),
		storeChan:      make(chan *storeMsg, 100),
		freeList:       newFreeList(),
		freeHybridList: newFreeList(),
		extDelCh:       make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		extReset:       make(chan struct{}),
		vol:            NewVol(),
		manager:        manager,
		verSeq:         conf.VerSeq,
		rocksdbManager: manager.rocksdbManager,
	}
	if conf.StoreMode == proto.StoreModeRocksDb {
		err := mp.rocksdbManager.Register(conf.RocksDBDir)
		if err != nil {
			panic(err)
		}
	}
	mp.config.Cursor = 0
	mp.config.End = 100000
	mp.uidManager = NewUidMgr(conf.VolName, mp.config.PartitionId)
	mp.mqMgr = NewQuotaManager(conf.VolName, mp.config.PartitionId)
	err := mp.initObjects(true)
	if err != nil {
		panic(err)
	}
	mp.vol.info = &proto.SimpleVolView{
		VolStorageClass: proto.StorageClass_Replica_SSD,
		Pools: map[uint8]*proto.StoragePoolInfo{
			proto.DefaultHDDPoolId: {
				Id:           proto.DefaultHDDPoolId,
				Name:         "default",
				StorageClass: uint8(proto.StorageClass_Replica_HDD),
			},
			proto.DefaultSSDPoolId: {
				Id:           proto.DefaultSSDPoolId,
				Name:         "default",
				StorageClass: uint8(proto.StorageClass_Replica_SSD),
			},
			proto.DefaultECPoolId: {
				Id:           proto.DefaultECPoolId,
				Name:         "default",
				StorageClass: uint8(proto.StorageClass_BlobStore),
			},
		},
		DefaultPoolId: proto.DefaultHDDPoolId,
	}

	return mp
}

func init() {
	cfg := config.LoadConfigString(cfgJSON)

	logDir := cfg.GetString(ConfigKeyLogDir)

	clusterEnableSnapshot = true

	if _, err := log.InitLog(logDir, "metanode", log.DebugLevel, nil, log.DefaultLogLeftSpaceLimitRatio); err != nil {
		fmt.Println("Fatal: failed to start the cubefs daemon - ", err)
		return
	}
	log.LogDebugf("action start")
}

func getMpConfig(storeMode proto.StoreMode) (config *MetaPartitionConfig) {
	config = &MetaPartitionConfig{
		PartitionId:   10001,
		VolName:       VolNameForTest,
		PartitionType: proto.VolumeTypeHot,
		StoreMode:     storeMode,
	}
	if config.StoreMode == proto.StoreModeRocksDb {
		config.RocksDBDir = fmt.Sprintf("%v/%v_%v", RocksdbMultiVerTestDir, partitionId, time.Now().UnixMilli())
	}
	return
}

func initMp(t *testing.T, storeMode proto.StoreMode) {
	config := getMpConfig(storeMode)
	mp = newPartition(config, newManager())
	mp.multiVersionList = &proto.VolVersionInfoList{}
	ino := testCreateInode(nil, DirModeType)
	t.Logf("cursor %v create ino[%v]", mp.config.Cursor, ino)
	mp.config.Cursor = 1000
}

func buildExtentKey(seq uint64, foffset uint64, extid uint64, exteoffset uint64, size uint32) proto.ExtentKey {
	return proto.ExtentKey{
		FileOffset:   foffset,
		PartitionId:  partitionId,
		ExtentId:     extid,
		ExtentOffset: exteoffset, // offset in extent like tiny extent offset large than 0,normal is 0
		Size:         size,       // extent real size?
		SnapInfo: &proto.ExtSnapInfo{
			VerSeq: seq,
		},
	}
}

func buildExtents(verSeq uint64, startFileOff uint64, extid uint64) (exts []proto.ExtentKey) {
	var (
		i      uint64
		extOff uint64 = 0
	)
	for ; i < 1; i++ {
		ext1 := buildExtentKey(verSeq, startFileOff+i*1000, extid, extOff+i*1000, 1000)
		exts = append(exts, ext1)
	}

	return
}

func isExtEqual(ek1 proto.ExtentKey, ek2 proto.ExtentKey) bool {
	return ek1.ExtentId == ek2.ExtentId &&
		ek1.FileOffset == ek2.FileOffset &&
		ek1.Size == ek2.Size &&
		ek1.ExtentOffset == ek2.ExtentOffset &&
		ek1.PartitionId == ek2.PartitionId
}

func isDentryEqual(den1 *proto.Dentry, den2 *Dentry) bool {
	return den1.Inode == den2.Inode &&
		den1.Name == den2.Name &&
		den1.Type == den2.Type
}

func checkOffSetInSequnce(t *testing.T, eks []proto.ExtentKey) bool {
	if len(eks) < 2 {
		return true
	}

	var (
		lastFileOff uint64 = eks[0].FileOffset
		lastSize    uint32 = eks[0].Size
	)

	for idx, ext := range eks[1:] {
		// t.Logf("idx:%v ext:%v, lastFileOff %v, lastSize %v", idx, ext, lastFileOff, lastSize)
		if ext.FileOffset != lastFileOff+uint64(lastSize) {
			t.Errorf("checkOffSetInSequnce not equal idx %v %v:(%v+%v) eks{%v}", idx, ext.FileOffset, lastFileOff, lastSize, eks)
			return false
		}
		lastFileOff = ext.FileOffset
		lastSize = ext.Size
	}
	return true
}

func testGetExtList(t *testing.T, ino *Inode, verRead uint64) (resp *proto.GetExtentsResponse) {
	reqExtList := &proto.GetExtentsRequest{
		VolName:     metaConf.VolName,
		PartitionID: partitionId,
		Inode:       ino.Inode,
	}
	packet := &Packet{}
	reqExtList.VerSeq = verRead
	assert.True(t, nil == mp.ExtentsList(reqExtList, packet))
	resp = &proto.GetExtentsResponse{}

	assert.True(t, nil == packet.UnmarshalData(resp))
	t.Logf("testGetExtList.resp %v", resp)
	assert.True(t, packet.ResultCode == proto.OpOk)
	assert.True(t, checkOffSetInSequnce(t, resp.Extents))
	return
}

func testCheckExtList(t *testing.T, inode uint64, seqArr []uint64) bool {
	reqExtList := &proto.GetExtentsRequest{
		VolName:     VolNameForTest,
		PartitionID: partitionId,
		Inode:       inode,
	}
	item := NewInode(inode, 0)

	for idx, verRead := range seqArr {
		t.Logf("check extlist index %v ver [%v]", idx, verRead)
		reqExtList.VerSeq = verRead
		getExtRsp := testGetExtList(t, item, verRead)
		ino, err := mp.inodeTree.Get(item)
		require.NoError(t, err)
		require.NotNil(t, ino)
		t.Logf("check extlist rsp %v size %v,%v", getExtRsp, getExtRsp.Size, ino.Size)
		// require.True(t, getExtRsp.Size == uint64(1000*(idx+1)))
	}
	return true
}

func testCreateInode(t *testing.T, mode uint32) *Inode {
	inoID, _ := mp.nextInodeID()
	if t != nil {
		t.Logf("inode id:%v", inoID)
	}

	ino := NewInode(inoID, mode)
	ino.StorageClass = proto.StorageClass_Replica_HDD
	ino.PoolId = proto.DefaultHDDPoolId
	ino.HybridCloudExtents.sortedEks = NewSortedExtents()
	ino.setVer(mp.verSeq)
	if t != nil {
		t.Logf("testCreateInode ino[%v]", ino)
	}

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	if t != nil {
		require.NoError(t, err)
	}

	status, err := mp.fsmCreateInode(handle, ino)
	if t != nil {
		require.NoError(t, err)
		require.Equal(t, status, proto.OpOk)
	}
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	if t != nil {
		require.NoError(t, err)
	}
	ino, err = mp.inodeTree.Get(ino)
	if t != nil {
		require.NoError(t, err)
	}
	return ino
}

func testCreateDentry(t *testing.T, parentId uint64, inodeId uint64, name string, mod uint32) *Dentry {
	dentry := &Dentry{
		ParentId:  parentId,
		Name:      name,
		Inode:     inodeId,
		Type:      mod,
		multiSnap: NewDentrySnap(mp.verSeq),
	}

	t.Logf("createDentry dentry %v", dentry)
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	ret, err := mp.fsmCreateDentry(handle, dentry, false)
	require.NoError(t, err)
	assert.True(t, proto.OpOk == ret)
	if ret != proto.OpOk {
		panic(nil)
	}
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	denParm := &Dentry{ParentId: parentId, Name: name}
	dentry, err = mp.dentryTree.Get(denParm)
	require.NoError(t, err)
	return dentry
}

func testEkMarshal(t *testing.T) {
	// inodeID uint64, ekRef *sync.Map, ek *proto.ExtentKey
	ino := testCreateInode(t, FileModeType)
	ino.multiSnap = NewMultiSnap(mp.verSeq)
	ino.multiSnap.ekRefMap = new(sync.Map)
	ek := &proto.ExtentKey{
		PartitionId: 10,
		ExtentId:    20,
		SnapInfo: &proto.ExtSnapInfo{
			VerSeq: 123444,
		},
	}
	id := storeEkSplit(0, 0, ino.multiSnap.ekRefMap, ek)
	dpID, extID := proto.ParseFromId(id)
	assert.True(t, dpID == ek.PartitionId)
	assert.True(t, extID == ek.ExtentId)

	ok, _ := ino.DecSplitEk(mp.config.PartitionId, ek)
	assert.True(t, ok == true)
	log.LogDebugf("TestEkMarshal close")
}

func TestEkMarshal(t *testing.T) {
	log.LogDebugf("TestEkMarshal")
	initMp(t, proto.StoreModeMem)
	testEkMarshal(t)
}

func TestEkMarshal_Rocksdb(t *testing.T) {
	log.LogDebugf("TestEkMarshal_Rocksdb")
	initMp(t, proto.StoreModeRocksDb)
	testEkMarshal(t)
}

func initVer() {
	verInfo := &proto.VolVersionInfo{
		Ver:    0,
		Status: proto.VersionNormal,
	}
	mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, verInfo)
}

// create
func testSplitKeyDeletion(t *testing.T) {
	initVer()
	mp.config.Cursor = 1100

	fileIno := testCreateInode(t, FileModeType)

	fileName := "fileTest"
	dirDen := testCreateDentry(t, 1, fileIno.Inode, fileName, FileModeType)
	assert.True(t, dirDen != nil)

	initExt := buildExtentKey(0, 0, 1024, 0, 1000)
	fileIno.GetExtents().eks = append(fileIno.GetExtentEks(), initExt)

	splitSeq := testCreateVer()
	splitKey := buildExtentKey(splitSeq, 500, 1024, 128100, 100)
	extents := &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp := &Inode{
		Inode: fileIno.Inode,
		multiSnap: &InodeMultiSnap{
			verSeq: splitSeq,
		},
		HybridCloudExtents: NewSortedHybridCloudExtents(),
	}

	mp.verSeq = iTmp.getVer()
	iTmp.StorageClass = proto.StorageClass_Replica_HDD
	iTmp.HybridCloudExtents.sortedEks = extents

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.fsmAppendExtentsWithCheck(handle, iTmp, true)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	//TODO: support-hybrid
	//assert.True(t, testGetSplitSize(t, fileIno) == 1)
	//assert.True(t, testGetEkRefCnt(t, fileIno, &initExt) == 4)
	//
	//testCleanSnapshot(t, 0)
	//delCnt := testDelDiscardEK(t, fileIno)
	//assert.True(t, 1 == delCnt)
	//
	//assert.True(t, testGetSplitSize(t, fileIno) == 1)
	//assert.True(t, testGetEkRefCnt(t, fileIno, &initExt) == 3)
	//
	//log.LogDebugf("try to deletion current")
	//testDeleteDirTree(t, 1, 0)
	//
	//fileIno.GetAllExtsOfflineInode(mp.config.PartitionId)
	//
	//splitCnt := uint32(testGetSplitSize(t, fileIno))
	//assert.True(t, 0 == splitCnt)
	//
	//assert.True(t, testGetSplitSize(t, fileIno) == 0)
	//assert.True(t, testGetEkRefCnt(t, fileIno, &initExt) == 0)
}

func TestSplitKeyDeletion(t *testing.T) {
	log.LogDebugf("action[TestSplitKeyDeletion] start!!!!!!!!!!!")
	initMp(t, proto.StoreModeMem)
	testSplitKeyDeletion(t)
}

func TestSplitKeyDeletion_Rocksdb(t *testing.T) {
	log.LogDebugf("action[TestSplitKeyDeletion_Rocksdb] start!!!!!!!!!!!")
	initMp(t, proto.StoreModeRocksDb)
	testSplitKeyDeletion(t)
}

func testGetlastVer() (verSeq uint64) {
	vlen := len(mp.multiVersionList.VerList)
	return mp.multiVersionList.VerList[vlen-1].Ver
}

var tm = time.Now().Unix()

func testCreateVer() (verSeq uint64) {
	mp.multiVersionList.RWLock.Lock()
	defer mp.multiVersionList.RWLock.Unlock()

	tm = tm + 1
	verInfo := &proto.VolVersionInfo{
		Ver:    uint64(tm),
		Status: proto.VersionNormal,
	}
	mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, verInfo)
	mp.verSeq = verInfo.Ver
	return verInfo.Ver
}

func testReadDirAll(t *testing.T, verSeq uint64, parentId uint64) (resp *ReadDirLimitResp) {
	// testPrintAllDentry(t)
	t.Logf("[testReadDirAll] with seq [%v] parentId %v", verSeq, parentId)
	req := &ReadDirLimitReq{
		PartitionID: partitionId,
		VolName:     mp.GetVolName(),
		ParentID:    parentId,
		Limit:       math.MaxUint64,
		VerSeq:      verSeq,
	}
	resp, _ = mp.readDirLimit(req)
	return
}

func testVerListRemoveVer(t *testing.T, verSeq uint64) bool {
	testPrintAllSysVerList(t)
	for i, ver := range mp.multiVersionList.VerList {
		if ver.Ver == verSeq {
			// mp.multiVersionList = append(mp.multiVersionList[:i], mp.multiVersionList[i+1:]...)
			if i == len(mp.multiVersionList.VerList)-1 {
				mp.multiVersionList.VerList = mp.multiVersionList.VerList[:i]
				return true
			}
			mp.multiVersionList.VerList = append(mp.multiVersionList.VerList[:i], mp.multiVersionList.VerList[i+1:]...)
			return true
		}
	}
	return false
}

var (
	ct        = uint64(time.Now().Unix())
	seqAllArr = []uint64{0, ct, ct + 2111, ct + 10333, ct + 53456, ct + 60000, ct + 72344, ct + 234424, ct + 334424}
)

func testAppendList(t *testing.T) {
	for _, verSeq := range seqAllArr {
		verInfo := &proto.VolVersionInfo{
			Ver:    verSeq,
			Status: proto.VersionNormal,
		}
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, verInfo)
	}

	ino := testCreateInode(t, 0)
	t.Logf("enter TestAppendList")
	index := 5
	seqArr := seqAllArr[1:index]
	t.Logf("layer len %v, arr size %v, seqarr(%v)", ino.getLayerLen(), len(seqArr), seqArr)
	for idx, seq := range seqArr {
		handle, err := mp.inodeTree.CreateBatchWriteHandle()
		require.NoError(t, err)
		exts := buildExtents(seq, uint64(idx*1000), uint64(idx))
		t.Logf("buildExtents exts[%v]", exts)
		iTmp := &Inode{
			Inode: ino.Inode,
			HybridCloudExtents: NewSortedHybridCloudExtentsExt(&SortedExtents{
				eks: exts,
			}),
			// ObjExtents: NewSortedObjExtents(),
			multiSnap: &InodeMultiSnap{
				verSeq: seq,
			},
			StorageClass: proto.StorageClass_Replica_HDD,
		}
		mp.verSeq = seq

		status, err := mp.fsmAppendExtentsWithCheck(handle, iTmp, false)
		require.NoError(t, err)
		if status != proto.OpOk {
			t.Errorf("status [%v]", status)
		}
		err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
		require.NoError(t, err)
	}
	t.Logf("layer len %v, arr size %v, seqarr(%v)", ino.getLayerLen(), len(seqArr), seqArr)
	// TODO:leonrayang
	// assert.True(t, ino.getLayerLen() == len(seqArr))
	// assert.True(t, ino.getVer() == mp.verSeq)

	ino, err := mp.inodeTree.Get(&Inode{Inode: ino.Inode})
	require.NoError(t, err)
	for i := 0; i < len(seqArr)-1; i++ {
		// TODO:hybrid cloud support snapshot
		// assert.True(t, ino.getLayerVer(i) == seqArr[len(seqArr)-i-2])
		t.Logf("layer %v len %v content %v,seq [%v], %v", i, len(ino.multiSnap.multiVersions[i].GetExtentEks()), ino.multiSnap.multiVersions[i].GetExtentEks(),
			ino.getLayerVer(i), seqArr[len(seqArr)-i-2])
		// TODO:leonrayang
		assert.True(t, len(ino.multiSnap.multiVersions[i].GetExtentEks()) == 0)
	}

	//-------------   split at begin -----------------------------------------
	t.Logf("start split at begin")
	splitSeq := seqAllArr[index]
	splitKey := buildExtentKey(splitSeq, 0, 0, 128000, 10)
	extents := &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp := &Inode{
		Inode: ino.Inode,
		multiSnap: &InodeMultiSnap{
			verSeq: splitSeq,
		},
		HybridCloudExtents: NewSortedHybridCloudExtentsExt(extents),
		StorageClass:       ino.StorageClass,
	}
	iTmp.StorageClass = proto.StorageClass_Replica_HDD
	mp.verSeq = iTmp.getVer()
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.fsmAppendExtentsWithCheck(handle, iTmp, true)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	ino, err = mp.inodeTree.Get(&Inode{Inode: ino.Inode})
	require.NoError(t, err)
	t.Logf("in split at begin")
	assert.True(t, ino.multiSnap.multiVersions[0].GetExtentEks()[0].GetSeq() == ino.getLayerVer(3))
	assert.True(t, ino.multiSnap.multiVersions[0].GetExtentEks()[0].FileOffset == 0)
	assert.True(t, ino.multiSnap.multiVersions[0].GetExtentEks()[0].ExtentId == 0)
	assert.True(t, ino.multiSnap.multiVersions[0].GetExtentEks()[0].ExtentOffset == 0)
	assert.True(t, ino.multiSnap.multiVersions[0].GetExtentEks()[0].Size == splitKey.Size)

	t.Logf("in split at begin")

	assert.True(t, isExtEqual(ino.GetExtentEks()[0], splitKey))
	assert.True(t, checkOffSetInSequnce(t, ino.GetExtentEks()))

	t.Logf("top layer len %v, layer 1 len %v arr size %v", len(ino.GetExtentEks()), len(ino.multiSnap.multiVersions[0].GetExtentEks()), len(seqArr))
	assert.True(t, len(ino.multiSnap.multiVersions[0].GetExtentEks()) == 1)
	assert.True(t, len(ino.GetExtentEks()) == len(seqArr)+1)
	// TODO:leonrayang
	testCheckExtList(t, ino.Inode, seqArr)

	//--------  split at middle  -----------------------------------------------
	t.Logf("start split at middle")

	lastTopEksLen := len(ino.GetExtentEks())
	t.Logf("split at middle lastTopEksLen %v", lastTopEksLen)

	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 500, 0, 128100, 100)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:              ino.Inode,
		HybridCloudExtents: NewSortedHybridCloudExtentsExt(extents),
		multiSnap: &InodeMultiSnap{
			verSeq: splitSeq,
		},
		StorageClass: ino.StorageClass,
	}
	t.Logf("split at middle multiSnap.multiVersions %v", ino.getLayerLen())
	mp.verSeq = iTmp.getVer()
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.fsmAppendExtentsWithCheck(handle, iTmp, true)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	ino, err = mp.inodeTree.Get(&Inode{Inode: ino.Inode})
	require.NoError(t, err)
	t.Logf("split at middle multiSnap.multiVersions %v", ino.getLayerLen())

	getExtRsp := testGetExtList(t, ino, ino.getLayerVer(0))
	t.Logf("split at middle getExtRsp len %v seq(%v), toplayer len:%v seq(%v)",
		len(getExtRsp.Extents), ino.getLayerVer(0), len(ino.GetExtentEks()), ino.getVer())

	assert.True(t, len(getExtRsp.Extents) == lastTopEksLen+2)
	assert.True(t, len(ino.GetExtentEks()) == lastTopEksLen+2)
	assert.True(t, checkOffSetInSequnce(t, ino.GetExtentEks()))

	t.Logf("ino exts{%v}", ino.GetExtentEks())

	//--------  split at end  -----------------------------------------------
	t.Logf("start split at end")
	// split at end
	lastTopEksLen = len(ino.GetExtentEks())
	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 3900, 3, 129000, 100)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:              ino.Inode,
		HybridCloudExtents: NewSortedHybridCloudExtentsExt(extents),
		multiSnap: &InodeMultiSnap{
			verSeq: splitSeq,
		},
		StorageClass: ino.StorageClass,
	}
	t.Logf("split key:%v", splitKey)
	getExtRsp = testGetExtList(t, ino, ino.getLayerVer(0))
	t.Logf("split at middle multiSnap.multiVersions %v, extent %v, level 1 %v", ino.getLayerLen(), getExtRsp.Extents, ino.multiSnap.multiVersions[0].GetExtentEks())
	mp.verSeq = iTmp.getVer()
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.fsmAppendExtentsWithCheck(handle, iTmp, true)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	ino, err = mp.inodeTree.Get(&Inode{Inode: ino.Inode})
	require.NoError(t, err)
	t.Logf("split at middle multiSnap.multiVersions %v", ino.getLayerLen())
	getExtRsp = testGetExtList(t, ino, ino.getLayerVer(0))
	t.Logf("split at middle multiSnap.multiVersions %v, extent %v, level 1 %v", ino.getLayerLen(), getExtRsp.Extents, ino.multiSnap.multiVersions[0].GetExtentEks())

	t.Logf("split at middle getExtRsp len %v seq(%v), toplayer len:%v seq(%v)",
		len(getExtRsp.Extents), ino.getLayerVer(0), len(ino.GetExtentEks()), ino.getVer())

	assert.True(t, len(getExtRsp.Extents) == lastTopEksLen+1)
	assert.True(t, len(ino.GetExtentEks()) == lastTopEksLen+1)
	assert.True(t, isExtEqual(ino.GetExtentEks()[lastTopEksLen], splitKey))
	// assert.True(t, false)

	//--------  split at the splited one  -----------------------------------------------
	t.Logf("start split at end")
	// split at end
	lastTopEksLen = len(ino.GetExtentEks())
	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 3950, 3, 129000, 20)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:              ino.Inode,
		HybridCloudExtents: NewSortedHybridCloudExtentsExt(extents),
		multiSnap: &InodeMultiSnap{
			verSeq: splitSeq,
		},
		StorageClass: proto.StorageClass_Replica_HDD,
	}
	t.Logf("split key:%v", splitKey)
	mp.verSeq = iTmp.getVer()
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.fsmAppendExtentsWithCheck(handle, iTmp, true)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	ino, err = mp.inodeTree.Get(&Inode{Inode: ino.Inode})
	require.NoError(t, err)

	_ = testGetExtList(t, ino, ino.getLayerVer(0))

	assert.True(t, len(ino.GetExtentEks()) == lastTopEksLen+2)
	assert.True(t, checkOffSetInSequnce(t, ino.GetExtentEks()))
}

func TestAppendList(t *testing.T) {
	initMp(t, proto.StoreModeMem)
	testAppendList(t)
}

func TestAppendList_Rocksdb(t *testing.T) {
	initMp(t, proto.StoreModeRocksDb)
	testAppendList(t)
}

//func MockSubmitTrue(mp *metaPartition, inode uint64, offset int, data []byte,
//	flags int) (write int, err error) {
//	return len(data), nil
//}

func testPrintAllSysVerList(t *testing.T) {
	for idx, info := range mp.multiVersionList.VerList {
		t.Logf("testPrintAllSysVerList idx %v, info %v", idx, info)
	}
}

func testPrintAllDentry(t *testing.T) uint64 {
	var cnt uint64
	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	defer snap.Close()
	snap.Range(DentryType, func(i interface{}) bool {
		den := i.(*Dentry)
		t.Logf("testPrintAllDentry name %v top layer dentry:%v", den.Name, den)
		if den.getSnapListLen() > 0 {
			for id, info := range den.multiSnap.dentryList {
				t.Logf("testPrintAllDentry name %v layer %v, denseq [%v] den %v", den.Name, id, info.getVerSeq(), info)
			}
		}
		cnt++
		return true
	})
	return cnt
}

func testPrintAllInodeInfo(t *testing.T) {
	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	defer snap.Close()
	snap.Range(InodeType, func(item interface{}) bool {
		i := item.(*Inode)
		t.Logf("action[PrintAllVersionInfo] toplayer inode[%v] verSeq [%v] hist len [%v]", i, i.getVer(), i.getLayerLen())
		if i.getLayerLen() == 0 {
			return true
		}
		for id, info := range i.multiSnap.multiVersions {
			t.Logf("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode[%v]", id, info.getVer(), info)
		}
		return true
	})
}

func testPrintInodeInfo(t *testing.T, ino *Inode) {
	i, _ := mp.inodeTree.Get(ino)
	t.Logf("action[PrintAllVersionInfo] toplayer inode[%v] verSeq [%v] hist len [%v]", i, i.getVer(), i.getLayerLen())
	if i.getLayerLen() == 0 {
		return
	}
	for id, info := range i.multiSnap.multiVersions {
		t.Logf("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode[%v]", id, info.getVer(), info)
	}
}

func testDelDirSnapshotVersion(t *testing.T, verSeq uint64, dirIno *Inode, dirDentry *Dentry) {
	if verSeq != 0 {
		assert.True(t, testVerListRemoveVer(t, verSeq))
	}

	rspReadDir := testReadDirAll(t, verSeq, dirIno.Inode)
	// testPrintAllDentry(t)

	rDirIno := dirIno.Copy().(*Inode)
	rDirIno.setVerNoCheck(verSeq)

	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	rspDelIno, err := mp.fsmUnlinkInode(handle, rDirIno, 0)
	require.NoError(t, err)

	t.Logf("rspDelinfo ret %v content %v", rspDelIno.Status, rspDelIno)
	assert.True(t, rspDelIno.Status == proto.OpOk)
	if rspDelIno.Status != proto.OpOk {
		return
	}
	rDirDentry := dirDentry.Copy().(*Dentry)
	rDirDentry.setVerSeq(verSeq)
	rspDelDen, err := mp.fsmDeleteDentry(handle, rDirDentry, false)
	require.NoError(t, err)
	assert.True(t, rspDelDen.Status == proto.OpOk)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	for idx, info := range rspReadDir.Children {
		t.Logf("testDelDirSnapshotVersion: delseq [%v]  to del idx %v infof %v", verSeq, idx, info)
		rino := &Inode{
			Inode: info.Inode,
			Type:  FileModeType,
			multiSnap: &InodeMultiSnap{
				verSeq: verSeq,
			},
		}
		testPrintInodeInfo(t, rino)
		log.LogDebugf("testDelDirSnapshotVersion get rino[%v] start", rino)
		t.Logf("testDelDirSnapshotVersion get rino[%v] start", rino)
		ino := mp.getInode(rino, false)
		log.LogDebugf("testDelDirSnapshotVersion get rino[%v] end", ino)
		t.Logf("testDelDirSnapshotVersion get rino[%v] end", rino)
		assert.True(t, ino.Status == proto.OpOk)
		if ino.Status != proto.OpOk {
			panic(nil)
		}
		rino.setVer(verSeq)

		handle, err = mp.dentryTree.CreateBatchWriteHandle()
		require.NoError(t, err)
		rspDelIno, err = mp.fsmUnlinkInode(handle, rino, 0)
		require.NoError(t, err)
		err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
		require.NoError(t, err)

		assert.True(t, rspDelIno.Status == proto.OpOk || rspDelIno.Status == proto.OpNotExistErr)
		if rspDelIno.Status != proto.OpOk && rspDelIno.Status != proto.OpNotExistErr {
			t.Logf("testDelDirSnapshotVersion: rspDelino[%v] return st %v", rspDelIno, proto.ParseErrorCode(int32(rspDelIno.Status)))
			panic(nil)
		}
		dentry := &Dentry{
			ParentId:  rDirIno.Inode,
			Name:      info.Name,
			Type:      FileModeType,
			multiSnap: NewDentrySnap(verSeq),
			Inode:     rino.Inode,
		}
		log.LogDebugf("test.testDelDirSnapshotVersion: dentry param %v ", dentry)
		// testPrintAllDentry(t)
		iden, st, err := mp.getDentry(dentry)
		require.NoError(t, err)
		if st != proto.OpOk {
			t.Logf("testDelDirSnapshotVersion: dentry %v return st %v", dentry, proto.ParseErrorCode(int32(st)))
		}
		log.LogDebugf("test.testDelDirSnapshotVersion: get dentry %v ", iden)
		assert.True(t, st == proto.OpOk)

		rDen := iden.Copy().(*Dentry)
		rDen.multiSnap = NewDentrySnap(verSeq)
		handle, err = mp.dentryTree.CreateBatchWriteHandle()
		require.NoError(t, err)
		rspDelDen, err = mp.fsmDeleteDentry(handle, rDen, false)
		require.NoError(t, err)
		assert.True(t, rspDelDen.Status == proto.OpOk)
		err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
		require.NoError(t, err)
	}
}

func testDentry(t *testing.T) {
	var denArry []*Dentry
	// err := gohook.HookMethod(mp, "submit", MockSubmitTrue, nil)
	mp.config.Cursor = 1100
	//--------------------build dir and it's child on different version ------------------
	seq0 := testCreateVer()
	dirIno := testCreateInode(t, DirModeType)
	assert.True(t, dirIno != nil)
	dirDen := testCreateDentry(t, 1, dirIno.Inode, "testDir", DirModeType)
	assert.True(t, dirDen != nil)

	fIno := testCreateInode(t, FileModeType)
	assert.True(t, fIno != nil)
	fDen := testCreateDentry(t, dirIno.Inode, fIno.Inode, "testfile", FileModeType)
	denArry = append(denArry, fDen)

	//--------------------------------------
	seq1 := testCreateVer()
	fIno1 := testCreateInode(t, FileModeType)
	fDen1 := testCreateDentry(t, dirIno.Inode, fIno1.Inode, "testfile2", FileModeType)
	denArry = append(denArry, fDen1)

	//--------------------------------------
	seq2 := testCreateVer()
	fIno2 := testCreateInode(t, FileModeType)
	fDen2 := testCreateDentry(t, dirIno.Inode, fIno2.Inode, "testfile3", FileModeType)
	denArry = append(denArry, fDen2)

	//--------------------------------------
	seq3 := testCreateVer()
	//--------------------read dir and it's child on different version ------------------

	t.Logf("TestDentry seq [%v],%v,uncommit %v,dir:%v, dentry {%v],inode[%v,%v,%v]", seq1, seq2, seq3, dirDen, denArry, fIno, fIno1, fIno2)
	//-----------read curr version --
	rspReadDir := testReadDirAll(t, 0, 1)
	t.Logf("len child %v, len arry %v", len(rspReadDir.Children), len(denArry))
	assert.True(t, len(rspReadDir.Children) == 1)
	assert.True(t, isDentryEqual(&rspReadDir.Children[0], dirDen))

	rspReadDir = testReadDirAll(t, 0, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == len(denArry))

	for idx, info := range rspReadDir.Children {
		t.Logf("getinfo:%v, expect:%v", info, denArry[idx])
		assert.True(t, isDentryEqual(&info, denArry[idx]))
	}

	//-----------read 0 version --
	rspReadDir = testReadDirAll(t, math.MaxUint64, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 0)

	//-----------read layer 1 version --   seq2 is the last layer, seq1 is the second layer
	rspReadDir = testReadDirAll(t, seq1, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 2)
	for idx, info := range rspReadDir.Children {
		t.Logf("getinfo:%v, expect:%v", info, denArry[idx])
		assert.True(t, isDentryEqual(&info, denArry[idx]))
	}
	//-----------read layer 2 version --
	rspReadDir = testReadDirAll(t, seq0, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 1)
	assert.True(t, isDentryEqual(&rspReadDir.Children[0], fDen))

	testPrintAllDentry(t)
	//--------------------del snapshot and read dir and it's child on different version(cann't be work on interfrace) ------------------
	t.Logf("try testDelDirSnapshotVersion %v", seq0)
	log.LogDebugf("try testDelDirSnapshotVersion %v", seq0)

	testDelDirSnapshotVersion(t, seq0, dirIno, dirDen)
	rspReadDir = testReadDirAll(t, seq0, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 0)

	testPrintAllDentry(t)
	//---------------------------------------------
	t.Logf("try testDelDirSnapshotVersion 0 top layer")
	log.LogDebugf("try testDelDirSnapshotVersion 0")
	testDelDirSnapshotVersion(t, 0, dirIno, dirDen)
	rspReadDir = testReadDirAll(t, 0, dirIno.Inode)
	t.Logf("after  testDelDirSnapshotVersion read seq [%v] can see file %v %v", 0, len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 0)
	rspReadDir = testReadDirAll(t, seq1, dirIno.Inode)
	t.Logf("after  testDelDirSnapshotVersion read seq [%v] can see file %v %v", seq1, len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 2)

	//---------------------------------------------
	t.Logf("try testDelDirSnapshotVersion %v", seq1)
	log.LogDebugf("try testDelDirSnapshotVersion %v", seq1)
	testDelDirSnapshotVersion(t, seq1, dirIno, dirDen)

	rspReadDir = testReadDirAll(t, seq1, dirIno.Inode)
	t.Logf("after  testDelDirSnapshotVersion %v can see file %v %v", seq1, len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 0)
	testPrintAllSysVerList(t)

	//---------------------------------------------
	t.Logf("try testDelDirSnapshotVersion %v", seq2)
	log.LogDebugf("try testDelDirSnapshotVersion %v", seq2)
	testDelDirSnapshotVersion(t, seq2, dirIno, dirDen)

	rspReadDir = testReadDirAll(t, seq2, dirIno.Inode)
	t.Logf("after  testDelDirSnapshotVersion %v can see file %v %v", seq1, len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 0)

	t.Logf("testPrintAllSysVerList")
	testPrintAllSysVerList(t)
	t.Logf("testPrintAllInodeInfo")
	testPrintAllInodeInfo(t)
}

func TestDentry(t *testing.T) {
	initMp(t, proto.StoreModeMem)
	testDentry(t)
}

func TestDentry_Rocksdb(t *testing.T) {
	initMp(t, proto.StoreModeRocksDb)
	testDentry(t)
}

func testPrintDirTree(t *testing.T, parentId uint64, path string, verSeq uint64) (dirCnt int, fCnt int) {
	if verSeq == 0 {
		verSeq = math.MaxUint64
	}
	rspReadDir := testReadDirAll(t, verSeq, parentId)
	for _, child := range rspReadDir.Children {
		pathInner := fmt.Sprintf("%v/%v", path, child.Name)
		if proto.IsDir(child.Type) {
			dirCnt++
			dc, fc := testPrintDirTree(t, child.Inode, pathInner, verSeq)
			dirCnt += dc
			fCnt += fc
			t.Logf("dir:%v", pathInner)
		} else {
			fCnt++
			t.Logf("file:%v", pathInner)
		}
	}
	return
}

func testAppendExt(t *testing.T, seq uint64, idx int, inode uint64) {
	exts := buildExtents(seq, uint64(idx*1000), uint64(idx))
	t.Logf("buildExtents exts[%v]", exts)
	iTmp := &Inode{
		Inode: inode,
		// ObjExtents: NewSortedObjExtents(),
		multiSnap: &InodeMultiSnap{
			verSeq: seq,
		},
		HybridCloudExtents: NewSortedHybridCloudExtentsExt(&SortedExtents{
			eks: exts,
		}),
		StorageClass: proto.StorageClass_Replica_HDD,
	}
	mp.verSeq = seq
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	status, err := mp.fsmAppendExtentsWithCheck(handle, iTmp, false)
	require.NoError(t, err)
	if status != proto.OpOk {
		t.Errorf("status [%v]", status)
	}
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
}

func testTruncateAndDel(t *testing.T) {
	mp.config.Cursor = 1100
	//--------------------build dir and it's child on different version ------------------
	initVer()
	fileIno := testCreateInode(t, FileModeType)
	assert.True(t, fileIno != nil)
	dirDen := testCreateDentry(t, 1, fileIno.Inode, "testDir", FileModeType)
	assert.True(t, dirDen != nil)
	log.LogDebugf("TestTruncate start")
	testAppendExt(t, 0, 0, fileIno.Inode)
	log.LogDebugf("TestTruncate start")
	seq1 := testCreateVer() // seq1 is NOT commited

	seq2 := testCreateVer() // seq1 is commited,seq2 not commited

	t.Logf("TestTruncate. create new snapshot seq [%v],%v,file verlist [%v]", seq1, seq2, fileIno.getLayerLen())
	log.LogDebugf("TestTruncate start")
	ino := &Inode{
		Inode:      fileIno.Inode,
		Size:       500,
		ModifyTime: time.Now().Unix(),
	}
	handle, err := mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	mp.fsmExtentsTruncate(handle, ino)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)
	fileIno, err = mp.inodeTree.Get(fileIno)
	require.NoError(t, err)
	require.NotNil(t, fileIno)
	log.LogDebugf("TestTruncate start")
	//TODO: leonrayang hybrid cloud support snapshot
	//t.Logf("TestTruncate. create new snapshot seq [%v],%v,file verlist size %v [%v]", seq1, seq2, len(fileIno.multiSnap.multiVersions), fileIno.multiSnap.multiVersions)
	//
	//assert.True(t, 2 == len(fileIno.multiSnap.multiVersions))
	//rsp := testGetExtList(t, fileIno, 0)
	//assert.True(t, rsp.Size == 500)
	//
	//rsp = testGetExtList(t, fileIno, seq2)
	//assert.True(t, rsp.Size == 500)
	//
	//rsp = testGetExtList(t, fileIno, seq1)
	//assert.True(t, rsp.Size == 1000)
	//
	//rsp = testGetExtList(t, fileIno, math.MaxUint64)
	//assert.True(t, rsp.Size == 1000)
	//
	//// -------------------------------------------------------
	//log.LogDebugf("TestTruncate start")
	//testCreateVer() // seq2 IS commited, seq3 not
	//mp.fsmUnlinkInode(ino, 0)
	//
	//log.LogDebugf("TestTruncate start")
	//assert.True(t, 3 == len(fileIno.multiSnap.multiVersions))
	//rsp = testGetExtList(t, fileIno, 0)
	//assert.True(t, len(rsp.Extents) == 0)
	//
	//rsp = testGetExtList(t, fileIno, seq2)
	//assert.True(t, rsp.Size == 500)
	//
	//rsp = testGetExtList(t, fileIno, seq1)
	//assert.True(t, rsp.Size == 1000)
	//
	//rsp = testGetExtList(t, fileIno, math.MaxUint64)
	//assert.True(t, rsp.Size == 1000)
}

func TestTruncateAndDel(t *testing.T) {
	log.LogDebugf("TestTruncate start")
	initMp(t, proto.StoreModeMem)
	testTruncateAndDel(t)
}

func TestTruncateAndDel_Rocksdb(t *testing.T) {
	log.LogDebugf("TestTruncate start")
	initMp(t, proto.StoreModeRocksDb)
	testTruncateAndDel(t)
}

func testDeleteFile(t *testing.T, verSeq uint64, parentId uint64, child *proto.Dentry) {
	t.Logf("testDeleteFile seq [%v]", verSeq)
	fsmDentry := &Dentry{
		ParentId:  parentId,
		Name:      child.Name,
		Inode:     child.Inode,
		Type:      child.Type,
		multiSnap: NewDentrySnap(verSeq),
	}
	t.Logf("testDeleteFile seq [%v] %v dentry %v", verSeq, fsmDentry.getSeqFiled(), fsmDentry)
	handle, err := mp.dentryTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	resp, err := mp.fsmDeleteDentry(handle, fsmDentry, false)
	require.NoError(t, err)
	require.NotEqual(t, resp, nil)
	err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	rino := &Inode{
		Inode: child.Inode,
		Type:  child.Type,
		multiSnap: &InodeMultiSnap{
			verSeq: verSeq,
		},
	}
	rino.setVer(verSeq)
	handle, err = mp.inodeTree.CreateBatchWriteHandle()
	require.NoError(t, err)
	rspDelIno, err := mp.fsmUnlinkInode(handle, rino, 0)
	require.NoError(t, err)
	err = mp.inodeTree.CommitAndReleaseBatchWriteHandle(handle, false)
	require.NoError(t, err)

	assert.True(t, rspDelIno.Status == proto.OpOk || rspDelIno.Status == proto.OpNotExistErr)
	if rspDelIno.Status != proto.OpOk && rspDelIno.Status != proto.OpNotExistErr {
		t.Logf("testDelDirSnapshotVersion: rspDelino[%v] return st %v", rspDelIno, proto.ParseErrorCode(int32(rspDelIno.Status)))
		panic(nil)
	}
}

func testDeleteDirTree(t *testing.T, parentId uint64, verSeq uint64) {
	t.Logf("testDeleteDirTree parentId %v seq [%v]", parentId, verSeq)
	rspReadDir := testReadDirAll(t, verSeq, parentId)
	for _, child := range rspReadDir.Children {
		if proto.IsDir(child.Type) {
			testDeleteDirTree(t, child.Inode, verSeq)
		}
		t.Logf("action[testDeleteDirTree] delete children %v", child)
		log.LogDebugf("action[testDeleteDirTree] seq [%v] delete children %v", verSeq, child)
		testDeleteFile(t, verSeq, parentId, &child)
	}
}

func testCleanSnapshot(t *testing.T, verSeq uint64) {
	t.Logf("action[testCleanSnapshot] verseq [%v]", verSeq)
	log.LogDebugf("action[testCleanSnapshot] verseq [%v]", verSeq)
	assert.True(t, testVerListRemoveVer(t, verSeq))
	if verSeq == 0 {
		verSeq = math.MaxUint64
	}
	testDeleteDirTree(t, 1, verSeq)
}

// create
func testSnapshotDeletion(t *testing.T, topFirst bool, storeMode proto.StoreMode) {
	log.LogDebugf("action[TestSnapshotDeletion] start!!!!!!!!!!!")
	initMp(t, storeMode)
	initVer()
	// err := gohook.HookMethod(mp, "submit", MockSubmitTrue, nil)
	mp.config.Cursor = 1100
	//--------------------build dir and it's child on different version ------------------

	dirLayCnt := 4
	var (
		dirName      string
		dirInoId     uint64 = 1
		verArr       []uint64
		renameDen    *Dentry
		renameDstIno uint64
		dirCnt       int
		fileCnt      int
	)

	for layIdx := 0; layIdx < dirLayCnt; layIdx++ {
		t.Logf("build tree:layer %v,last dir name %v inodeid %v", layIdx, dirName, dirInoId)
		dirIno := testCreateInode(t, DirModeType)
		assert.True(t, dirIno != nil)
		dirName = fmt.Sprintf("dir_layer_%v_1", layIdx+1)
		dirDen := testCreateDentry(t, dirInoId, dirIno.Inode, dirName, DirModeType)
		assert.True(t, dirDen != nil)
		if dirDen == nil {
			panic(nil)
		}
		dirIno1 := testCreateInode(t, DirModeType)
		assert.True(t, dirIno1 != nil)
		dirName1 := fmt.Sprintf("dir_layer_%v_2", layIdx+1)
		dirDen1 := testCreateDentry(t, dirInoId, dirIno1.Inode, dirName1, DirModeType)
		assert.True(t, dirDen1 != nil)

		if layIdx == 2 {
			renameDen = dirDen.Copy().(*Dentry)
		}
		if layIdx == 1 {
			renameDstIno = dirIno1.Inode
		}
		for fileIdx := 0; fileIdx < (layIdx+1)*2; fileIdx++ {
			fileIno := testCreateInode(t, FileModeType)
			assert.True(t, dirIno != nil)

			fileName := fmt.Sprintf("layer_%v_file_%v", layIdx+1, fileIdx+1)
			dirDen = testCreateDentry(t, dirIno.Inode, fileIno.Inode, fileName, FileModeType)
			assert.True(t, dirDen != nil)
		}
		dirInoId = dirIno.Inode
		ver := testGetlastVer()
		verArr = append(verArr, ver)

		dCnt, fCnt := testPrintDirTree(t, 1, "root", ver)
		if layIdx+1 < dirLayCnt {
			log.LogDebugf("testCreateVer")
			testCreateVer()
		}

		log.LogDebugf("PrintALl verseq [%v] get dirCnt %v, fCnt %v mp verlist size %v", ver, dCnt, fCnt, len(mp.multiVersionList.VerList))
	}

	t.Logf("---------------------------------------------------------------------")
	t.Logf("--------testPrintDirTree by ver -------------------------------------")
	t.Logf("---------------------------------------------------------------------")
	for idx, ver := range verArr {
		dCnt, fCnt := testPrintDirTree(t, 1, "root", ver)
		t.Logf("---------------------------------------------------------------------")
		t.Logf("PrintALl verseq [%v] get dirCnt %v, fCnt %v", ver, dCnt, fCnt)
		assert.True(t, dCnt == dirCnt+2)
		assert.True(t, fCnt == fileCnt+(idx+1)*2)
		dirCnt = dCnt
		fileCnt = fCnt
	}
	t.Logf("------------rename dir ----------------------")
	if renameDen != nil {
		t.Logf("try to move dir %v", renameDen)
		handle, err := mp.dentryTree.CreateBatchWriteHandle()
		require.NoError(t, err)
		resp, err := mp.fsmDeleteDentry(handle, renameDen, false)
		require.NoError(t, err)
		require.NotEqual(t, resp, nil)
		err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
		require.NoError(t, err)
		renameDen.Name = fmt.Sprintf("rename_from_%v", renameDen.Name)
		renameDen.ParentId = renameDstIno

		t.Logf("try to move to dir %v", renameDen)
		handle, err = mp.dentryTree.CreateBatchWriteHandle()
		require.NoError(t, err)
		status, err := mp.fsmCreateDentry(handle, renameDen, false)
		require.NoError(t, err)
		require.Equal(t, status, proto.OpOk)
		err = mp.dentryTree.CommitAndReleaseBatchWriteHandle(handle, false)
		require.NoError(t, err)
		testPrintDirTree(t, 1, "root", 0)
	}
	delSnapshotList := func() {
		t.Logf("---------------------------------------------------------------------")
		t.Logf("--------testCleanSnapshot by ver-------------------------------------")
		t.Logf("---------------------------------------------------------------------")
		for idx, ver := range verArr {
			t.Logf("---------------------------------------------------------------------")
			t.Logf("index %v ver [%v] try to deletion", idx, ver)
			log.LogDebugf("index %v ver [%v] try to deletion", idx, ver)
			t.Logf("---------------------------------------------------------------------")
			testCleanSnapshot(t, ver)
			t.Logf("---------------------------------------------------------------------")
			if idx == len(verArr)-2 {
				break
			}
		}
	}
	delCurrent := func() {
		t.Logf("---------------------------------------------------------------------")
		t.Logf("--------testDeleteAll current -------------------------------------")
		t.Logf("---------------------------------------------------------------------")
		log.LogDebugf("try to deletion current")
		testDeleteDirTree(t, 1, 0)
		log.LogDebugf("try to deletion current finish")
	}

	if topFirst {
		delCurrent()
		delSnapshotList()
	} else {
		delSnapshotList()
		delCurrent()
	}

	// base on 3.2.0 the dir will push to freelist, not count in in later release version
	// assert.True(t, mp.freeList.Len() == fileCnt+dirCnt)
	assert.True(t, 0 == testPrintAllDentry(t))
	t.Logf("---------------------------------------------------------------------")

	t.Logf("---------------------------------------------------------------------")
	t.Logf("--------testPrintAllInodeInfo should have no inode -------------------------------------")
	t.Logf("---------------------------------------------------------------------")
	testPrintAllInodeInfo(t)
	t.Logf("---------------------------------------------")
	// assert.True(t, false)
}

// create
func TestSnapshotDeletion(t *testing.T) {
	testSnapshotDeletion(t, true, proto.StoreModeMem)
	testSnapshotDeletion(t, false, proto.StoreModeMem)
}

func TestSnapshotDeletion_Rocksdb(t *testing.T) {
	testSnapshotDeletion(t, true, proto.StoreModeRocksDb)
	testSnapshotDeletion(t, false, proto.StoreModeRocksDb)
}

func testDentryVerMarshal(t *testing.T) {
	mp.verSeq = 10
	den1 := &Dentry{
		ParentId:  1,
		Name:      "txt",
		Inode:     10,
		multiSnap: NewDentrySnap(mp.GetVerSeq()),
	}
	val, err := den1.Marshal()
	if err != nil {
		return
	}

	den2 := &Dentry{}
	den2.Unmarshal(val)
	t.Logf("seq, %v %v,parent %v", den1.getVerSeq(), den2.getVerSeq(), den2.ParentId)
	assert.True(t, den1.getVerSeq() == den2.getVerSeq())
	assert.True(t, reflect.DeepEqual(den1, den2))
}

func TestDentryVerMarshal(t *testing.T) {
	initMp(t, proto.StoreModeMem)
	testDentryVerMarshal(t)
}

func TestDentryVerMarshal_Rocksdb(t *testing.T) {
	initMp(t, proto.StoreModeRocksDb)
	testDentryVerMarshal(t)
}

func testInodeVerMarshal(t *testing.T) {
	var topSeq uint64 = 10
	var sndSeq uint64 = 2
	mp.verSeq = 100000
	ino1 := NewInode(10, 5)
	ino1.StorageClass = proto.StorageClass_Replica_HDD
	ino1.PoolId = proto.DefaultHDDPoolId
	ino1.setVer(topSeq)
	ino1_1 := NewInode(10, 5)
	ino1_1.setVer(sndSeq)
	ino1_1.PoolId = proto.DefaultHDDPoolId
	ino1_1.StorageClass = proto.StorageClass_Replica_HDD
	ino1.multiSnap.multiVersions = append(ino1.multiSnap.multiVersions, ino1_1)
	v1, err := ino1.Marshal()
	require.NoError(t, err)

	ino2 := NewInode(0, 0)
	err = ino2.Unmarshal(v1)
	require.NoError(t, err)
	assert.True(t, ino2.getVer() == topSeq)
	assert.True(t, ino2.getLayerLen() == ino1.getLayerLen())
	assert.True(t, ino2.getLayerVer(0) == sndSeq)
	// TODO:leonrayang
	// assert.True(t, reflect.DeepEqual(ino1, ino2))
}

func TestInodeVerMarshal(t *testing.T) {
	initMp(t, proto.StoreModeMem)
	testInodeVerMarshal(t)
}

func TestInodeVerMarshal_Rocksdb(t *testing.T) {
	initMp(t, proto.StoreModeRocksDb)
	testInodeVerMarshal(t)
}

func TestSplitKey(t *testing.T) {
	dp := &DataPartition{
		PartitionID: 10,
		Hosts:       []string{"192.168.0.1", "192.168.0.2", "192.168.0.3"},
	}
	ext := &proto.ExtentKey{
		ExtentId:     28,
		ExtentOffset: 10,
		Size:         util.PageSize,
		SnapInfo: &proto.ExtSnapInfo{
			IsSplit: true,
		},
	}
	_, invalid := NewPacketToDeleteExtent(dp, ext)
	assert.True(t, invalid == true)

	ext.ExtentOffset = 0
	_, invalid = NewPacketToDeleteExtent(dp, ext)
	assert.True(t, invalid == false)

	ext.ExtentOffset = 10
	ext.Size = 2 * util.PageSize
	_, invalid = NewPacketToDeleteExtent(dp, ext)
	assert.True(t, invalid == false)
}

func NewMetaPartitionForTest(storeMode proto.StoreMode) *metaPartition {
	mpC := &MetaPartitionConfig{
		PartitionId: PartitionIdForTest,
		VolName:     VolNameForTest,
		StoreMode:   storeMode,
	}
	if mpC.StoreMode == proto.StoreModeRocksDb {
		mpC.RocksDBDir = fmt.Sprintf("%v/%v_%v", RocksdbMultiVerTestDir, partitionId, time.Now().UnixMilli())
	}
	partition := NewMetaPartition(mpC, nil).(*metaPartition)
	partition.rocksdbManager = NewPerDiskRocksdbManager(&RocksdbManagerConfig{})
	err := partition.rocksdbManager.Register(mpC.RocksDBDir)
	if err != nil {
		panic(err)
	}
	err = partition.initObjects(true)
	if err != nil {
		panic(err)
	}
	partition.uniqChecker.keepTime = 1
	partition.uniqChecker.keepOps = 0
	partition.mqMgr = NewQuotaManager(VolNameForTest, 1)
	return partition
}

func mockPartitionRaftForTest(ctrl *gomock.Controller, storeMode proto.StoreMode) *metaPartition {
	partition := NewMetaPartitionForTest(storeMode)
	raft := raftstoremock.NewMockPartition(ctrl)
	idx := uint64(0)
	raft.EXPECT().Submit(gomock.Any()).DoAndReturn(func(cmd []byte) (resp interface{}, err error) {
		idx++
		return partition.Apply(cmd, idx)
	}).AnyTimes()

	raft.EXPECT().IsRaftLeader().DoAndReturn(func(cmd []byte) (resp interface{}, err error) {
		return true, nil
	}).AnyTimes()

	raft.EXPECT().LeaderTerm().Return(uint64(1), uint64(1)).AnyTimes()
	partition.raftPartition = raft
	return partition
}

func testCheckVerList(t *testing.T) {
	mp.multiVersionList.VerList = append(mp.multiVersionList.VerList,
		[]*proto.VolVersionInfo{
			{Ver: 20, Status: proto.VersionNormal},
			{Ver: 30, Status: proto.VersionNormal},
			{Ver: 40, Status: proto.VersionNormal},
		}...)

	masterList := &proto.VolVersionInfoList{
		VerList: []*proto.VolVersionInfo{
			{Ver: 0, Status: proto.VersionNormal},
			{Ver: 20, Status: proto.VersionNormal},
			{Ver: 30, Status: proto.VersionNormal},
			{Ver: 40, Status: proto.VersionNormal},
			{Ver: 50, Status: proto.VersionNormal},
		},
	}
	var verData []byte
	mp.checkVerList(masterList, false)
	verData = <-mp.verUpdateChan
	mp.submit(opFSMVersionOp, verData)
	assert.True(t, mp.verSeq == 50)
	assert.True(t, mp.multiVersionList.VerList[len(mp.multiVersionList.VerList)-1].Ver == 50)

	masterList = &proto.VolVersionInfoList{
		VerList: []*proto.VolVersionInfo{
			{Ver: 20, Status: proto.VersionNormal},
			{Ver: 40, Status: proto.VersionNormal},
		},
	}

	needUpdate, _ := mp.checkVerList(masterList, false)
	assert.True(t, needUpdate == false)

	assert.True(t, mp.verSeq == 50)
	assert.True(t, len(mp.multiVersionList.VerList) == 5)
	mp.stop()
}

func TestCheckVerList(t *testing.T) {
	newMpWithMock(t, proto.StoreModeMem)
	testCheckVerList(t)
}

func TestCheckVerList_Rocksdb(t *testing.T) {
	newMpWithMock(t, proto.StoreModeRocksDb)
	testCheckVerList(t)
}

func checkStoreMode(t *testing.T, ExtentType uint8) (err error) {
	if proto.IsTinyExtentType(ExtentType) || proto.IsNormalExtentType(ExtentType) {
		return
	}
	t.Logf("action[checkStoreMode] extent type %v", ExtentType)
	return fmt.Errorf("error")
}

func TestCheckMod(t *testing.T) {
	var tp uint8 = 192
	err := checkStoreMode(t, tp)
	assert.True(t, err == nil)
}

func managerVersionPrepare(manager *metadataManager, req *proto.MultiVersionOpRequest) (err error) {
	if err, _ = manager.prepareCreateVersion(req); err != nil {
		return
	}
	return manager.commitCreateVersion(req.VolumeID, req.VerSeq, req.Op, true)
}

func newMpWithMock(t *testing.T, storeMode proto.StoreMode) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mp = mockPartitionRaftForTest(mockCtrl, storeMode)

	mp.verUpdateChan = make(chan []byte, 100)
	mp.config = getMpConfig(storeMode)
	mp.config.Cursor = 0
	mp.config.End = 100000
	mp.uidManager = NewUidMgr(metaConf.VolName, metaConf.PartitionId)
	mp.mqMgr = NewQuotaManager(metaConf.VolName, metaConf.PartitionId)
	mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, &proto.VolVersionInfo{
		Ver: 0,
	})
	mp.multiVersionList.TemporaryVerMap = make(map[uint64]*proto.VolVersionInfo)
}

func testOpCommitVersion(t *testing.T, storeMode proto.StoreMode) {
	mockCtrl := gomock.NewController(t)
	manager := newManager()
	defer mockCtrl.Finish()
	for i := 1; i < 5; i++ {
		mp = mockPartitionRaftForTest(mockCtrl, storeMode)
		mp.config.PartitionId = uint64(i)
		mp.manager = manager
		mp.manager.partitions[mp.config.PartitionId] = mp
		mp.config.NodeId = 1
	}

	err := managerVersionPrepare(manager, &proto.MultiVersionOpRequest{VolumeID: VolNameForTest, Op: proto.CreateVersionPrepare, VerSeq: 10000})
	for _, m := range manager.partitions {
		mList := m.GetVerList()
		t.Logf("mp(%v) version(%v)", m.GetBaseConfig().PartitionId, mList)
	}
	require.NoError(t, err)
	for _, m := range manager.partitions {
		mList := m.GetVerList()
		require.EqualValues(t, 1, len(mList))
		require.EqualValues(t, 10000, mList[0].Ver)
		require.EqualValues(t, proto.VersionPrepare, mList[0].Status)
	}
	err = manager.commitCreateVersion(VolNameForTest, 10000, proto.CreateVersionPrepare, true)
	assert.True(t, err == nil)
	for _, m := range manager.partitions {
		mList := m.GetVerList()
		assert.True(t, len(mList) == 1)
		assert.True(t, mList[0].Ver == 10000)
		assert.True(t, mList[0].Status == proto.VersionPrepare)
	}
	err = managerVersionPrepare(manager, &proto.MultiVersionOpRequest{VolumeID: VolNameForTest, Op: proto.CreateVersionPrepare, VerSeq: 5000})
	assert.True(t, err == nil)
	for _, m := range manager.partitions {
		mList := m.GetVerList()
		assert.True(t, len(mList) == 1)
		assert.True(t, mList[0].Ver == 10000)
		assert.True(t, mList[0].Status == proto.VersionPrepare)
	}
	err = managerVersionPrepare(manager, &proto.MultiVersionOpRequest{VolumeID: VolNameForTest, Op: proto.CreateVersionPrepare, VerSeq: 20000})
	assert.True(t, err == nil)
	for _, m := range manager.partitions {
		mList := m.GetVerList()
		assert.True(t, len(mList) == 2)
		assert.True(t, mList[0].Ver == 10000)
		assert.True(t, mList[0].Status == proto.VersionPrepare)
		assert.True(t, mList[1].Ver == 20000)
		assert.True(t, mList[1].Status == proto.VersionPrepare)
	}
	err = manager.commitCreateVersion(VolNameForTest, 20000, proto.CreateVersionCommit, true)
	assert.True(t, err == nil)
	for _, m := range manager.partitions {
		mList := m.GetVerList()
		assert.True(t, len(mList) == 2)
		assert.True(t, mList[0].Ver == 10000)
		assert.True(t, mList[0].Status == proto.VersionPrepare)
		assert.True(t, mList[1].Ver == 20000)
		assert.True(t, mList[1].Status == proto.VersionNormal)
	}
}

func TestOpCommitVersion(t *testing.T) {
	testOpCommitVersion(t, proto.StoreModeMem)
}

func TestOpCommitVersion_Rocksdb(t *testing.T) {
	testOpCommitVersion(t, proto.StoreModeRocksDb)
}

func TestExtendSerialization(t *testing.T) {
	dataMap := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}
	mv := &Extend{
		inode:   123,
		dataMap: dataMap,
		multiSnap: &ExtendMultiSnap{
			verSeq: 456,
		},
	}

	checkFunc := func() {
		bytes, err := mv.Bytes()
		if err != nil {
			t.Errorf("Failed to serialize Extend: %v", err)
		}

		newExtend, err := NewExtendFromBytes(bytes)
		if err != nil {
			t.Errorf("Failed to deserialize Extend: %v", err)
		}

		if !reflect.DeepEqual(mv, newExtend) {
			t.Errorf("Deserialized Extend does not match the original object")
		}
	}

	checkFunc()
	mv.multiSnap = &ExtendMultiSnap{verSeq: 101}
	mv.multiSnap.multiVers = []*Extend{
		{
			inode:     789,
			dataMap:   map[string][]byte{"key3": []byte("value3")},
			multiSnap: &ExtendMultiSnap{verSeq: 999},
		},
		{
			inode:     789,
			dataMap:   map[string][]byte{"key4": []byte("value4")},
			multiSnap: &ExtendMultiSnap{verSeq: 1999},
		},
	}
	checkFunc()
}

func testXAttrOperation(t *testing.T) {
	mp.SetXAttr(&proto.SetXAttrRequest{Key: "test", Value: "value"}, &Packet{})
	mp.SetXAttr(&proto.SetXAttrRequest{Key: "test1", Value: "value1"}, &Packet{})

	testCreateVer()

	// operation on top of snapshot version
	err := mp.SetXAttr(&proto.SetXAttrRequest{Key: "test1", Value: "value2"}, &Packet{})
	assert.True(t, err == nil)
	packRsp := &Packet{}
	mp.GetXAttr(&proto.GetXAttrRequest{Key: "test1", VerSeq: 0}, packRsp)
	assert.True(t, packRsp.ResultCode == proto.OpOk)
	resp := new(proto.GetXAttrResponse)
	err = packRsp.UnmarshalData(resp)
	assert.True(t, err == nil)
	assert.True(t, resp.Value == "value2")

	// remove test1 but it should exist in snapshot
	err = mp.RemoveXAttr(&proto.RemoveXAttrRequest{Key: "test1"}, &Packet{})
	assert.True(t, err == nil)

	mp.GetXAttr(&proto.GetXAttrRequest{Key: "test1", VerSeq: 0}, packRsp)
	assert.True(t, packRsp.ResultCode == proto.OpOk)
	err = packRsp.UnmarshalData(resp)
	assert.True(t, err == nil)
	assert.True(t, resp.Value == "")

	// get snapshot xattr the 0 version
	packRsp = &Packet{}
	mp.GetXAttr(&proto.GetXAttrRequest{Key: "test1", VerSeq: math.MaxUint64}, packRsp)
	assert.True(t, packRsp.ResultCode == proto.OpOk)

	resp = new(proto.GetXAttrResponse)
	err = packRsp.UnmarshalData(resp)
	assert.True(t, err == nil)
	assert.True(t, resp.Value == "value1")
}

func TestXAttrOperation(t *testing.T) {
	newMpWithMock(t, proto.StoreModeMem)
	testXAttrOperation(t)
}

func TestXAttrOperation_Rocksdb(t *testing.T) {
	newMpWithMock(t, proto.StoreModeRocksDb)
	testXAttrOperation(t)
}

func testUpdateDenty(t *testing.T) {
	testCreateInode(nil, DirModeType)
	err := mp.CreateDentry(&CreateDentryReq{Name: "testfile", ParentID: 1, Inode: 1000}, &Packet{}, localAddrForAudit)
	assert.True(t, err == nil)
	testCreateVer()
	mp.UpdateDentry(&UpdateDentryReq{Name: "testfile", ParentID: 1, Inode: 2000}, &Packet{}, localAddrForAudit)
	den := &Dentry{Name: "testfile", ParentId: 1}
	den.setVerSeq(math.MaxUint64)
	denRsp, status, err := mp.getDentry(den)
	require.NoError(t, err)
	assert.True(t, status == proto.OpOk)
	assert.True(t, denRsp.Inode == 1000)
}

func TestUpdateDenty(t *testing.T) {
	newMpWithMock(t, proto.StoreModeMem)
	testUpdateDenty(t)
}

func TestUpdateDenty_Rocksdb(t *testing.T) {
	newMpWithMock(t, proto.StoreModeRocksDb)
	testUpdateDenty(t)
}

func TestCheckEkEqual(t *testing.T) {
	ek1 := &proto.ExtentKey{FileOffset: 10, SnapInfo: &proto.ExtSnapInfo{VerSeq: 10, IsSplit: true}}
	ek2 := &proto.ExtentKey{FileOffset: 10, SnapInfo: &proto.ExtSnapInfo{VerSeq: 10, IsSplit: true}}
	assert.True(t, ek1.Equals(ek2))
}

func testDelPartitionVersion(t *testing.T) {
	manager := newManager()
	mp.manager = manager
	mp.manager.partitions[mp.config.PartitionId] = mp
	mp.config.NodeId = 1

	err := managerVersionPrepare(manager, &proto.MultiVersionOpRequest{VolumeID: VolNameForTest, Op: proto.CreateVersionPrepare, VerSeq: 10})
	assert.True(t, err == nil)

	ino := testCreateInode(t, FileModeType)
	assert.True(t, ino.getVer() == 10)
	mp.SetXAttr(&proto.SetXAttrRequest{Inode: ino.Inode, Key: "key1", Value: "0000"}, &Packet{})
	mp.CreateDentry(&CreateDentryReq{Inode: ino.Inode, Name: "dentryName"}, &Packet{}, "/dentryName")

	err = managerVersionPrepare(manager, &proto.MultiVersionOpRequest{VolumeID: VolNameForTest, Op: proto.CreateVersionPrepare, VerSeq: 25})
	mp.SetXAttr(&proto.SetXAttrRequest{Inode: ino.Inode, Key: "key1", Value: "1111"}, &Packet{})

	assert.True(t, err == nil)

	extend, _ := mp.extendTree.Get(NewExtend(ino.Inode))
	assert.True(t, len(extend.multiSnap.multiVers) == 1)

	masterList := &proto.VolVersionInfoList{
		VerList: []*proto.VolVersionInfo{
			{Ver: 20, Status: proto.VersionNormal},
			{Ver: 30, Status: proto.VersionNormal},
			{Ver: 40, Status: proto.VersionNormal},
			{Ver: 50, Status: proto.VersionNormal},
		},
	}
	mp.checkByMasterVerlist(mp.multiVersionList, masterList)
	mp.checkVerList(masterList, true)
	assert.True(t, len(mp.multiVersionList.TemporaryVerMap) == 2)

	mp.SetXAttr(&proto.SetXAttrRequest{Inode: ino.Inode, Key: "key1", Value: "2222"}, &Packet{})

	go mp.multiVersionTTLWork(time.Millisecond * 10)

	cnt := 30
	for cnt > 0 {
		if len(mp.multiVersionList.TemporaryVerMap) != 0 {
			time.Sleep(time.Millisecond * 100)
			cnt--
			continue
		}
		break
	}
	resp := mp.getInode(&Inode{Inode: ino.Inode}, false)
	require.Equal(t, resp.Status, proto.OpOk)
	inoNew := resp.Msg
	assert.True(t, inoNew.getVer() == 25)
	extend, _ = mp.extendTree.Get(NewExtend(ino.Inode))
	t.Logf("extent verseq [%v], multivers %v", extend.getVersion(), extend.multiSnap.multiVers)
	assert.True(t, extend.multiSnap.verSeq == 50)
	// assert.True(t, len(extend.multiSnap.multiVers) == 1)
	assert.True(t, extend.multiSnap.multiVers[0].getVersion() == 25)

	assert.True(t, string(extend.multiSnap.multiVers[0].dataMap["key1"]) == "1111")

	err = extend.checkSequence()
	t.Logf("extent checkSequence err %v", err)
	assert.True(t, err == nil)
	assert.True(t, len(mp.multiVersionList.TemporaryVerMap) == 0)
}

func TestDelPartitionVersion(t *testing.T) {
	newMpWithMock(t, proto.StoreModeMem)
	testDelPartitionVersion(t)
}

func TestDelPartitionVersion_Rocksdb(t *testing.T) {
	newMpWithMock(t, proto.StoreModeRocksDb)
	testDelPartitionVersion(t)
}

func testMpMultiVerStore(t *testing.T) {
	filePath := "/tmp/"
	snap, err := mp.GetSnapShot()
	require.NoError(t, err)
	require.NotNil(t, snap)
	defer snap.Close()
	crc, _ := mp.storeMultiVersion(filePath, &storeMsg{
		multiVerList: []*proto.VolVersionInfo{{Ver: 20, Status: proto.VersionNormal}, {Ver: 30, Status: proto.VersionNormal}},
		snap:         snap,
	})
	err = mp.loadMultiVer(filePath, crc)
	assert.True(t, err == nil)
}

// NOTE: no rocksdb test
func TestMpMultiVerStore(t *testing.T) {
	initMp(t, proto.StoreModeMem)
	testMpMultiVerStore(t)
}

func testGetAllVerList(t *testing.T) {
	mp.multiVersionList = &proto.VolVersionInfoList{
		VerList: []*proto.VolVersionInfo{
			{Ver: 20, Status: proto.VersionNormal},
			{Ver: 30, Status: proto.VersionNormal},
			{Ver: 40, Status: proto.VersionNormal},
			{Ver: 50, Status: proto.VersionNormal},
		},
	}
	tmp := mp.multiVersionList.VerList
	mp.multiVersionList.VerList = append(mp.multiVersionList.VerList[:1], mp.multiVersionList.VerList[2:]...)
	tmp = append(tmp, &proto.VolVersionInfo{Ver: 30, Status: proto.VersionNormal})

	sort.SliceStable(tmp, func(i, j int) bool { return tmp[i].Ver < tmp[j].Ver })

	t.Logf("tmp[%v]", tmp)
	t.Logf("mp.multiVersionList %v", mp.multiVersionList)

	mp.multiVersionList.TemporaryVerMap = make(map[uint64]*proto.VolVersionInfo)
	mp.multiVersionList.TemporaryVerMap[25] = &proto.VolVersionInfo{Ver: 25, Status: proto.VersionNormal}
	mp.multiVersionList.TemporaryVerMap[45] = &proto.VolVersionInfo{Ver: 45, Status: proto.VersionNormal}
	newList := mp.GetAllVerList()
	oldList := mp.multiVersionList.VerList
	t.Logf("newList %v oldList %v", newList, oldList)
	assert.True(t, true)
}

func TestGetAllVerList(t *testing.T) {
	initMp(t, proto.StoreModeMem)
	testGetAllVerList(t)
}

func TestGetAllVerList_Rocksdb(t *testing.T) {
	initMp(t, proto.StoreModeRocksDb)
	testGetAllVerList(t)
}

func TestVerlistSnapshot(t *testing.T) {
	verList := []*proto.VolVersionInfo{
		{Ver: 20, Status: proto.VersionNormal},
	}
	var verListBuf1 []byte
	var err error
	if verListBuf1, err = json.Marshal(verList); err != nil {
		return
	}
	t.Logf("mp.TestVerlistSnapshot  %v", verListBuf1)
	var verList12 []*proto.VolVersionInfo
	if err = json.Unmarshal(verListBuf1, &verList12); err != nil {
		t.Logf("mp.TestVerlistSnapshot  err %v", err)
	}
	t.Logf("mp.TestVerlistSnapshot  %v", verList12)
	assert.True(t, true)
}

func TestCleanRocksdbDir(t *testing.T) {
	os.RemoveAll("/tmp/cfs/logs")
	os.RemoveAll(RocksdbMultiVerTestDir)
}
