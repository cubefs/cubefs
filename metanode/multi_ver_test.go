package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"testing"
	"time"
)

var partitionId uint64 = 10
var ModeDirType uint32 = 2147484141
var ModFileType uint32 = 420
var manager = &metadataManager{}
var mp *metaPartition

//PartitionId   uint64              `json:"partition_id"`
//VolName       string              `json:"vol_name"`
//PartitionType int                 `json:"partition_type"`
var metaConf = &MetaPartitionConfig{
	PartitionId:   10001,
	VolName:       "testVol",
	PartitionType: proto.VolumeTypeHot,
}

const (
	ConfigKeyLogDir   = "logDir"
	ConfigKeyLogLevel = "logLevel"
)

var cfgJSON = `{
		"role": "meta",
		"logDir": "/tmp/chubaofs/Logs",
		"logLevel":"debug",
		"walDir":"/tmp/chubaofs/raft",
		"clusterName":"chubaofs"
	}`

func newPartition(conf *MetaPartitionConfig, manager *metadataManager) (mp *metaPartition) {
	mp = &metaPartition{
		config:        conf,
		dentryTree:    NewBtree(),
		inodeTree:     NewBtree(),
		extendTree:    NewBtree(),
		multipartTree: NewBtree(),
		stopC:         make(chan bool),
		storeChan:     make(chan *storeMsg, 100),
		freeList:      newFreeList(),
		extDelCh:      make(chan []proto.ExtentKey, defaultDelExtentsCnt),
		extReset:      make(chan struct{}),
		vol:           NewVol(),
		manager:       manager,
		verSeq:        conf.VerSeq,
	}
	mp.config.Cursor = 0
	mp.config.End = 100000
	return mp
}

func init() {
	cfg := config.LoadConfigString(cfgJSON)

	logDir := cfg.GetString(ConfigKeyLogDir)
	os.RemoveAll(logDir)

	if _, err := log.InitLog(logDir, "metanode", log.DebugLevel, nil); err != nil {
		fmt.Println("Fatal: failed to start the chubaofs daemon - ", err)
		return
	}
	log.LogDebugf("action start")
	return
}

func initMp(t *testing.T) {
	mp = newPartition(metaConf, manager)
	mp.multiVersionList = &proto.VolVersionInfoList{}
	ino := createInode(nil, ModeDirType)
	t.Logf("cursor %v create ino %v", mp.config.Cursor, ino)
	mp.config.Cursor = 1000
}

func buildExtentKey(seq uint64, foffset uint64, extid uint64, exteoffset uint64, size uint32) proto.ExtentKey {
	return proto.ExtentKey{
		FileOffset:   foffset,
		PartitionId:  partitionId,
		ExtentId:     extid,
		ExtentOffset: exteoffset, // offset in extent like tiny extent offset large than 0,normal is 0
		Size:         size,       // extent real size?
		VerSeq:       seq,
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
		//t.Logf("idx:%v ext:%v, lastFileOff %v, lastSize %v", idx, ext, lastFileOff, lastSize)
		if ext.FileOffset != lastFileOff+uint64(lastSize) {
			t.Errorf("checkOffSetInSequnce not equal idx %v %v:(%v+%v) eks{%v}", idx, ext.FileOffset, lastFileOff, lastSize, eks)
			return false
		}
		lastFileOff = ext.FileOffset
		lastSize = ext.Size
	}
	return true
}

func getExtList(t *testing.T, ino *Inode, verRead uint64) (resp *proto.GetExtentsResponse) {
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

	assert.True(t, packet.ResultCode == proto.OpOk)
	assert.True(t, checkOffSetInSequnce(t, resp.Extents))
	return
}

func checkExtList(t *testing.T, ino *Inode, seqArr []uint64) bool {
	reqExtList := &proto.GetExtentsRequest{
		VolName:     metaConf.VolName,
		PartitionID: partitionId,
		Inode:       ino.Inode,
	}

	for idx, verRead := range seqArr {
		t.Logf("check extlist index %v ver %v", idx, verRead)
		reqExtList.VerSeq = verRead
		getExtRsp := getExtList(t, ino, verRead)
		t.Logf("check extlist rsp %v size %v,%v", getExtRsp, getExtRsp.Size, ino.Size)
		assert.True(t, getExtRsp.Size == uint64(1000*idx))
	}
	return true
}

func createInode(t *testing.T, mode uint32) *Inode {
	inoID, _ := mp.nextInodeID()
	if t != nil {
		t.Logf("inode id:%v", inoID)
	}

	ino := NewInode(inoID, mode)
	ino.verSeq = mp.verSeq
	mp.fsmCreateInode(ino)
	return ino
}

func createDentry(t *testing.T, parentId uint64, inodeId uint64, name string, mod uint32) *Dentry {

	dentry := &Dentry{
		ParentId: parentId,
		Name:     name,
		Inode:    inodeId,
		Type:     mod,
		VerSeq:   mp.verSeq,
	}
	ret := mp.fsmCreateDentry(dentry, false)
	assert.True(t, proto.OpOk == ret)
	if ret != proto.OpOk {
		panic(nil)
	}
	return dentry
}

func createVer() (verSeq uint64) {
	tm := time.Now().Unix()
	verInfo := &proto.VolVersionInfo{
		Ver:    uint64(tm),
		Ctime:  time.Now(),
		Status: proto.VersionNormal,
	}
	mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, verInfo)
	mp.verSeq = verInfo.Ver
	return verInfo.Ver
}

func readDirAll(t *testing.T, verSeq uint64, parentId uint64) (resp *ReadDirLimitResp) {
	//printAllDentry(t)
	//	t.Logf("[readDirAll] with seq %v parentId %v", verSeq, parentId)
	req := &ReadDirLimitReq{
		PartitionID: partitionId,
		VolName:     mp.GetVolName(),
		ParentID:    parentId,
		Limit:       math.MaxUint64,
		VerSeq:      verSeq,
	}
	return mp.readDirLimit(req)
}

func verListRemoveVer(t *testing.T, verSeq uint64) bool {
	printAllSysVerList(t)
	for i, ver := range mp.multiVersionList.VerList {
		if ver.Ver == verSeq {
			// mp.multiVersionList = append(mp.multiVersionList[:i], mp.multiVersionList[i+1:]...)
			mp.multiVersionList.VerList = append(mp.multiVersionList.VerList[:i], mp.multiVersionList.VerList[i+1:]...)
			return true
		}
	}
	return false
}

var ct = uint64(time.Now().Unix())
var seqAllArr = []uint64{0, ct, ct + 2111, ct + 10333, ct + 53456, ct + 60000, ct + 72344, ct + 234424, ct + 334424}

func TestAppendList(t *testing.T) {
	initMp(t)
	for _, verSeq := range seqAllArr {
		verInfo := &proto.VolVersionInfo{
			Ver:    verSeq,
			Ctime:  time.Unix(int64(verSeq), 0),
			Status: proto.VersionNormal,
		}
		mp.multiVersionList.VerList = append(mp.multiVersionList.VerList, verInfo)
	}

	var ino = createInode(t, 0)
	t.Logf("enter TestAppendList")
	index := 5
	seqArr := seqAllArr[1:index]
	t.Logf("layer len %v, arr size %v, seqarr(%v)", len(ino.multiVersions), len(seqArr), seqArr)
	for idx, seq := range seqArr {
		exts := buildExtents(seq, uint64(idx*1000), uint64(idx))
		t.Logf("buildExtents exts[%v]", exts)
		iTmp := &Inode{
			Inode: ino.Inode,
			Extents: &SortedExtents{
				eks: exts,
			},
			ObjExtents: NewSortedObjExtents(),
			verSeq:     seq,
		}
		mp.verSeq = seq

		if status := mp.fsmAppendExtentsWithCheck(iTmp, false); status != proto.OpOk {
			t.Errorf("status %v", status)
		}
	}
	t.Logf("layer len %v, arr size %v, seqarr(%v)", len(ino.multiVersions), len(seqArr), seqArr)
	assert.True(t, len(ino.multiVersions) == len(seqArr))
	assert.True(t, ino.verSeq == mp.verSeq)

	for i := 0; i < len(seqArr)-1; i++ {
		assert.True(t, ino.multiVersions[i].verSeq == seqArr[len(seqArr)-i-2])
		t.Logf("layer %v len %v content %v,seq %v, %v", i, len(ino.multiVersions[i].Extents.eks), ino.multiVersions[i].Extents.eks,
			ino.multiVersions[i].verSeq, seqArr[len(seqArr)-i-2])
		assert.True(t, len(ino.multiVersions[i].Extents.eks) == 0)
	}

	//-------------   split at begin -----------------------------------------
	t.Logf("start split at begin")
	var splitSeq = seqAllArr[index]
	splitKey := buildExtentKey(splitSeq, 0, 0, 128000, 10)
	extents := &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp := &Inode{
		Inode:   ino.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}
	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("in split at begin")
	assert.True(t, ino.multiVersions[0].Extents.eks[0].VerSeq == ino.multiVersions[3].verSeq)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].FileOffset == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].ExtentId == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].ExtentOffset == 0)
	assert.True(t, ino.multiVersions[0].Extents.eks[0].Size == splitKey.Size)

	t.Logf("in split at begin")

	assert.True(t, isExtEqual(ino.Extents.eks[0], splitKey))
	assert.True(t, checkOffSetInSequnce(t, ino.Extents.eks))

	t.Logf("top layer len %v, layer 1 len %v arr size %v", len(ino.Extents.eks), len(ino.multiVersions[0].Extents.eks), len(seqArr))
	assert.True(t, len(ino.multiVersions[0].Extents.eks) == 1)
	assert.True(t, len(ino.Extents.eks) == len(seqArr)+1)

	checkExtList(t, ino, seqArr)

	//--------  split at middle  -----------------------------------------------
	t.Logf("start split at middle")

	lastTopEksLen := len(ino.Extents.eks)
	t.Logf("split at middle lastTopEksLen %v", lastTopEksLen)

	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 500, 0, 128100, 100)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:   ino.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))
	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))

	getExtRsp := getExtList(t, ino, ino.multiVersions[0].verSeq)
	t.Logf("split at middle getExtRsp len %v seq(%v), toplayer len:%v seq(%v)",
		len(getExtRsp.Extents), ino.multiVersions[0].verSeq, len(ino.Extents.eks), ino.verSeq)

	assert.True(t, len(getExtRsp.Extents) == lastTopEksLen+2)
	assert.True(t, len(ino.Extents.eks) == lastTopEksLen+2)
	assert.True(t, checkOffSetInSequnce(t, ino.Extents.eks))

	t.Logf("ino exts{%v}", ino.Extents.eks)

	//--------  split at end  -----------------------------------------------
	t.Logf("start split at end")
	// split at end
	lastTopEksLen = len(ino.Extents.eks)
	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 3900, 3, 129000, 100)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:   ino.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}
	t.Logf("split key:%v", splitKey)
	getExtRsp = getExtList(t, ino, ino.multiVersions[0].verSeq)
	t.Logf("split at middle multiVersions %v, extent %v, level 1 %v", len(ino.multiVersions), getExtRsp.Extents, ino.multiVersions[0].Extents.eks)

	mp.fsmAppendExtentsWithCheck(iTmp, true)
	t.Logf("split at middle multiVersions %v", len(ino.multiVersions))
	getExtRsp = getExtList(t, ino, ino.multiVersions[0].verSeq)
	t.Logf("split at middle multiVersions %v, extent %v, level 1 %v", len(ino.multiVersions), getExtRsp.Extents, ino.multiVersions[0].Extents.eks)

	t.Logf("split at middle getExtRsp len %v seq(%v), toplayer len:%v seq(%v)",
		len(getExtRsp.Extents), ino.multiVersions[0].verSeq, len(ino.Extents.eks), ino.verSeq)

	assert.True(t, len(getExtRsp.Extents) == lastTopEksLen+1)
	assert.True(t, len(ino.Extents.eks) == lastTopEksLen+1)
	assert.True(t, isExtEqual(ino.Extents.eks[lastTopEksLen], splitKey))
	//assert.True(t, false)

	//--------  split at the splited one  -----------------------------------------------
	t.Logf("start split at end")
	// split at end
	lastTopEksLen = len(ino.Extents.eks)
	index++
	splitSeq = seqAllArr[index]
	splitKey = buildExtentKey(splitSeq, 3950, 3, 129000, 20)
	extents = &SortedExtents{}
	extents.eks = append(extents.eks, splitKey)

	iTmp = &Inode{
		Inode:   ino.Inode,
		Extents: extents,
		verSeq:  splitSeq,
	}
	t.Logf("split key:%v", splitKey)
	mp.fsmAppendExtentsWithCheck(iTmp, true)

	getExtRsp = getExtList(t, ino, ino.multiVersions[0].verSeq)

	assert.True(t, len(ino.Extents.eks) == lastTopEksLen+2)
	assert.True(t, checkOffSetInSequnce(t, ino.Extents.eks))
}

//func MockSubmitTrue(mp *metaPartition, inode uint64, offset int, data []byte,
//	flags int) (write int, err error) {
//	return len(data), nil
//}

func printAllSysVerList(t *testing.T) {
	for idx, info := range mp.multiVersionList.VerList {
		t.Logf("printAllSysVerList idx %v, info %v", idx, info)
	}
}

func printAllDentry(t *testing.T) {
	mp.dentryTree.Ascend(func(i BtreeItem) bool {
		den := i.(*Dentry)
		t.Logf("printAllDentry name %v top layer dentry:%v", den.Name, den)
		for id, info := range den.dentryList {
			t.Logf("printAllDentry name %v layer %v, denSeq %v den %v", den.Name, id, info.VerSeq, info)
		}
		return true
	})
}

func PrintAllInodeInfo(t *testing.T) {
	mp.inodeTree.Ascend(func(item BtreeItem) bool {
		i := item.(*Inode)
		t.Logf("action[PrintAllVersionInfo] toplayer inode [%v] verSeq [%v] hist len [%v]", i, i.verSeq, len(i.multiVersions))
		for id, info := range i.multiVersions {
			t.Logf("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode [%v]", id, info.verSeq, info)
		}
		return true
	})
}

func PrintInodeInfo(t *testing.T, ino *Inode) {

	i := mp.inodeTree.Get(ino).(*Inode)
	t.Logf("action[PrintAllVersionInfo] toplayer inode [%v] verSeq [%v] hist len [%v]", i, i.verSeq, len(i.multiVersions))
	for id, info := range i.multiVersions {
		t.Logf("action[PrintAllVersionInfo] layer [%v]  verSeq [%v] inode [%v]", id, info.verSeq, info)
	}

}

func DelVersion(t *testing.T, verSeq uint64, dirIno *Inode, dirDentry *Dentry) {
	if verSeq != 0 {
		assert.True(t, verListRemoveVer(t, verSeq))
	}

	rspReadDir := readDirAll(t, verSeq, dirIno.Inode)
	//printAllDentry(t)

	rDirIno := dirIno.Copy().(*Inode)
	rDirIno.verSeq = verSeq
	rspDelIno := mp.fsmUnlinkInode(rDirIno, mp.multiVersionList.VerList)
	t.Logf("rspDelinfo ret %v content %v", rspDelIno.Status, rspDelIno)
	assert.True(t, rspDelIno.Status == proto.OpOk)

	rDirDentry := dirDentry.Copy().(*Dentry)
	rDirDentry.VerSeq = verSeq
	rspDelDen := mp.fsmDeleteDentry(rDirDentry, false)
	assert.True(t, rspDelDen.Status == proto.OpOk)

	for idx, info := range rspReadDir.Children {
		t.Logf("DelVersion: delSeq %v  to del idx %v infof %v", verSeq, idx, info)
		rino := &Inode{
			Inode:  info.Inode,
			Type:   ModFileType,
			verSeq: verSeq,
		}
		PrintInodeInfo(t, rino)
		log.LogDebugf("DelVersion get rino %v start", rino)
		t.Logf("DelVersion get rino %v start", rino)
		ino := mp.getInode(rino)
		log.LogDebugf("DelVersion get rino %v end", ino)
		t.Logf("DelVersion get rino %v end", rino)
		assert.True(t, ino.Status == proto.OpOk)
		if ino.Status != proto.OpOk {
			panic(nil)
		}
		rino.verSeq = verSeq
		rspDelIno = mp.fsmUnlinkInode(rino, mp.multiVersionList.VerList)

		assert.True(t, rspDelIno.Status == proto.OpOk || rspDelIno.Status == proto.OpNotExistErr)
		if rspDelIno.Status != proto.OpOk && rspDelIno.Status != proto.OpNotExistErr {
			t.Logf("DelVersion: rspDelIno %v return st %v", rspDelIno, proto.ParseErrorCode(int32(rspDelIno.Status)))
			panic(nil)
		}
		dentry := &Dentry{
			ParentId: rDirIno.Inode,
			Name:     info.Name,
			Type:     ModFileType,
			VerSeq:   verSeq,
			Inode:    rino.Inode,
		}
		log.LogDebugf("test.DelVersion: dentry param %v ", dentry)
		//printAllDentry(t)
		iden, st := mp.getDentry(dentry)
		if st != proto.OpOk {
			t.Logf("DelVersion: dentry %v return st %v", dentry, proto.ParseErrorCode(int32(st)))
		}
		log.LogDebugf("test.DelVersion: get dentry %v ", iden)
		assert.True(t, st == proto.OpOk)

		rDen := iden.Copy().(*Dentry)
		rDen.VerSeq = verSeq
		rspDelDen = mp.fsmDeleteDentry(rDen, false)
		assert.True(t, rspDelDen.Status == proto.OpOk)
	}
}

func TestDentry(t *testing.T) {
	initMp(t)

	var denArry []*Dentry
	//err := gohook.HookMethod(mp, "submit", MockSubmitTrue, nil)
	mp.config.Cursor = 1100
	//--------------------build dir and it's child on different version ------------------
	seq0 := createVer()
	dirIno := createInode(t, ModeDirType)
	assert.True(t, dirIno != nil)
	dirDen := createDentry(t, 1, dirIno.Inode, "testDir", ModeDirType)
	assert.True(t, dirDen != nil)

	fIno := createInode(t, ModFileType)
	assert.True(t, fIno != nil)
	fDen := createDentry(t, dirIno.Inode, fIno.Inode, "testfile", ModFileType)
	denArry = append(denArry, fDen)
	time.Sleep(time.Second)

	//--------------------------------------
	seq1 := createVer()
	fIno1 := createInode(t, ModFileType)
	fDen1 := createDentry(t, dirIno.Inode, fIno1.Inode, "testfile2", ModFileType)
	denArry = append(denArry, fDen1)
	time.Sleep(time.Second)

	//--------------------------------------
	seq2 := createVer()
	fIno2 := createInode(t, ModFileType)
	fDen2 := createDentry(t, dirIno.Inode, fIno2.Inode, "testfile3", ModFileType)
	denArry = append(denArry, fDen2)
	time.Sleep(time.Second)

	//--------------------------------------
	seq3 := createVer()
	//--------------------read dir and it's child on different version ------------------

	t.Logf("TestDentry seq %v,%v,uncommit %v,dir:%v, dentry {%v],inode[%v,%v,%v]", seq1, seq2, seq3, dirDen, denArry, fIno, fIno1, fIno2)
	//-----------read curr version --
	rspReadDir := readDirAll(t, 0, 1)
	t.Logf("len child %v, len arry %v", len(rspReadDir.Children), len(denArry))
	assert.True(t, len(rspReadDir.Children) == 1)
	assert.True(t, isDentryEqual(&rspReadDir.Children[0], dirDen))

	rspReadDir = readDirAll(t, 0, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == len(denArry))
	for idx, info := range rspReadDir.Children {
		t.Logf("getinfo:%v, expect:%v", info, denArry[idx])
		assert.True(t, isDentryEqual(&info, denArry[idx]))
	}

	//-----------read 0 version --
	rspReadDir = readDirAll(t, math.MaxUint64, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 0)

	//-----------read layer 1 version --   seq2 is the last layer, seq1 is the second layer
	rspReadDir = readDirAll(t, seq1, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 2)
	for idx, info := range rspReadDir.Children {
		t.Logf("getinfo:%v, expect:%v", info, denArry[idx])
		assert.True(t, isDentryEqual(&info, denArry[idx]))
	}
	//-----------read layer 2 version --
	rspReadDir = readDirAll(t, seq0, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 1)
	assert.True(t, isDentryEqual(&rspReadDir.Children[0], fDen))

	printAllDentry(t)

	//--------------------del snapshot and read dir and it's child on different version(cann't be work on interfrace) ------------------
	t.Logf("try DelVersion %v", seq0)
	log.LogDebugf("try DelVersion %v", seq0)

	DelVersion(t, seq0, dirIno, dirDen)
	rspReadDir = readDirAll(t, seq0, dirIno.Inode)
	assert.True(t, len(rspReadDir.Children) == 1)

	printAllDentry(t)
	//---------------------------------------------
	t.Logf("try DelVersion 0 top layer")
	log.LogDebugf("try DelVersion 0")
	DelVersion(t, 0, dirIno, dirDen)
	rspReadDir = readDirAll(t, 0, dirIno.Inode)
	t.Logf("after  DelVersion 0 can see file %v %v", len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 0)
	rspReadDir = readDirAll(t, seq1, dirIno.Inode)
	t.Logf("after  DelVersion 0 can see file %v %v", len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 2)

	//---------------------------------------------
	t.Logf("try DelVersion %v", seq1)
	log.LogDebugf("try DelVersion %v", seq1)
	DelVersion(t, seq1, dirIno, dirDen)

	rspReadDir = readDirAll(t, seq1, dirIno.Inode)
	t.Logf("after  DelVersion %v can see file %v %v", seq1, len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 2)
	printAllSysVerList(t)

	//---------------------------------------------
	t.Logf("try DelVersion %v", seq2)
	log.LogDebugf("try DelVersion %v", seq2)
	DelVersion(t, seq2, dirIno, dirDen)

	rspReadDir = readDirAll(t, seq2, dirIno.Inode)
	t.Logf("after  DelVersion %v can see file %v %v", seq1, len(rspReadDir.Children), rspReadDir.Children)
	assert.True(t, len(rspReadDir.Children) == 0)

	t.Logf("printAllSysVerList")
	printAllSysVerList(t)
	t.Logf("PrintAllInodeInfo")
	PrintAllInodeInfo(t)
}

func PrintDirTree(t *testing.T, parentId uint64, path string) {
	rspReadDir := readDirAll(t, 0, parentId)
	for _, child := range rspReadDir.Children {
		pathInner := fmt.Sprintf("%v/%v", path, child.Name)
		if proto.IsDir(child.Type) {
			PrintDirTree(t, child.Inode, pathInner)
			t.Logf("dir:%v", pathInner)
		} else {
			t.Logf("file:%v", pathInner)
		}
	}
}

// create
func TestComplicateSnapshotDeletion(t *testing.T) {
	initMp(t)

	//err := gohook.HookMethod(mp, "submit", MockSubmitTrue, nil)
	mp.config.Cursor = 1100
	//--------------------build dir and it's child on different version ------------------

	dirLayCnt := 6
	var dirName string
	var dirInoId uint64 = 1
	var renameDen *Dentry
	var renameDstIno uint64
	for layIdx := 0; layIdx < dirLayCnt; layIdx++ {
		t.Logf("build tree:layer %v,last dir name %v inodeid %v", layIdx, dirName, dirInoId)
		dirIno := createInode(t, ModeDirType)
		assert.True(t, dirIno != nil)
		dirName = fmt.Sprintf("dir_layer_%v_1", layIdx)
		dirDen := createDentry(t, dirInoId, dirIno.Inode, dirName, ModeDirType)
		assert.True(t, dirDen != nil)
		if dirDen == nil {
			panic(nil)
		}
		dirIno1 := createInode(t, ModeDirType)
		assert.True(t, dirIno1 != nil)
		dirName1 := fmt.Sprintf("dir_layer_%v_2", layIdx)
		dirDen1 := createDentry(t, dirInoId, dirIno1.Inode, dirName1, ModeDirType)
		assert.True(t, dirDen1 != nil)

		if layIdx == 2 {
			renameDen = dirDen.Copy().(*Dentry)
		}
		if layIdx == 1 {
			renameDstIno = dirIno1.Inode
		}
		for fileIdx := 0; fileIdx < layIdx*2; fileIdx++ {
			fileIno := createInode(t, ModFileType)
			assert.True(t, dirIno != nil)

			fileName := fmt.Sprintf("layer_%v_file_%v", layIdx, fileIdx)
			dirDen = createDentry(t, dirIno.Inode, fileIno.Inode, fileName, ModFileType)
			assert.True(t, dirDen != nil)
		}
		dirInoId = dirIno.Inode
		createVer()
	}

	PrintDirTree(t, 1, "root")
	t.Logf("----------------------------------------------")

	t.Logf("try to move dir %v", renameDen)
	renameDen.VerSeq = 0
	assert.True(t, nil != mp.fsmDeleteDentry(renameDen, false))
	renameDen.Name = fmt.Sprintf("rename_from_%v", renameDen.Name)
	renameDen.ParentId = renameDstIno

	t.Logf("try to move to dir %v", renameDen)
	assert.True(t, mp.fsmCreateDentry(renameDen, false) == proto.OpOk)
	PrintDirTree(t, 1, "root")

	//assert.True(t, false)
}
