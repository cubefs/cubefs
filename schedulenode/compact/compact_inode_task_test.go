package compact

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
	"os"
	"strings"
	"syscall"
	"testing"
)

const (
	clusterName   = "chubaofs01"
	ltptestVolume = "ltptest"
	ltptestMaster = "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
	size10M       = 10 * 1024 * 1024
)

var cmpInodeTask *CmpInodeTask

var vol = &CompactVolumeInfo{
	Name:       ltptestVolume,
	limitSizes: []uint32{8},
	limitCnts:  []uint16{10},
}

var inode = &proto.CmpInodeInfo{
	Inode: &proto.InodeInfo{},
}

var cmpMpTask = &CmpMpTask{
	id:  1,
	vol: vol,
}
var clusterInfo = &ClusterInfo{
	name: clusterName,
	mc:   master.NewMasterClient(strings.Split(ltptestMaster, ","), false),
}

type Result struct {
	startIndex     int
	endIndex       int
	lastCmpEkIndex int
}

// 正常ek链 ek.size < limitsize
func TestCalCompactEksArea1(t *testing.T) {
	cmpInodeTask = &CmpInodeTask{vol: vol, Inode: inode.Inode, extents: nil}
	inode.Extents = make([]proto.ExtentKey, 0)
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 0, Size: 1})  // 0
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 1, Size: 2})  // 1
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 3, Size: 3})  // 2
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 6, Size: 4})  // 3
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 10, Size: 5}) // 4
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 15, Size: 6}) // 5
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 21, Size: 7}) // 6
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 28, Size: 1}) // 7
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 29, Size: 2}) // 8
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 31, Size: 2}) // 9
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 33, Size: 3}) // 10
	cmpInodeTask.extents = inode.Extents
	cmpInodeTask.InitTask()
	results := []Result{
		{0, 3, 4},
		{4, 5, 6},
		{6, 7, 8},
		{8, 9, 10},
		{0, 0, 10},
		{0, 0, 10},
	}
	for i, result := range results {
		cmpInodeTask.calCompactEksArea(vol.limitSizes[0], vol.limitCnts[0])
		if cmpInodeTask.startIndex != result.startIndex {
			t.Fatalf("startIndex mismatch, index:%v, expect:%v, actual:%v", i, result.startIndex, cmpInodeTask.startIndex)
		}
		if cmpInodeTask.endIndex != result.endIndex {
			t.Fatalf("endIndex mismatch, index:%v, expect:%v, actual:%v", i, result.endIndex, cmpInodeTask.endIndex)
		}
		if cmpInodeTask.lastCmpEkIndex != result.lastCmpEkIndex {
			t.Fatalf("lastCmpEkIndex mismatch, index:%v, expect:%v, actual:%v", i, result.lastCmpEkIndex, cmpInodeTask.lastCmpEkIndex)
		}
	}
}

// 正常ek链 ek.size >= limitsize
func TestCalCompactEksArea11(t *testing.T) {
	cmpInodeTask = &CmpInodeTask{vol: vol, Inode: inode.Inode, extents: nil}
	inode.Extents = make([]proto.ExtentKey, 0)
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 0, Size: 1})   // 0
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 1, Size: 2})   // 1
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 3, Size: 3})   // 2
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 6, Size: 9})   // 3 case: size>8
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 15, Size: 5})  // 4
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 20, Size: 6})  // 5
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 26, Size: 7})  // 6
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 33, Size: 10}) // 7 case: size>8
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 43, Size: 2})  // 8
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 45, Size: 2})  // 9
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 47, Size: 8})  // 10 case: size=8
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 55, Size: 3})  // 11
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 58, Size: 3})  // 12
	cmpInodeTask.extents = inode.Extents
	cmpInodeTask.InitTask()
	results := []Result{
		{0, 2, 3},
		{4, 5, 6},
		{8, 9, 10},
		{0, 0, 12},
		{0, 0, 12},
	}
	for i, result := range results {
		cmpInodeTask.calCompactEksArea(vol.limitSizes[0], vol.limitCnts[0])
		if cmpInodeTask.startIndex != result.startIndex {
			t.Fatalf("startIndex mismatch, index:%v, expect:%v, actual:%v", i, result.startIndex, cmpInodeTask.startIndex)
		}
		if cmpInodeTask.endIndex != result.endIndex {
			t.Fatalf("endIndex mismatch, index:%v, expect:%v, actual:%v", i, result.endIndex, cmpInodeTask.endIndex)
		}
		if cmpInodeTask.lastCmpEkIndex != result.lastCmpEkIndex {
			t.Fatalf("lastCmpEkIndex mismatch, index:%v, expect:%v, actual:%v", i, result.lastCmpEkIndex, cmpInodeTask.lastCmpEkIndex)
		}
	}
}

// overlap
func TestCalCompactEksArea2(t *testing.T) {
	cmpInodeTask = &CmpInodeTask{vol: vol, Inode: inode.Inode, extents: nil}
	inode.Extents = make([]proto.ExtentKey, 0)
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 0, Size: 1})  // 0
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 1, Size: 2})  // 1
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 3, Size: 3})  // 2
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 5, Size: 4})  // 3 overlap
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 10, Size: 5}) // 4
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 15, Size: 6}) // 5
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 20, Size: 7}) // 6 overlap
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 20, Size: 1}) // 7 overlap
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 29, Size: 2}) // 8
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 31, Size: 2}) // 9
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 33, Size: 3}) // 10
	cmpInodeTask.extents = inode.Extents
	cmpInodeTask.InitTask()
	results := []Result{
		{0, 1, 3},
		{8, 9, 10},
		{0, 0, 10},
		{0, 0, 10},
	}
	for i, result := range results {
		cmpInodeTask.calCompactEksArea(vol.limitSizes[0], vol.limitCnts[0])
		if cmpInodeTask.startIndex != result.startIndex {
			t.Fatalf("startIndex mismatch, index:%v, expect:%v, actual:%v", i, result.startIndex, cmpInodeTask.startIndex)
		}
		if cmpInodeTask.endIndex != result.endIndex {
			t.Fatalf("endIndex mismatch, index:%v, expect:%v, actual:%v", i, result.endIndex, cmpInodeTask.endIndex)
		}
		if cmpInodeTask.lastCmpEkIndex != result.lastCmpEkIndex {
			t.Fatalf("lastCmpEkIndex mismatch, index:%v, expect:%v, actual:%v", i, result.lastCmpEkIndex, cmpInodeTask.lastCmpEkIndex)
		}
	}
}

// discontinuous
func TestCalCompactEksArea3(t *testing.T) {
	cmpInodeTask = &CmpInodeTask{vol: vol, Inode: inode.Inode, extents: nil}
	inode.Extents = make([]proto.ExtentKey, 0)
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 0, Size: 1}) // 0
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 1, Size: 2}) // 1
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 3, Size: 3}) // 2
	//inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 6, Size: 4})
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 10, Size: 5}) // 3
	//inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 15, Size: 6})
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 21, Size: 7}) // 4
	//inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 28, Size: 1})
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 29, Size: 2}) // 5
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 31, Size: 2}) // 6
	inode.Extents = append(inode.Extents, proto.ExtentKey{FileOffset: 33, Size: 3}) // 7
	cmpInodeTask.extents = inode.Extents
	cmpInodeTask.InitTask()
	results := []Result{
		{0, 1, 3},
		{5, 6, 7},
		{0, 0, 7},
		{0, 0, 7},
	}
	for i, result := range results {
		cmpInodeTask.calCompactEksArea(vol.limitSizes[0], vol.limitCnts[0])
		if cmpInodeTask.startIndex != result.startIndex {
			t.Fatalf("startIndex mismatch, index:%v, expect:%v, actual:%v", i, result.startIndex, cmpInodeTask.startIndex)
		}
		if cmpInodeTask.endIndex != result.endIndex {
			t.Fatalf("endIndex mismatch, index:%v, expect:%v, actual:%v", i, result.endIndex, cmpInodeTask.endIndex)
		}
		if cmpInodeTask.lastCmpEkIndex != result.lastCmpEkIndex {
			t.Fatalf("lastCmpEkIndex mismatch, index:%v, expect:%v, actual:%v", i, result.lastCmpEkIndex, cmpInodeTask.lastCmpEkIndex)
		}
	}
}

func TestCalcCmpExtents(t *testing.T) {
	inodeInfo := &proto.InodeInfo{
		Inode: 100,
	}
	cmpInode := &proto.CmpInodeInfo{
		Inode: inodeInfo,
	}
	offsetSizes := [][2]int{
		{0, 10},            // 0
		{10, 1048566},      // 1
		{1048576, 10},      // 2
		{1048586, 1048566}, // 3
		{2097152, 10},      // 4
		{2097162, 1048566}, // 5
		{3145728, 10},      // 6
		{3145738, 1048566}, // 7
		{4194304, 10},      // 8
		{4194314, 1048566}, // 9
		{5242880, 10},      // 10
		{5242890, 1048566}, // 11
		{6291456, 10},      // 12
		{6291466, 1048566}, // 13
		{7340032, 10},      // 14
		{7340042, 1048566}, // 15
		{8388608, 10},      // 16
		{8388618, 1048566}, // 17
		{9437184, 10},      // 18
		{9437194, 1048566}, // 19
	}
	cmpInode.Extents = make([]proto.ExtentKey, len(offsetSizes))
	for i, offsetSize := range offsetSizes {
		cmpInode.Extents[i] = proto.ExtentKey{FileOffset: uint64(offsetSize[0]), Size: uint32(offsetSize[1])}
	}
	// mpId uint64, inode *proto.CmpInodeInfo, vol *CompactVolumeInfo
	cmpInodeTask = NewCmpInodeTask(cmpMpTask, cmpInode, vol)
	cmpInodeTask.InitTask()
	results := [][3]int{
		{0, 18, 3},
		{0, 0, 6},
		{0, 0, 6},
	}
	index := 0
	for {
		_ = cmpInodeTask.CalcCmpExtents()
		if !(results[index][0] == cmpInodeTask.startIndex &&
			results[index][1] == cmpInodeTask.endIndex &&
			results[index][2] == int(cmpInodeTask.State)) {
			t.Fatalf("TestCalcCmpExtents result mismatch, startIndex:endIndex:State expect:%v,%v,%v, actual:%v,%v,%v",
				results[index][0], results[index][1], results[index][2], cmpInodeTask.startIndex, cmpInodeTask.endIndex, cmpInodeTask.State)
		}
		index++
		if cmpInodeTask.State == proto.InodeCmpStopped {
			break
		}
	}
}

func TestInitTask(t *testing.T) {
	inodeInfo := &proto.InodeInfo{
		Inode: 100,
	}
	cmpInode := &proto.CmpInodeInfo{
		Inode:   inodeInfo,
		Extents: []proto.ExtentKey{},
	}
	subTask := NewCmpInodeTask(cmpMpTask, cmpInode, vol)
	subTask.InitTask()
	if subTask.statisticsInfo.CmpEkCnt != 0 {
		t.Fatalf("inode task Initial CmpEkCnt expect:%v, actual:%v", 0, subTask.statisticsInfo.CmpEkCnt)
	}
	if subTask.statisticsInfo.CmpCnt != 0 {
		t.Fatalf("inode task Initial CmpCnt expect:%v, actual:%v", 0, subTask.statisticsInfo.CmpCnt)
	}
	if subTask.State != proto.InodeCmpOpenFile {
		t.Fatalf("inode task Initial State expect:%v, actual:%v", proto.InodeCmpOpenFile, subTask.State)
	}
}

func TestOpenFile(t *testing.T) {
	testFilePath := "/cfs/mnt/TestOpenFile"
	file, _ := os.Create(testFilePath)
	defer func() {
		file.Close()
		os.Remove(testFilePath)
		log.LogFlush()
	}()
	mw, ec, _ := creatHelper(t)
	ctx := context.Background()
	defer func() {
		if err := ec.Close(ctx); err != nil {
			t.Errorf("close ExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
		}
		if err := mw.Close(); err != nil {
			t.Errorf("close MetaWrapper failed: err(%v), vol(%v)", err, ltptestVolume)
		}
	}()
	stat := getFileStat(t, testFilePath)
	_ = ec.OpenStream(stat.Ino, false, false)

	bytes := make([]byte, 512)
	_, _, err := ec.Write(ctx, stat.Ino, 0, bytes, false, false)
	if err != nil {
		t.Fatalf("ec.write failed: err(%v)", err)
	}
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	cmpInode := &proto.CmpInodeInfo{
		Inode:   inodeInfo,
		Extents: []proto.ExtentKey{},
	}
	cmpMpTask.vol.dataClient = ec
	subTask := NewCmpInodeTask(cmpMpTask, cmpInode, vol)
	err = subTask.OpenFile()
	if err != nil {
		t.Fatalf("inode task OpenFile failed: err(%v) inodeId(%v)", err, stat.Ino)
	}
	if subTask.State != proto.InodeCmpCalcCmpEKS {
		t.Fatalf("inode task OpenFile State expect:%v, actual:%v", proto.InodeCmpCalcCmpEKS, subTask.State)
	}

	var notExistInoId uint64 = 200
	inodeInfo = &proto.InodeInfo{
		Inode: notExistInoId,
	}
	cmpInode = &proto.CmpInodeInfo{
		Inode:   inodeInfo,
		Extents: []proto.ExtentKey{},
	}
	subTask = NewCmpInodeTask(cmpMpTask, cmpInode, vol)
	err = subTask.OpenFile()
	if err == nil {
		t.Fatalf("inode task OpenFile should hava error, but it does not hava, inodeId(%v)", notExistInoId)
	}
}

func TestReadAndWriteEkData(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestReadEkData"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	cmpMpTask.vol.dataClient = ec
	cmpMpTask.vol.metaClient = mw
	defer func() {
		cmpMpTask.vol.ReleaseResource()
	}()
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, cmpMpTask.vol.dataClient)
	_, _, extents, _ := cmpMpTask.vol.metaClient.GetExtents(ctx, stat.Ino)
	cmpInode := &proto.CmpInodeInfo{
		Inode:   inodeInfo,
		Extents: extents,
	}
	subTask := NewCmpInodeTask(cmpMpTask, cmpInode, vol)
	_ = subTask.OpenFile()
	for i := 0; i < len(extents); i++ {
		subTask.startIndex = i
		subTask.endIndex = len(extents) - 1
		err := subTask.ReadAndWriteEkData()
		if err != nil {
			cmpEksCnt := subTask.endIndex - subTask.startIndex + 1
			if len(subTask.newEks) >= cmpEksCnt {
				t.Logf("%v", err)
				continue
			}
			t.Fatalf("inode task ReadEkData failed: err(%v)", err)
		}
		if subTask.State != proto.InodeCmpMetaMerge {
			t.Fatalf("inode task Initial State expect:%v, actual:%v", proto.InodeCmpMetaMerge, subTask.State)
		}
	}
}

func TestReadAndWriteEkData2(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestWriteMergeExtentData"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	cmpMpTask.vol.dataClient = ec
	cmpMpTask.vol.metaClient = mw
	defer func() {
		cmpMpTask.vol.ReleaseResource()
	}()
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, cmpMpTask.vol.dataClient)
	_, _, extents, _ := cmpMpTask.vol.metaClient.GetExtents(ctx, stat.Ino)
	cmpInode := &proto.CmpInodeInfo{
		Inode:   inodeInfo,
		Extents: extents,
	}
	subTask := NewCmpInodeTask(cmpMpTask, cmpInode, vol)
	subTask.startIndex = 0
	subTask.endIndex = len(extents) - 1
	_ = subTask.OpenFile()
	err := subTask.ReadAndWriteEkData()
	if err != nil {
		cmpEksCnt := subTask.endIndex - subTask.startIndex + 1
		if len(subTask.newEks) >= cmpEksCnt {
			t.Logf("%v", err)
			return
		}
	}
	startOffset := subTask.extents[subTask.startIndex].FileOffset
	size := subTask.extents[subTask.endIndex].FileOffset + uint64(subTask.extents[subTask.endIndex].Size) - startOffset
	if subTask.newEks[0].FileOffset != extents[subTask.startIndex].FileOffset {
		t.Fatalf("inode task WriteMergeExtentData FileOffset expect:%v, actual:%v", extents[subTask.startIndex].FileOffset, subTask.newEks[0].FileOffset)
	}
	var cmpSize uint32
	for _, newEk := range subTask.newEks {
		cmpSize += newEk.Size
	}
	if uint64(cmpSize) != size {
		t.Fatalf("inode task WriteMergeExtentData Size expect:%v, actual:%v", size, cmpSize)
	}
	if err != nil {
		t.Fatalf("inode task WriteMergeExtentData failed: err(%v)", err)
	}
	if subTask.State != proto.InodeCmpMetaMerge {
		t.Fatalf("inode task Initial State expect:%v, actual:%v", proto.InodeCmpMetaMerge, subTask.State)
	}
}

func TestMetaMergeExtents(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestWriteMergeExtentData"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	cmpMpTask.vol.dataClient = ec
	cmpMpTask.vol.metaClient = mw
	defer func() {
		cmpMpTask.vol.ReleaseResource()
	}()
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, cmpMpTask.vol.dataClient)
	gen, _, extents, _ := cmpMpTask.vol.metaClient.GetExtents(ctx, stat.Ino)
	fmt.Printf("before merge gen:%v\n", gen)
	cmpInode := &proto.CmpInodeInfo{
		Inode:   inodeInfo,
		Extents: extents,
	}
	subTask := NewCmpInodeTask(cmpMpTask, cmpInode, vol)
	subTask.startIndex = 0
	subTask.endIndex = len(extents) - 2
	_ = subTask.OpenFile()
	_ = subTask.ReadAndWriteEkData()
	afterCompactEkLen := len(subTask.newEks) + 1
	err := subTask.MetaMergeExtents()
	if err != nil {
		t.Fatalf("inode task MetaMergeExtents failed: err(%v)", err)
	}
	gen, _, extents, _ = cmpMpTask.vol.metaClient.GetExtents(ctx, stat.Ino)
	fmt.Printf("after merge gen:%v\n", gen)
	if len(extents) != afterCompactEkLen {
		t.Fatalf("inode task MetaMergeExtents failed: extents length, expect:%v, actual:%v", afterCompactEkLen, len(extents))
	}
	if subTask.State != proto.InodeCmpCalcCmpEKS {
		t.Fatalf("inode task Initial State expect:%v, actual:%v", proto.InodeCmpCalcCmpEKS, subTask.State)
	}
}

func TestMetaMergeExtentsError(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestWriteMergeExtentData"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	cmpMpTask.vol.dataClient = ec
	cmpMpTask.vol.metaClient = mw
	defer func() {
		cmpMpTask.vol.ReleaseResource()
	}()
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, cmpMpTask.vol.dataClient)
	_, _, extents, _ := cmpMpTask.vol.metaClient.GetExtents(ctx, stat.Ino)
	cmpInode := &proto.CmpInodeInfo{
		Inode:   inodeInfo,
		Extents: extents,
	}
	subTask := NewCmpInodeTask(cmpMpTask, cmpInode, vol)
	subTask.startIndex = 0
	subTask.endIndex = len(extents) - 2
	_ = subTask.OpenFile()
	_ = subTask.ReadAndWriteEkData()
	// modify file
	_, _, _ = ec.Write(ctx, stat.Ino, 0, []byte{1, 2, 3, 4, 5}, false, false)
	if err := ec.Flush(ctx, stat.Ino); err != nil {
		t.Fatalf("Flush failed, err(%v)", err)
	}
	err := subTask.MetaMergeExtents()
	if err == nil {
		t.Fatalf("inode task MetaMergeExtents should hava error, but it does not hava, inodeId(%v)", stat.Ino)
	}
}

func TestWriteFileByRow(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestWriteFileByRow"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	cmpMpTask.vol.dataClient = ec
	cmpMpTask.vol.metaClient = mw
	defer func() {
		cmpMpTask.vol.ReleaseResource()
	}()
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, cmpMpTask.vol.dataClient)
}

// Write data using mount directory
func writeRowFileByMountDir() (file *os.File, filePath string) {
	filePath = "/cfs/mnt/writeRowFile"
	file, _ = os.Create(filePath)
	bs := 10
	size := bs * 1024 * 1024 // M
	bufStr := strings.Repeat("A", size)
	bytes := []byte(bufStr)
	_, _ = file.WriteAt(bytes, 0)
	mStr := strings.Repeat("b", 10)
	mBytes := []byte(mStr)
	for i := 0; i < size; i++ {
		remain := i % (1024 * 1024)
		if remain == 0 {
			_, _ = file.WriteAt(mBytes, int64(i))
		}
	}
	_ = file.Sync()
	return
}

func writeRowFileBySdk(t *testing.T, ctx context.Context, inoId uint64, size int, ec *data.ExtentClient) {
	bufStr := strings.Repeat("A", size)
	bytes := []byte(bufStr)
	if err := ec.OpenStream(inoId, false, false); err != nil {
		t.Fatalf("writeRowFileBySdk OpenStream failed: inodeId(%v), err(%v)", inoId, err)
	}
	_, _, err := ec.Write(ctx, inoId, 0, bytes, false, false)
	if err != nil {
		t.Fatalf("writeRowFileBySdk SyncWrite failed: inodeId(%v), err(%v)", inoId, err)
	}
	mStr := strings.Repeat("b", 10)
	mBytes := []byte(mStr)
	for i := 0; i < size; i++ {
		remain := i % (1024 * 1024)
		if remain == 0 {
			_, _, _ = ec.Write(ctx, inoId, uint64(i), mBytes, false, false)
		}
	}
	if err = ec.Flush(ctx, inoId); err != nil {
		t.Fatalf("Flush failed, err(%v)", err)
	}
}

func creatHelper(t *testing.T) (mw *meta.MetaWrapper, ec *data.ExtentClient, err error) {
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        ltptestVolume,
		Masters:       strings.Split(ltptestMaster, ","),
		ValidateOwner: true,
		Owner:         ltptestVolume,
	}); err != nil {
		t.Fatalf("NewMetaWrapper failed: err(%v) vol(%v)", err, ltptestVolume)
	}
	if ec, err = data.NewExtentClient(&data.ExtentConfig{
		Volume:            ltptestVolume,
		Masters:           strings.Split(ltptestMaster, ","),
		FollowerRead:      false,
		OnInsertExtentKey: mw.InsertExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		TinySize:          -1,
	}, nil); err != nil {
		t.Fatalf("NewExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
	}
	return mw, ec, nil
}

func setVolForceRow(forceRow bool) {
	mc := getMasterClient()
	vv, _ := mc.AdminAPI().GetVolumeSimpleInfo(ltptestVolume)
	_ = mc.AdminAPI().UpdateVolume(vv.Name, vv.Capacity, int(vv.DpReplicaNum), int(vv.MpReplicaNum), int(vv.TrashRemainingDays),
		int(vv.DefaultStoreMode), vv.FollowerRead, vv.VolWriteMutexEnable, vv.NearRead, vv.Authenticate, vv.EnableToken, vv.AutoRepair,
		forceRow, vv.IsSmart, vv.EnableWriteCache, calcAuthKey(vv.Owner), vv.ZoneName, fmt.Sprintf("%v,%v", vv.MpLayout.PercentOfMP, vv.MpLayout.PercentOfReplica), strings.Join(vv.SmartRules, ","),
		uint8(vv.OSSBucketPolicy), uint8(vv.CrossRegionHAType), vv.ExtentCacheExpireSec, vv.CompactTag, vv.DpFolReadDelayConfig.DelaySummaryInterval, vv.FolReadHostWeight, 0, 0, 0)
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}

func getFileStat(t *testing.T, file string) *syscall.Stat_t {
	info, err := os.Stat(file)
	if err != nil {
		t.Fatalf("Get Stat failed: err(%v) file(%v)", err, file)
	}
	return info.Sys().(*syscall.Stat_t)
}

func getMasterClient() *master.MasterClient {
	masterClient := master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	return masterClient
}
