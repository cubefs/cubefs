package data

import (
	"context"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/master"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	InnerDataType = iota
	TinyType
	NormalType
	UnknownType
)

const (
	defaultEcDataNum   = 4
	defaultEcParityNum = 2
)
var (
	testMc  = master.NewMasterClient([]string{"192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010"}, false)
)

type Rule struct {
	offset 		uint64
	size		int
	writeCount	int
	isFlush		bool
	isOverWrite	bool
	isRead		bool
}

type ExtentRule struct {
	FileOffset	uint64
	Size		uint32
	ExtentType	int
}

func (rule *ExtentRule) String() string {
	if rule == nil {
		return ""
	}
	return fmt.Sprintf("FileOffset(%v) Size(%v) Type(%v)", rule.FileOffset, rule.Size, rule.ExtentType)
}

func TestStreamWrite_ChangeExtentHandler_InnerData(t *testing.T)  {
	logDir := "/tmp/logs/cfs"
	os.RemoveAll(logDir)
	log.InitLog(logDir, "test", log.DebugLevel, nil)
	// create vol for RocksDB
	err := testMc.AdminAPI().CreateVolume("rocksdbtest", "test", 3, 120, 10, 3, 3, 10,
		2, 0, false, false, false, false, false, false, true, "default",
		"0,0", "", 0, "default", defaultEcDataNum, defaultEcParityNum, false)
	if err != nil {
		t.Fatalf("TestStreamWrite_ChangeExtentHandler_InnerData: create rocksdb vol err(%v)", err)
	}
	fmt.Println("TestStreamWrite_ChangeExtentHandler_InnerData: done create volume")
	time.Sleep(5 * time.Second)

	// create inode
	mw, ec, err := creatHelper(t, "rocksdbtest", "test")
	if err != nil {
		t.Fatalf("TestStreamWrite_ChangeExtentHandler_InnerData: create wrapper err(%v)", err)
	}
	ec.tinySize = util.DefaultTinySizeLimit

	// create local directory
	localTestDir := "/tmp/innerdata_test"
	os.MkdirAll(localTestDir, 0755)

	defer func() {
		//testMc.AdminAPI().DeleteVolume("rocksdbtest", calcAuthKey("test"))
		//os.RemoveAll(localTestDir)
		log.LogFlush()
		//os.RemoveAll(logDir)
	}()

	fmt.Println("TestStreamWrite_ChangeExtentHandler_InnerData: start test")

	tests := []struct{
		name					string
		innerSize				uint64
		writeRule				[]*Rule
		extentTypeList			[]*ExtentRule
		recoverExtentTypeList	[]*ExtentRule
	} {
		{ // inner
			name: 			"test01",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 4*1024, writeCount: 4, isFlush: false},
				{offset: 6*1024, size: 2, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: []*ExtentRule{{FileOffset: 0, Size: 16*1024, ExtentType: InnerDataType}},
		},
		{ // inner -> tiny
			name: 			"test02",
			innerSize: 		5*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 4*1024, writeCount: 3, isFlush: false},
				{offset: 4*1024, size: 2*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: 		[]*ExtentRule{{FileOffset: 0, Size: 12*1024, ExtentType: TinyType}},
			recoverExtentTypeList: 	[]*ExtentRule{{FileOffset: 0, Size: 12*1024, ExtentType: NormalType}},
		},
		{ // inner -> tiny
			name: 			"test03",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 1024/8, isFlush: false},
				{offset: 15*1024, size: 8*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: 		[]*ExtentRule{{FileOffset: 0, Size: 1024*1024, ExtentType: TinyType}},
			recoverExtentTypeList: 	[]*ExtentRule{{FileOffset: 0, Size: 1024*1024, ExtentType: NormalType}},
		},
		{ // inner -> tiny -> normal
			name: 			"test04",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 1024/8+1, isFlush: false},
				{offset: 15*1024, size: 8*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: []*ExtentRule{{FileOffset: 0, Size: 1024*(1024+8), ExtentType: NormalType}},
		},
		{ // tiny -> normal
			name: 			"test05",
			innerSize: 		8*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 16*1024, writeCount: 1024/16 + 1, isFlush: false},
				{offset: 6*1024, size: 8*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: []*ExtentRule{{FileOffset: 0, Size: 1024*(1024+16), ExtentType: NormalType}},
		},
		{ // tiny -> normal
			name: 			"test06",
			innerSize: 		8*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 500*1024, writeCount: 3, isFlush: false},
				{offset: 500*1024, size: 1000*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: []*ExtentRule{{FileOffset: 0, Size: 1024*1500, ExtentType: NormalType}},
		},
		{ // inner, tiny
			name: 			"test07",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 2, isFlush: true},
				{offset: 16*1024, size: 1024*1024, writeCount: 1, isFlush: false},
				{offset: 12*1024, size: 8*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: 		[]*ExtentRule{
				{FileOffset: 0, Size: 16*1024, ExtentType: InnerDataType},
				{FileOffset: 16*1024, Size: 1024*1024, ExtentType: TinyType},
			},
			recoverExtentTypeList: 	[]*ExtentRule{
				{FileOffset: 0, Size: 16*1024, ExtentType: InnerDataType},
				{FileOffset: 16*1024, Size: 1024*1024, ExtentType: NormalType},
			},
		},
		{ // inner, inner -> tiny
			name: 			"test08",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 1, isFlush: true},
				{offset: 8*1024, size: 8*1024, writeCount: 2, isFlush: false},
				{offset: 6*1024, size: 8*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: 		[]*ExtentRule{
				{FileOffset: 0, Size: 8*1024, ExtentType: InnerDataType},
				{FileOffset: 8*1024, Size: 16*1024, ExtentType: TinyType},
			},
			recoverExtentTypeList: 	[]*ExtentRule{
				{FileOffset: 0, Size: 8*1024, ExtentType: InnerDataType},
				{FileOffset: 8*1024, Size: 16*1024, ExtentType: NormalType},
			},
		},
		{ // inner, inner -> tiny -> normal
			name: 			"test09",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 1, isFlush: true},
				{offset: 8*1024, size: 8*1024, writeCount: 1024/8+1, isFlush: false},
				{offset: 5*1024, size: 8*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 0, Size: 8*1024, ExtentType: InnerDataType},
				{FileOffset: 8*1024, Size: (1024+8)*1024, ExtentType: NormalType},
			},
		},
		{ // inner, tiny -> normal
			name: 			"test10",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 2, isFlush: true},
				{offset: 16*1024, size: 16*1024, writeCount: 1024/16+1, isFlush: false},
				{offset: 12*1024, size: 8*1024, writeCount: 1, isFlush: false, isOverWrite: true},  // random-write
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 0, Size: 16*1024, ExtentType: InnerDataType},
				{FileOffset: 16*1024, Size: (1024+16)*1024, ExtentType: NormalType},
			},
		},
		{ // inner, tiny, normal
			name: 			"test11",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 1, isFlush: true},
				{offset: 8*1024, size: 1024*1024, writeCount: 1, isFlush: true},
				{offset: (8+1024)*1024, size: 1024, writeCount: 1, isFlush: false},
				{offset: 6*1024, size: 8*1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
				{offset: 1024*1024, size: 9*1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
			},
			extentTypeList: 		[]*ExtentRule{
				{FileOffset: 0, Size: 8*1024, ExtentType: InnerDataType},
				{FileOffset: 8*1024, Size: 1024*1024, ExtentType: TinyType},
				{FileOffset: (8+1024)*1024, Size: 1024, ExtentType: NormalType},
			},
			recoverExtentTypeList: 	[]*ExtentRule{
				{FileOffset: 0, Size: 8*1024, ExtentType: InnerDataType},
				{FileOffset: 8*1024, Size: (1024+1)*1024, ExtentType: NormalType},
			},
		},
		{ // inner -> tiny, normal
			name: 			"test12",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 2, isFlush: false},
				{offset: 16*1024, size: 512*1024, writeCount: 1, isFlush: true},
				{offset: (16+512)*1024, size: 1024, writeCount: 1, isFlush: false},
				{offset: 512*1024, size: 17*1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
			},
			extentTypeList: 		[]*ExtentRule{
				{FileOffset: 0, Size: (16+512)*1024, ExtentType: TinyType},
				{FileOffset: (16+512)*1024, Size: 1024, ExtentType: NormalType},
			},
			recoverExtentTypeList: 	[]*ExtentRule{
				{FileOffset: 0, Size: (16+512+1)*1024, ExtentType: NormalType},
			},
		},
		{ // inner -> tiny -> normal, normal
			name: 			"test13",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 2, isFlush: false},
				{offset: 16*1024, size: 512*1024, writeCount: 2, isFlush: false},
				{offset: (16+1024)*1024, size: 1024, writeCount: 1, isFlush: true},
				{offset: (16+1024+1)*1024, size: 1024, writeCount: 1, isFlush: true},
				{offset: 10*1024, size: 20*1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 0, Size: (16+1024+2)*1024, ExtentType: NormalType},
			},
		},
		{ // tiny
			name: 			"test14",
			innerSize: 		0,
			writeRule: 		[]*Rule{
				{offset: 0, size: 16*1024, writeCount: 2, isFlush: false},
				{offset: 15*1024, size: 10*1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
			},
			extentTypeList: 		[]*ExtentRule{
				{FileOffset: 0, Size: 32*1024, ExtentType: TinyType},
			},
			recoverExtentTypeList: 	[]*ExtentRule{
				{FileOffset: 0, Size: 32*1024, ExtentType: NormalType},
			},
		},
		{ // tiny, normal
			name: 			"test15",
			innerSize: 		0,
			writeRule: 		[]*Rule{
				{offset: 0, size: 1024*1024, writeCount: 1, isFlush: true},
				{offset: 1024*1024, size: 16*1024, writeCount: 2, isFlush: false},
				{offset: 1020*1024, size: 20*1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
			},
			extentTypeList: 		[]*ExtentRule{
				{FileOffset: 0, Size: 1024*1024, ExtentType: TinyType},
				{FileOffset: 1024*1024, Size: 32*1024, ExtentType: NormalType},
			},
			recoverExtentTypeList: 	[]*ExtentRule{
				{FileOffset: 0, Size: (1024+32)*1024, ExtentType: NormalType},
			},
		},
		{ // tiny -> normal, normal
			name: 			"test16",
			innerSize: 		0,
			writeRule: 		[]*Rule{
				{offset: 0, size: 512*1024, writeCount: 2, isFlush: false},
				{offset: 1024*1024, size: 16*1024, writeCount: 2, isFlush: true},
				{offset: (1024+32)*1024, size: 16*1024, writeCount: 1, isFlush: false},
				{offset: 1024*1024, size: 20*1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 0, Size: (1024+48)*1024, ExtentType: NormalType},
			},
		},
		{ // normal
			name: 			"test17",
			innerSize: 		0,
			writeRule: 		[]*Rule{
				{offset: 0, size: 1025*1024, writeCount: 1, isFlush: false},
				{offset: 1024*1024, size: 1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 0, Size: 1025*1024, ExtentType: NormalType},
			},
		},
		{ // inner, normal
			name: 			"test18",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 2, isFlush: true},
				{offset: 17*1024, size: 16*1024, writeCount: 1, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 0, Size: 16*1024, ExtentType: InnerDataType},
				{FileOffset: 17*1024, Size: 16*1024, ExtentType: NormalType},
			},
		},
		{ // inner, normal
			name: 			"test19",
			innerSize: 		16*1024,
			writeRule: 		[]*Rule{
				{offset: 0, size: 8*1024, writeCount: 1, isFlush: true},
				{offset: 8*1024, size: 1025*1024, writeCount: 1, isFlush: false},
				{offset: 7*1024, size: 16*1024, writeCount: 1, isFlush: false, isOverWrite: true},  	// random-write
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 0, Size: 8*1024, ExtentType: InnerDataType},
				{FileOffset: 8*1024, Size: 1025*1024, ExtentType: NormalType},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inodeInfo, err := mw.Create_ll(context.Background(), 1, "TestStreamer_InnerData_" + tt.name, 0644, 0, 0, nil)
			if err != nil {
				t.Fatalf("TestStreamWrite_ChangeExtentHandler_InnerData: creat inode failed, err(%v)", err)
			}
			streamer := NewStreamer(ec, inodeInfo.Inode, ec.streamerConcurrentMap.GetMapSegment(inodeInfo.Inode), false, false)
			streamer.refcnt++
			fmt.Println("TestStreamWrite_ChangeExtentHandler_InnerData: done create inode")
			// set inner size
			streamer.innerSize = tt.innerSize
			// create local file
			localFile, err := os.Create(path.Join(localTestDir, tt.name))
			defer func() {
				localFile.Close()
				streamer.done <- struct{}{}
			}()
			if err != nil {
				t.Fatalf("TestInsertInnerData create local file(%v) err: %v", tt.name, err)
			}
			totalSize := 0
			writeIndex := 0
			// write
			for _, rule := range tt.writeRule {
				size := rule.size
				writeData := make([]byte, size)
				for i := 0; i < rule.writeCount; i++ {
					writeIndex++
					for j := 0; j < size; j++ {
						writeData[j] = byte(writeIndex)
					}
					offset := rule.offset + uint64(i*size)
					fmt.Printf("test(%v) write offset(%v) size(%v)\n", tt.name, offset, size)
					total, isROW, err := streamer.write(context.Background(), writeData, offset, size, false, false)
					if err != nil || total != size || isROW != false {
						t.Fatalf("TestInsertInnerData write: name(%v) err(%v) total(%v) isROW(%v) expect size(%v)", tt.name, err, total, isROW, size)
					}
					if _, err = localFile.WriteAt(writeData, int64(offset)); err != nil {
						t.Fatalf("TestInsertInnerData failed: write local file err(%v) name(%v)", err, localFile.Name())
					}
				}
				// compute total size
				if !rule.isOverWrite {
					totalSize = int(rule.offset) + rule.size*rule.writeCount
				}
				// flush
				if rule.isFlush {
					if err = streamer.flush(context.Background(), true); err != nil {
						t.Fatalf("TestInsertInnerData cfs flush err(%v) test(%v)", err, tt.name)
					}
					if err = localFile.Sync(); err != nil {
						t.Fatalf("TestInsertInnerData local flush err(%v) file(%v)", err, localFile.Name())
					}
				}
			}
			// flush
			fmt.Println("TestInsertInnerData: start flush file")
			if err = streamer.flush(context.Background(), true); err != nil {
				t.Fatalf("TestInsertInnerData cfs flush err(%v) test(%v)", err, tt.name)
			}
			if err = localFile.Sync(); err != nil {
				t.Fatalf("TestInsertInnerData local flush err(%v) file(%v)", err, localFile.Name())
			}
			// read data
			fmt.Println("TestInsertInnerData: start read file")
			readCFSData := make([]byte, totalSize)
			expectTotal, hasHole, err := streamer.read(context.Background(), readCFSData, 0, totalSize)
			if err != nil || expectTotal != totalSize {
				t.Fatalf("TestInsertInnerData read CFS file err(%v) test(%v) expect size(%v) but size(%v) hasHole(%v)",
					err, tt.name, expectTotal, totalSize, hasHole)
			}
			// check data crc
			readLocalData := make([]byte, totalSize)
			_, err = localFile.ReadAt(readLocalData, 0)
			if err != nil {
				t.Fatalf("TestInsertInnerData read local file err(%v) file(%v)", err, localFile.Name())
			}
			fmt.Println("TestInsertInnerData: start check crc")
			if crc32.ChecksumIEEE(readCFSData[:totalSize]) != crc32.ChecksumIEEE(readLocalData[:totalSize]) {
				t.Fatalf("TestInsertInnerData failed: test(%v) crc is inconsistent", tt.name)
			}
			// check extent
			fmt.Println("TestInsertInnerData: start check extent")
			extents := streamer.extents.List()
			if len(extents) == 0 || ( len(extents) != len(tt.extentTypeList) && len(extents) != len(tt.recoverExtentTypeList) ) {
				t.Fatalf("TestInsertInnerData failed: test(%v) expect extent length(%v) or (%v) but(%v)",
					tt.name, len(tt.extentTypeList), len(tt.recoverExtentTypeList), len(extents))
			}
			unexpectedExtentType := false
			unexpectedRecoverExtentType := false
			if len(extents) == len(tt.extentTypeList) {
				for i, ext := range extents {
					extType := getExtentType(ext)
					expectExtent := tt.extentTypeList[i]
					if extType != expectExtent.ExtentType || ext.FileOffset != expectExtent.FileOffset || ext.Size != expectExtent.Size {
						unexpectedExtentType = true
						break
					}
				}
			}
			if len(extents) == len(tt.recoverExtentTypeList) {
				for i, ext := range extents {
					extType := getExtentType(ext)
					expectExtent := tt.recoverExtentTypeList[i]
					if extType != expectExtent.ExtentType || ext.FileOffset != expectExtent.FileOffset || ext.Size != expectExtent.Size {
						unexpectedRecoverExtentType = true
						break
					}
				}
			}
			if unexpectedExtentType && unexpectedRecoverExtentType {
				t.Fatalf("TestInsertInnerData failed: test(%v) expect extent list(%v) and recover extent list(%v) but(%v)",
					tt.name, tt.extentTypeList, tt.recoverExtentTypeList, extents)
			}
			fmt.Println("TestInsertInnerData: extent list: ", extents)
			return
		})
	}
}

func getExtentType(ext proto.ExtentKey) int {
	if ext.StoreType == proto.InnerData && ext.PartitionId == math.MaxUint64 && ext.ExtentId == math.MaxUint64 {
		return InnerDataType
	}
	if ext.StoreType == proto.NormalData && ext.ExtentId <= storage.TinyExtentCount && ext.ExtentId > 0 {
		return TinyType
	}
	if ext.StoreType == proto.NormalData && ext.ExtentId > storage.TinyExtentCount {
		return NormalType
	}
	return UnknownType
}
