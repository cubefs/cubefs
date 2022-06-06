package data

import (
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
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
}

func (rule *ExtentRule) String() string {
	if rule == nil {
		return ""
	}
	return fmt.Sprintf("FileOffset(%v) Size(%v)", rule.FileOffset, rule.Size)
}

func TestStreamer_WritePendingPacket(t *testing.T) {
	s := &Streamer{handler: &ExtentHandler{inode: 999, storeMode: proto.NormalExtentType}}
	tests := []struct{
		name           			string
		writeOffset				uint64
		writeSize				int
		orgPendingPacketList	[]*Packet
		expectPendingPacketList []*Packet
	} {
		{
			name: "test01",
			writeOffset: 10,
			writeSize: 128,
			orgPendingPacketList: []*Packet{},
			expectPendingPacketList: []*Packet{
				newPacket(10, 128),
			},
		},
		{
			// 插入头
			name: "test02",
			writeOffset: 5,
			writeSize: 10,
			orgPendingPacketList: []*Packet{
				newPacket(100, 50),
				newPacket(200, 100),
				newPacket(300, 400),
			},
			expectPendingPacketList: []*Packet{
				newPacket(5, 10),
				newPacket(100, 50),
				newPacket(200, 100),
				newPacket(300, 400),
			},
		},
		{
			// 插入中间
			name: "test03",
			writeOffset: 160,
			writeSize: 30,
			orgPendingPacketList: []*Packet{
				newPacket(100, 50),
				newPacket(200, 100),
				newPacket(300, 400),
			},
			expectPendingPacketList: []*Packet{
				newPacket(100, 50),
				newPacket(160, 30),
				newPacket(200, 100),
				newPacket(300, 400),
			},
		},
		{
			// 插入尾
			name: "test04",
			writeOffset: 500,
			writeSize: 600,
			orgPendingPacketList: []*Packet{
				newPacket(100, 50),
				newPacket(200, 100),
				newPacket(300, 400),
			},
			expectPendingPacketList: []*Packet{
				newPacket(100, 50),
				newPacket(200, 100),
				newPacket(300, 400),
				newPacket(500, 600),
			},
		},
		{
			// 连接第一个packet
			name: "test05",
			writeOffset: 50,
			writeSize: 50,
			orgPendingPacketList: []*Packet{
				newPacket(100, 50),
				newPacket(200, 100),
				newPacket(300, 400),
			},
			expectPendingPacketList: []*Packet{
				newPacket(50, 50),
				newPacket(100, 50),
				newPacket(200, 100),
				newPacket(300, 400),
			},
		},
		{
			// 写入第一个packet
			name: "test06",
			writeOffset: 150,
			writeSize: 50,
			orgPendingPacketList: []*Packet{
				newPacket(100, 50),
				newPacket(200, 100),
				newPacket(300, 400),
			},
			expectPendingPacketList: []*Packet{
				newPacket(100, 100),
				newPacket(200, 100),
				newPacket(300, 400),
			},
		},
		{
			// 写入第一个packet + 跨packet
			name: "test07",
			writeOffset: 150,
			writeSize: 128*1024,
			orgPendingPacketList: []*Packet{
				newPacket(100, 50),
				newPacket(2*128*1024, 128*1024),
				newPacket(3*128*1024, 100),
			},
			expectPendingPacketList: []*Packet{
				newPacket(100, 128*1024),
				newPacket(100+128*1024, 50),
				newPacket(2*128*1024, 128*1024),
				newPacket(3*128*1024, 100),
			},
		},
		{
			// 写入第二个packet + 跨packet + 与第三个packet相接
			name: "test08",
			writeOffset: (2*128+64)*1024,
			writeSize: (2*128-64)*1024,
			orgPendingPacketList: []*Packet{
				newPacket(0, 128*1024),
				newPacket(2*128*1024, 64*1024),
				newPacket(4*128*1024, 128*1024),
			},
			expectPendingPacketList: []*Packet{
				newPacket(0, 128*1024),
				newPacket(2*128*1024, 128*1024),
				newPacket(3*128*1024, 128*1024),
				newPacket(4*128*1024, 128*1024),
			},
		},
		{
			// 接在第二个packet尾部 + 跨packet
			name: "test09",
			writeOffset: (2*128+64)*1024,
			writeSize: 128*1024,
			orgPendingPacketList: []*Packet{
				newPacket(0, 128*1024),
				newPacket(2*128*1024, 64*1024),
				newPacket(4*128*1024, 128*1024),
			},
			expectPendingPacketList: []*Packet{
				newPacket(0, 128*1024),
				newPacket(2*128*1024, 128*1024),
				newPacket(3*128*1024, 64*1024),
				newPacket(4*128*1024, 128*1024),
			},
		},
		{
			// 接在第三个packet尾部
			name: "test10",
			writeOffset: (4*128+64)*1024,
			writeSize: 64*1024,
			orgPendingPacketList: []*Packet{
				newPacket(0, 128*1024),
				newPacket(2*128*1024, 64*1024),
				newPacket(4*128*1024, 64*1024),
			},
			expectPendingPacketList: []*Packet{
				newPacket(0, 128*1024),
				newPacket(2*128*1024, 64*1024),
				newPacket(4*128*1024, 128*1024),
			},
		},
		{
			// 接在第三个packet之后
			name: "test11",
			writeOffset: 5*128*1024,
			writeSize: 128*1024,
			orgPendingPacketList: []*Packet{
				newPacket(0, 128*1024),
				newPacket(2*128*1024, 64*1024),
				newPacket(4*128*1024, 128*1024),
			},
			expectPendingPacketList: []*Packet{
				newPacket(0, 128*1024),
				newPacket(2*128*1024, 64*1024),
				newPacket(4*128*1024, 128*1024),
				newPacket(5*128*1024, 128*1024),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s.pendingPacketList = tt.orgPendingPacketList
			writeData := make([]byte, tt.writeSize)
			for i := 0; i < tt.writeSize; i++ {
				writeData[i] = 'a'
			}
			ek, err := s.WritePendingPacket(writeData, tt.writeOffset, tt.writeSize, true)
			if err != nil {
				t.Fatalf("TestStreamer_WritePendingPacket: write err(%v) name(%v) offset(%v) size(%v)", err, tt.name, tt.writeOffset, tt.writeSize)
			}
			// check ek
			if ek.FileOffset != uint64(tt.writeOffset) || ek.Size != uint32(tt.writeSize) {
				t.Fatalf("TestStreamer_WritePendingPacket: name(%v) expect offset(%v) but offset(%v) expect size(%v) but size(%v)", 
					tt.name, tt.writeOffset, ek.FileOffset, tt.writeSize, ek.Size)
			}
			// check pending packet list
			if len(tt.expectPendingPacketList) != len(s.pendingPacketList) {
				t.Fatalf("TestStreamer_WritePendingPacket: name(%v) expect list len(%v) but(%v)", tt.name, len(tt.expectPendingPacketList), len(s.pendingPacketList))
			}
			fmt.Println("pending packet list: ", s.pendingPacketList)
			for i := 0; i < len(tt.expectPendingPacketList); i++ {
				expectPacket := tt.expectPendingPacketList[i]
				packet := s.pendingPacketList[i]
				if expectPacket.KernelOffset != packet.KernelOffset || expectPacket.Size != expectPacket.Size {
					t.Fatalf("TestStreamer_WritePendingPacket: name(%v) expect offset(%v) but offset(%v) expect size(%v) but size(%v)",
						tt.name, expectPacket.KernelOffset, packet.KernelOffset, expectPacket.Size, packet.Size)
				}
			}
			return
		})
	}
}

func newPacket(offset uint64, size uint32) (packet *Packet) {
	packet = &Packet{}
	packet.KernelOffset = offset
	packet.Size = size
	packet.Data = make([]byte, 128*1024)
	return packet
}

func TestStreamer_WriteFile_Pending(t *testing.T)  {
	logDir := "/tmp/logs/cfs"
	log.InitLog(logDir, "test", log.DebugLevel, nil)

	// create inode
	mw, ec, err := creatHelper(t)
	if err != nil {
		t.Fatalf("TestExtentHandler_PendingPacket: create wrapper err(%v)", err)
	}
	ec.SetEnableWriteCache(true)
	ec.tinySize = util.DefaultTinySizeLimit
	inodeInfo, err := mw.Create_ll(context.Background(), 1, "TestPendingPacket", 0644, 0, 0, nil)
	if err != nil {
		t.Fatalf("TestExtentHandler_PendingPacket: creat inode failed, err(%v)", err)
	}
	streamer := NewStreamer(ec, inodeInfo.Inode, ec.streamerConcurrentMap.GetMapSegment(inodeInfo.Inode), false, false)
	streamer.refcnt++
	fmt.Println("TestExtentHandler_PendingPacket: done create inode")

	// create local directory
	localTestDir := "/tmp/pending_packet_test"
	os.MkdirAll(localTestDir, 0755)

	defer func() {
		streamer.done <- struct{}{}
		mw.Delete_ll(context.Background(), 1, "TestPendingPacket", false)
		os.RemoveAll(localTestDir)
		log.LogFlush()
	}()

	fmt.Println("TestExtentHandler_PendingPacket: start test")

	tests := []struct{
		name           string
		operationRules []*Rule
		extentTypeList []*ExtentRule
	} {
		{
			name: 			"test01",
			operationRules: 		[]*Rule {
				{offset: 0, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 2*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 3*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
			},
			extentTypeList: []*ExtentRule{{FileOffset: 0, Size: 4*128*1024}},
		},
		{
			name: 			"test02",
			operationRules: 		[]*Rule {
				{offset: 128*1024, size: 128*1024, writeCount: 5, isFlush: false},
				{offset: 10*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 8*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
				{offset: 6*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
			},
			extentTypeList: []*ExtentRule{{FileOffset: 128*1024, Size: 10*128*1024}},
		},
		{
			name: 			"test02_flush_continuous_packet",
			operationRules: 		[]*Rule {
				{offset: 128*1024, size: 128*1024, writeCount: 5, isFlush: false},
				{offset: 10*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 8*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 7*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 6*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 9*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
			},
			extentTypeList: []*ExtentRule{{FileOffset: 128*1024, Size: 10*128*1024}},
		},
		{
			name: 			"test02_read_pending_packet",
			operationRules: 		[]*Rule {
				{offset: 128*1024, size: 128*1024, writeCount: 5, isFlush: false},
				{offset: 10*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 8*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
				{offset: 9*128*1024, size: 2*128*1024, isRead: true},
				{offset: 6*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 7*128*1024},
				{FileOffset: 8*128*1024, Size: 3*128*1024},
			},
		},
		{
			name: 			"test02_read_extent",
			operationRules: 		[]*Rule {
				{offset: 128*1024, size: 128*1024, writeCount: 5, isFlush: false},
				{offset: 10*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 8*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
				{offset: 3*128*1024, size: 2*128*1024, isRead: true},
				{offset: 6*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
			},
			extentTypeList: []*ExtentRule{{FileOffset: 128*1024, Size: 10*128*1024}},
		},
		{
			name: 			"test03",
			operationRules: 		[]*Rule {
				// eh满了之后关闭，下一个包跳了offset
				{offset: 128*1024, size: 128*1024, writeCount: 1020, isFlush: false},
				{offset: 1030*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 1025*128*1024, size: 128*1024, writeCount: 5, isFlush: false},
				{offset: 1021*128*1024, size: 128*1024, writeCount: 4, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 1020*128*1024},
				{FileOffset: 1021*128*1024, Size: 9*128*1024},
				{FileOffset: 1030*128*1024, Size: 128*1024},
			},
		},
		{
			name: 			"test04",
			operationRules: 		[]*Rule {
				// eh满了之后关闭，下一个包跳了offset
				{offset: 128*1024, size: 128*1024, writeCount: 1020, isFlush: false},
				{offset: 1022*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 1030*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 1021*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 1023*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
				{offset: 1027*128*1024, size: 128*1024, writeCount: 3, isFlush: false},
				{offset: 1025*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
				{offset: 1031*128*1024, size: 128*1024, writeCount: 1024, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 1020*128*1024},
				{FileOffset: 1021*128*1024, Size: 128*1024},
				{FileOffset: 1022*128*1024, Size: 128*1024},
				{FileOffset: 1023*128*1024, Size: 7*128*1024},
				{FileOffset: 1030*128*1024, Size: 1024*128*1024},
				{FileOffset: (1030+1024)*128*1024, Size: 128*1024},
			},
		},
		{
			name: 			"test05",
			operationRules: 		[]*Rule {
				{offset: 128*1024, size: 128*1024, writeCount: 4, isFlush: false},
				{offset: 5*128*1024, size: 16*1024, writeCount: 1, isFlush: false},
				{offset: 6*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 7*128*1024, size: 16*1024, writeCount: 7, isFlush: false},
				{offset: 9*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: (7*128+7*16)*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: (8*128+7*16)*1024, size: 16*1024, writeCount: 1, isFlush: false},
				{offset: (5*128+16)*1024, size: 112*1024, writeCount: 1, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 9*128*1024},
			},
		},
		{
			name: 			"overwrite_local_pending_packet_01",
			operationRules: 		[]*Rule {
				// overwrite写本地多个pendingPacket
				{offset: 128*1024, size: 128*1024, writeCount: 9, isFlush: false},
				{offset: 12*128*1024, size: 128*1024, writeCount: 5, isFlush: false},
				{offset: 15*128*1024, size: 128*1024, writeCount: 5, isFlush: false},	// overwrite 15*128*1024 ~ 17*128*1024
				{offset: 20*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 10*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 20*128*1024},
			},
		},
		{
			name: 			"overwrite_local_pending_packet_02",
			operationRules: 		[]*Rule {
				// overwrite写本地一个pendingPacket
				{offset: 128*1024, size: 128*1024, writeCount: 9, isFlush: false},
				{offset: 12*128*1024, size: 128*1024, writeCount: 5, isFlush: false},
				{offset: 16*128*1024, size: 128*1024, writeCount: 4, isFlush: false},	// overwrite 16*128*1024 ~ 17*128*1024
				{offset: 20*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 10*128*1024, size: 128*1024, writeCount: 2, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 20*128*1024},
			},
		},
		{
			name: 			"overwrite_local_pending_packet_03",
			operationRules: 		[]*Rule {
				// overwrite写本地多个pendingPacket
				{offset: 128*1024, size: 128*1024, writeCount: 3, isFlush: false},
				{offset: 4*128*1024, size: 16*1024, writeCount: 1, isFlush: false},
				{offset: 7*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 5*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 6*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 9*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: (6*128-16)*1024, size: 32*1024, writeCount: 1, isFlush: false},	// overwrite (6*128-16)*1024 ~ (6*128+16)*1024
				{offset: 11*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 10*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 8*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: (4*128+16)*1024, size: (128-16)*1024, writeCount: 1, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 11*128*1024},
			},
		},
		{
			name: 			"overwrite_local_eh_packet",
			operationRules: 		[]*Rule {
				// overwrite写本地的eh.packet
				{offset: 128*1024, size: 128*1024, writeCount: 9, isFlush: false},
				{offset: 16*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 15*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 10*128*1024, size: 64*1024, writeCount: 1, isFlush: false},
				{offset: (10*128+60)*1024, size: 128*1024, writeCount: 4, isFlush: false},		// overwrite (10*128+60)*1024 ~ (10*128+64)*1024
				{offset: (14*128+60)*1024, size: (128-60)*1024, writeCount: 1, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 16*128*1024},
			},
		},
		{
			name: 			"overwrite_remote_eh_packet",
			operationRules: 		[]*Rule {
				// overwrite超过了eh.packet的范围，flush后写远程
				{offset: 128*1024, size: 128*1024, writeCount: 3, isFlush: false},
				{offset: 4*128*1024, size: 16*1024, writeCount: 1, isFlush: false},
				{offset: 7*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 5*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 6*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 9*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: (4*128-16)*1024, size: 32*1024, writeCount: 1, isFlush: false},	// flush and overwrite (4*128-16)*1024 ~ (4*128+16)*1024
				{offset: 8*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: (4*128+16)*1024, size: (128-16)*1024, writeCount: 1, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 128*1024, Size: 4*128*1024},
				{FileOffset: 5*128*1024, Size: 3*128*1024},
				{FileOffset: 8*128*1024, Size: 1*128*1024},
				{FileOffset: 9*128*1024, Size: 1*128*1024},
			},
		},
		{
			name: 			"tiny_pending_packet",
			operationRules: 		[]*Rule {
				{offset: 0, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 2*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 5*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 10*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
				{offset: 12*128*1024, size: 128*1024, writeCount: 1, isFlush: false},
			},
			extentTypeList: []*ExtentRule{
				{FileOffset: 0, Size: 128*1024},
				{FileOffset: 2*128*1024, Size: 128*1024},
				{FileOffset: 5*128*1024, Size: 128*1024},
				{FileOffset: 10*128*1024, Size: 128*1024},
				{FileOffset: 12*128*1024, Size: 128*1024},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// create local file
			localFile, err := os.Create(path.Join(localTestDir, tt.name))
			defer localFile.Close()
			if err != nil {
				t.Fatalf("TestStreamer_WriteFile_Pending create local file(%v) err: %v", tt.name, err)
			}
			// clean CFS file
			if err := streamer.truncate(context.Background(), 0); err != nil {
				t.Fatalf("TestStreamer_WriteFile_Pending truncate err: %v, name(%v) ino(%v)", err, tt.name, streamer.inode)
			}
			writeIndex := 0
			// write
			for _, rule := range tt.operationRules {
				if rule.isRead {
					time.Sleep(10 * time.Second)
					readCFSData := make([]byte, rule.size)
					readSize, _, err := streamer.read(context.Background(), readCFSData, rule.offset, rule.size)
					if err != nil || readSize != rule.size {
						t.Fatalf("TestStreamer_WriteFile_Pending read CFS file offset(%v) size(%v) err(%v) test(%v) expect size(%v) but size(%v)",
							rule.offset, rule.size, err, tt.name, rule.size, readSize)
					}
					// check data crc
					readLocalData := make([]byte, rule.size)
					_, err = localFile.ReadAt(readLocalData, int64(rule.offset))
					if err != nil {
						t.Fatalf("TestStreamer_WriteFile_Pending read local file offset(%v) size(%v) err(%v) file(%v)",
							rule.offset, rule.size, err, localFile.Name())
					}
					if crc32.ChecksumIEEE(readCFSData[:rule.size]) != crc32.ChecksumIEEE(readLocalData[:rule.size]) {
						t.Fatalf("TestStreamer_WriteFile_Pending failed: test(%v) offset(%v) size(%v) crc is inconsistent",
							tt.name, rule.offset, rule.size)
					}
					continue
				}
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
						t.Fatalf("TestStreamer_WriteFile_Pending write: name(%v) err(%v) total(%v) isROW(%v) expect size(%v)", tt.name, err, total, isROW, size)
					}
					if _, err = localFile.WriteAt(writeData, int64(offset)); err != nil {
						t.Fatalf("TestStreamer_WriteFile_Pending failed: write local file err(%v) name(%v)", err, localFile.Name())
					}
				}
				// flush
				if rule.isFlush {
					if err = streamer.flush(context.Background(), true); err != nil {
						t.Fatalf("TestStreamer_WriteFile_Pending cfs flush err(%v) test(%v)", err, tt.name)
					}
					if err = localFile.Sync(); err != nil {
						t.Fatalf("TestStreamer_WriteFile_Pending local flush err(%v) file(%v)", err, localFile.Name())
					}
				}
			}
			// flush
			fmt.Println("TestStreamer_WriteFile_Pending: start flush file")
			if err = streamer.flush(context.Background(), true); err != nil {
				t.Fatalf("TestStreamer_WriteFile_Pending cfs flush err(%v) test(%v)", err, tt.name)
			}
			if err = localFile.Sync(); err != nil {
				t.Fatalf("TestStreamer_WriteFile_Pending local flush err(%v) file(%v)", err, localFile.Name())
			}
			// check extent
			fmt.Println("TestStreamer_WriteFile_Pending: start check extent")
			extents := streamer.extents.List()
			if len(extents) == 0 || len(extents) != len(tt.extentTypeList) {
				t.Fatalf("TestStreamer_WriteFile_Pending failed: test(%v) expect extent length(%v) but(%v) extents(%v)",
					tt.name, len(tt.extentTypeList), len(extents), extents)
			}
			unexpectedExtent := false
			for i, ext := range extents {
				expectExtent := tt.extentTypeList[i]
				if ext.FileOffset != expectExtent.FileOffset || ext.Size != expectExtent.Size {
					unexpectedExtent = true
					break
				}
			}
			if unexpectedExtent {
				t.Fatalf("TestStreamer_WriteFile_Pending failed: test(%v) expect extent list(%v) but(%v)",
					tt.name, tt.extentTypeList, extents)
			}
			fmt.Println("TestStreamer_WriteFile_Pending: extent list: ", extents)
			// read data and check crc
			for _, ext := range extents {
				fmt.Println("TestStreamer_WriteFile_Pending: start read file extent: ", ext)
				offset := ext.FileOffset
				size := int(ext.Size)
				readCFSData := make([]byte, size)
				readSize, _, err := streamer.read(context.Background(), readCFSData, offset, size)
				if err != nil || readSize != size {
					t.Fatalf("TestStreamer_WriteFile_Pending read CFS file offset(%v) size(%v) err(%v) test(%v) expect size(%v) but size(%v)",
						offset, size, err, tt.name, size, readSize)
				}
				// check data crc
				readLocalData := make([]byte, size)
				_, err = localFile.ReadAt(readLocalData, int64(offset))
				if err != nil {
					t.Fatalf("TestStreamer_WriteFile_Pending read local file offset(%v) size(%v) err(%v) file(%v)", offset, size, err, localFile.Name())
				}
				if crc32.ChecksumIEEE(readCFSData[:size]) != crc32.ChecksumIEEE(readLocalData[:size]) {
					t.Fatalf("TestStreamer_WriteFile_Pending failed: test(%v) offset(%v) size(%v) crc is inconsistent", tt.name, offset, size)
				}
			}
			return
		})
	}
}

func TestStreamer_WriteFile_discontinuous(t *testing.T)  {
	logDir := "/tmp/logs/cfs"
	log.InitLog(logDir, "test", log.DebugLevel, nil)

	// create inode
	mw, ec, err := creatHelper(t)
	if err != nil {
		t.Fatalf("TestExtentHandler_PendingPacket: create wrapper err(%v)", err)
	}
	ec.SetEnableWriteCache(true)
	ec.tinySize = util.DefaultTinySizeLimit
	inodeInfo, err := mw.Create_ll(context.Background(), 1, "TestStreamer_WriteFile_discontinuous", 0644, 0, 0, nil)
	if err != nil {
		t.Fatalf("TestExtentHandler_PendingPacket: creat inode failed, err(%v)", err)
	}
	streamer := NewStreamer(ec, inodeInfo.Inode, ec.streamerConcurrentMap.GetMapSegment(inodeInfo.Inode), false, false)

	localPath := "/tmp/TestStreamer_WriteFile_discontinuous"
	localFile, _ := os.Create(localPath)

	defer func() {
		streamer.done <- struct{}{}
		mw.Delete_ll(context.Background(), 1, "TestStreamer_WriteFile_discontinuous", false)
		log.LogFlush()
		localFile.Close()
	}()
	// discontinuous write
	streamer.refcnt++
	writeSize := 128*1024
	if err = writeLocalAndCFS(localFile, streamer, 0, writeSize); err != nil {
		t.Fatalf("TestStreamer_WriteFile_discontinuous: err(%v)", err)
		return
	}
	if err = writeLocalAndCFS(localFile, streamer, 2*128*1024, writeSize); err != nil {
		t.Fatalf("TestStreamer_WriteFile_discontinuous: err(%v)", err)
		return
	}
	if err = writeLocalAndCFS(localFile, streamer, 4*128*1024, writeSize); err != nil {
		t.Fatalf("TestStreamer_WriteFile_discontinuous: err(%v)", err)
		return
	}
	time.Sleep(30 * time.Second)
	localFile.Sync()
	// verify data
	if err = verifyLocalAndCFS(localFile, streamer, 0, writeSize); err != nil {
		t.Fatalf("TestStreamer_WriteFile_discontinuous: err(%v)", err)
		return
	}
	if err = verifyLocalAndCFS(localFile, streamer, 2*128*1024, writeSize); err != nil {
		t.Fatalf("TestStreamer_WriteFile_discontinuous: err(%v)", err)
		return
	}
	if err = verifyLocalAndCFS(localFile, streamer, 4*128*1024, writeSize); err != nil {
		t.Fatalf("TestStreamer_WriteFile_discontinuous: err(%v)", err)
		return
	}
}

func writeLocalAndCFS(localF *os.File, streamer *Streamer, offset int64, size int) error {
	n := rand.Intn(5)
	writeBytes := make([]byte, size)
	for i := 0; i < size; i++ {
		writeBytes[i] = byte(n)
	}
	localF.WriteAt(writeBytes, offset)
	n, _, err := streamer.write(context.Background(), writeBytes, uint64(offset), size, false, false)
	if err != nil || n != size {
		return fmt.Errorf("write file err(%v) write size(%v)", err, size)
	}
	return nil
}

func verifyLocalAndCFS(localF *os.File, streamer *Streamer, offset int64, size int) error {
	readLocalData := make([]byte, size)
	readCFSData := make([]byte, size)
	localF.ReadAt(readLocalData, offset)
	n, _, err := streamer.read(context.Background(), readCFSData, uint64(offset), size)
	if err != nil || n != size {
		return fmt.Errorf("read file err(%v) read size(%v)", err, size)
	}
	if crc32.ChecksumIEEE(readCFSData[:size]) != crc32.ChecksumIEEE(readLocalData[:size]) {
		return fmt.Errorf("offset(%v) size(%v) crc is inconsistent", offset, size)
	}
	return nil
}