package meta

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/chubaofs/chubaofs/proto"
)

const (
	ltptestVolume = "ltptest"
	ltptestMaster = "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
	readFilePath  = "/cfs/mnt/testread.txt"
)

func TestMetaConsistenceRead(t *testing.T) {
	mw, err := NewMetaWrapper(&MetaConfig{
		Volume:        ltptestVolume,
		Masters:       strings.Split(ltptestMaster, ","),
		ValidateOwner: true,
		Owner:         ltptestVolume,
	})
	if err != nil {
		t.Fatalf("NewMetaWrapper: err(%v) vol(%v)", err, ltptestVolume)
	}
	var readFile *os.File
	if readFile, err = os.Create(readFilePath); err != nil {
		t.Fatalf("create file failed: err(%v) file(%v)", err, readFilePath)
	}
	writeDataToFile(t, readFile)
	var fInfo os.FileInfo
	if fInfo, err = os.Stat(readFilePath); err != nil {
		t.Fatalf("stat file: err(%v) file(%v)", err, readFilePath)
	}
	inode := fInfo.Sys().(*syscall.Stat_t).Ino
	mp := mw.getPartitionByInode(context.Background(), inode)

	req := &proto.GetExtentsRequest{
		VolName:     mw.volname,
		PartitionID: mp.PartitionID,
		Inode:       inode,
	}
	packet := proto.NewPacketReqID(context.Background())
	packet.Opcode = proto.OpMetaExtentsList
	packet.PartitionID = mp.PartitionID
	err = packet.MarshalData(req)
	if err != nil {
		t.Fatalf("packet marshal data: err(%v) packet(%v)", err, packet)
	}

	consistResp1 := getExtentsByConsistenceRead(t, mw, mp, packet, true)
	leaderResp := getExtentsByLeaderRead(t, mw, mp, packet)
	if consistResp1.Size != leaderResp.Size || consistResp1.Generation != leaderResp.Generation || !equalExtents(consistResp1, leaderResp) {
		t.Fatalf("data is not consistent: consistResp1(%v) leaderResp(%v)", consistResp1, leaderResp)
	}

	consistResp2 := getExtentsByConsistenceRead(t, mw, mp, packet, false)
	if consistResp2.Size != leaderResp.Size || consistResp2.Generation != leaderResp.Generation || !equalExtents(consistResp2, leaderResp) {
		t.Fatalf("data is not consistent: consistResp1(%v) leaderResp(%v)", consistResp1, leaderResp)
	}
}

func writeDataToFile(t *testing.T, readFile *os.File) {
	fmt.Println("write to file: ", readFile.Name())
	var err error
	writeData := "TestMetaConsistenceRead"
	writeBytes := []byte(writeData)
	writeOffset := int64(0)
	for i := 0; i < 1024; i++ {
		var writeLen int
		if writeLen, err = readFile.WriteAt(writeBytes, writeOffset); err != nil {
			t.Fatalf("write file failed: err(%v) expect write length(%v) but (%v)", err, len(writeBytes), writeLen)
		}
		writeOffset += int64(writeLen)
	}
	if err = readFile.Sync(); err != nil {
		t.Errorf("sync file failed: err(%v) file(%v)", err, readFilePath)
	}
}

func equalExtents(consistResp *proto.GetExtentsResponse, leaderResp *proto.GetExtentsResponse) bool {
	if len(consistResp.Extents) != len(leaderResp.Extents) {
		return false
	}
	fmt.Println("extents length: ", len(consistResp.Extents))
	for i, ext1 := range consistResp.Extents {
		ext2 := leaderResp.Extents[i]
		if ext1.String() != ext2.String() {
			return false
		}
	}
	return true
}

func getExtentsByConsistenceRead(t *testing.T, mw *MetaWrapper, mp *MetaPartition, packet *proto.Packet, strongConsist bool) (resp *proto.GetExtentsResponse) {
	var (
		respPacket *proto.Packet
		err        error
	)
	if respPacket, err = mw.readConsistentFromHosts(context.Background(), mp, packet, strongConsist); err != nil {
		t.Fatalf("getExtentsByConsistenceRead: err(%v) mp(%v) reqPacket(%v)", err, mp, packet)
	}
	if status := parseStatus(respPacket.ResultCode); status != statusOK {
		t.Fatalf("getExtentsByConsistenceRead: status(%v) is not ok, mp(%v) resp(%v) req(%v)", status, mp, respPacket, packet)
	}
	resp = new(proto.GetExtentsResponse)
	err = respPacket.UnmarshalData(resp)
	if err != nil {
		t.Fatalf("getExtentsByConsistenceRead: response packet unmarshal data err(%v) resp(%v)", err, respPacket)
	}
	return
}

func getExtentsByLeaderRead(t *testing.T, mw *MetaWrapper, mp *MetaPartition, packet *proto.Packet) (resp *proto.GetExtentsResponse) {
	var (
		respPacket *proto.Packet
		err        error
	)
	if respPacket, _, err = mw.sendToMetaPartition(context.Background(), mp, packet, mp.LeaderAddr); err != nil {
		t.Fatalf("getExtentsByLeaderRead: err(%v) mp(%v) reqPacket(%v)", err, mp, packet)
	}
	if status := parseStatus(respPacket.ResultCode); status != statusOK {
		t.Fatalf("getExtentsByLeaderRead: status(%v) is not ok, mp(%v) resp(%v) req(%v)", status, mp, respPacket, packet)
	}
	resp = new(proto.GetExtentsResponse)
	err = respPacket.UnmarshalData(resp)
	if err != nil {
		t.Fatalf("getExtentsByLeaderRead: response packet unmarshal data err(%v) resp(%v)", err, respPacket)
	}
	return
}

func TestExcludeLeaner(t *testing.T) {
	mp := &MetaPartition{
		PartitionID: 99,
		Members:     []string{"192.168.0.21:17020", "192.168.0.22:17020", "192.168.0.23:17020", "192.168.0.24:17020", "192.168.0.25:17020"},
		Learners:    []string{"192.168.0.21:17020", "192.168.0.25:17020"},
	}
	voters := excludeLearner(mp)
	if strings.Join(voters, ",") != strings.Join([]string{"192.168.0.22:17020", "192.168.0.23:17020", "192.168.0.24:17020"}, ",") {
		t.Fatalf("TestExcludeLeaner: failed, actual(%v)", voters)
	}
	return
}