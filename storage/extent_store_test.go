package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"hash/crc32"
	"math/rand"
	"os"
	"testing"
	"time"
)

var (
	s *ExtentStore
	t *testing.T
)

type RandomWriteOpItem struct {
	opcode   uint8
	extentID uint64
	offset   int64
	size     int64
	data     []byte
	crc      uint32
}

const (
	BinaryMarshalMagicVersion = 0xFF
)

func MarshalRandWriteRaftLog(opcode uint8, extentID uint64, offset, size int64, data []byte, crc uint32) (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(8 + 8*2 + 4 + int(size) + 4 + 4)
	if err = binary.Write(buff, binary.BigEndian, uint32(BinaryMarshalMagicVersion)); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, opcode); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, extentID); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, offset); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, size); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, crc); err != nil {
		return
	}
	if _, err = buff.Write(data); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

func init() {
	rand.Seed(time.Now().UnixNano())
	partitionID := rand.Uint64() % 500
	os.RemoveAll("/tmp/extentStore")
	defaultStoreSize := 128 * 1024 * 1024 * 1024
	storePath := fmt.Sprintf("/tmp/extentStore/datapartition_%v_%v", partitionID, defaultStoreSize)
	var err error
	s, err = NewExtentStore(storePath, partitionID, defaultStoreSize)
	if err != nil {
		t.Errorf("init storage error %v", err.Error())
		t.FailNow()
	}

}

func UnmarshalRandWriteRaftLog(raw []byte) (opItem *RandomWriteOpItem, err error) {
	opItem = new(RandomWriteOpItem)
	buff := bytes.NewBuffer(raw)
	var version uint32
	if err = binary.Read(buff, binary.BigEndian, &version); err != nil {
		return
	}

	if version != BinaryMarshalMagicVersion {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.opcode); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.extentID); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.offset); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.crc); err != nil {
		return
	}
	opItem.data = make([]byte, opItem.size)
	if _, err = buff.Read(opItem.data); err != nil {
		return
	}

	return
}

type EnableTruncateRaftLogCmd struct {
	PartitionID            uint64
	DisableTruncateRaftLog bool
}

func TestExtentStore_Write_ExtentNotFoundError(t *testing.T) {
	var err error
	cmd := new(EnableTruncateRaftLogCmd)
	cmd.PartitionID = s.partitionID
	cmd.DisableTruncateRaftLog = false
	data, _ := json.Marshal(cmd)
	crc := crc32.ChecksumIEEE(data)
	var val []byte
	val, err = MarshalRandWriteRaftLog(proto.OpEnableTruncateRaftLog, 0, 0, int64(len(data)), data, crc)
	if err != nil {
		err = fmt.Errorf("MarshalRandWriteRaftLog error (%v)", err)
		t.Fatalf(err.Error())
		return
	}
	var (
		opItem *RandomWriteOpItem
	)
	opItem, err = UnmarshalRandWriteRaftLog(val)
	for i := 0; i < 2; i++ {
		err = s.Write(nil, opItem.extentID, opItem.offset, opItem.size, opItem.data, opItem.crc, RandomWriteType, opItem.opcode == proto.OpSyncRandomWrite)
		if err != ExtentNotFoundError {
			t.Fatalf("expect error(%v),actual error(%v)", ExtentNotFoundError, err)
			return
		}
	}
}
