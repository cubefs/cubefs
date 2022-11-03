package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/chubaofs/chubaofs/proto"
	"hash/crc32"
	"math/rand"
	"testing"
	"time"
)


func init() {
	rand.Seed(time.Now().UnixNano())
}
var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestUnmarshalRandWriteRaftLog(t *testing.T) {
	data:=RandStringRunes(100*1024)
	item:=GetRandomWriteOpItem()
	item.data=([]byte)(data)
	item.opcode=proto.OpRandomWrite
	item.extentID=2
	item.offset=100
	item.size=100*1024
	item.crc=crc32.ChecksumIEEE(item.data)
	oldMarshalResult,err:=MarshalRandWriteRaftLog(item.opcode,item.extentID,item.offset,item.size,item.data,item.crc)
	if err!=nil {
		t.Logf("MarshalRandWriteRaftLog Item(%v) failed (%v)",item,err)
		t.FailNow()
	}

	newOpItem,err:=UnmarshalRandWriteRaftLog(oldMarshalResult)
	if err!=nil {
		t.Logf("UnMarshalRandWriteRaftLog Item(%v) failed (%v)",item,err)
		t.FailNow()
	}
	if item.opcode!=newOpItem.opcode || item.extentID!=newOpItem.extentID|| item.offset!=newOpItem.offset ||
		item.size!=newOpItem.size || item.crc!=newOpItem.crc || bytes.Compare(([]byte)(data),newOpItem.data) !=0{
		t.Logf("UnMarshalRandWriteRaftLog Item(%v) newItem(%v) failed ",item,err)
		t.FailNow()
	}
}

func TestUnmarshalRandWriteRaftLogV3(t *testing.T) {
	data:=RandStringRunes(100*1024)
	item:=GetRandomWriteOpItem()
	item.data=make([]byte,len(data)+proto.RandomWriteRaftLogV3HeaderSize)
	copy(item.data[proto.RandomWriteRaftLogV3HeaderSize:],data)
	item.opcode=proto.OpRandomWrite
	item.extentID=2
	item.offset=100
	item.size=100*1024
	item.crc=crc32.ChecksumIEEE(item.data[proto.RandomWriteRaftLogV3HeaderSize:])

	oldMarshalResult,err:=MarshalRandWriteRaftLogV3(item.opcode,item.extentID,item.offset,item.size,item.data,item.crc)
	if err!=nil {
		t.Logf("MarshalRandWriteRaftLog Item(%v) failed (%v)",item,err)
		t.FailNow()
	}

	newOpItem,err:=UnmarshalRandWriteRaftLog(oldMarshalResult)
	if err!=nil {
		t.Logf("UnMarshalRandWriteRaftLog Item(%v) failed (%v)",item,err)
		t.FailNow()
	}
	if item.opcode!=newOpItem.opcode || item.extentID!=newOpItem.extentID|| item.offset!=newOpItem.offset ||
		item.size!=newOpItem.size || item.crc!=newOpItem.crc || bytes.Compare(([]byte)(data),newOpItem.data) !=0{
		t.Logf("UnMarshalRandWriteRaftLog Item(%v) newItem(%v) failed ",item,err)
		t.FailNow()
	}
}

func TestBinaryUnmarshalRandWriteRaftLogV3(t *testing.T) {
	data:=RandStringRunes(100*1024)
	item:=GetRandomWriteOpItem()
	item.data=make([]byte,len(data)+proto.RandomWriteRaftLogV3HeaderSize)
	copy(item.data[proto.RandomWriteRaftLogV3HeaderSize:],data)
	item.opcode=proto.OpRandomWrite
	item.extentID=2
	item.offset=100
	item.size=100*1024
	item.crc=crc32.ChecksumIEEE(item.data[proto.RandomWriteRaftLogV3HeaderSize:])

	oldMarshalResult,err:=MarshalRandWriteRaftLogV3(item.opcode,item.extentID,item.offset,item.size,item.data,item.crc)
	if err!=nil {
		t.Logf("MarshalRandWriteRaftLog Item(%v) failed (%v)",item,err)
		t.FailNow()
	}

	newOpItem, err := BinaryUnmarshalRandWriteRaftLogV3(oldMarshalResult)
	if err!=nil {
		t.Logf("BinaryUnmarshalRandWriteRaftLogV3 Item(%v) failed (%v)",item,err)
		t.FailNow()
	}
	if item.opcode!=newOpItem.opcode || item.extentID!=newOpItem.extentID|| item.offset!=newOpItem.offset ||
		item.size!=newOpItem.size || item.crc!=newOpItem.crc || bytes.Compare(([]byte)(data),newOpItem.data) !=0{
		t.Logf("BinaryUnmarshalRandWriteRaftLogV3 Item(%v) newItem(%v) failed ",item,err)
		t.FailNow()
	}
}

func TestUnmarshalOldVersionRaftLog(t *testing.T) {
	data:=RandStringRunes(100*1024)
	item:=GetRandomWriteOpItem()
	item.data=make([]byte,len(data))
	copy(item.data[:],data)
	item.opcode=proto.OpRandomWrite
	item.extentID=2
	item.offset=100
	item.size=100*1024
	item.crc=crc32.ChecksumIEEE(item.data[proto.RandomWriteRaftLogV3HeaderSize:])
	v, err := MarshalOldVersionRandWriteOpItemForTest(item)
	if err != nil {
		t.Logf("MarshalOldVersionRandWriteOpItemForTest Item(%v) failed (%v)", item, err)
		t.FailNow()
	}
	raftOpItem := new(RaftCmdItem)
	raftOpItem.V = v
	raftOpItem.Op = uint32(item.opcode)
	raftOpItemBytes, err := json.Marshal(raftOpItem)
	if err != nil {
		t.Logf("json Marshal raftOpItem(%v) failed (%v)", raftOpItem, err)
		t.FailNow()
	}
	newOpItem, err := UnmarshalOldVersionRaftLog(raftOpItemBytes)
	if err != nil {
		t.Logf("UnmarshalOldVersionRaftLog Item(%v) failed (%v)", item, err)
		t.FailNow()
	}

	if item.opcode!=newOpItem.opcode || item.extentID!=newOpItem.extentID|| item.offset!=newOpItem.offset ||
		item.size!=newOpItem.size || item.crc!=newOpItem.crc || bytes.Compare(([]byte)(data), newOpItem.data) !=0{
		t.Logf("UnmarshalOldVersionRaftLog Item(%v) newItem(%v) failed ", item, err)
		t.FailNow()
	}
}

func MarshalOldVersionRandWriteOpItemForTest(item *rndWrtOpItem) (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(buff, binary.BigEndian, item.extentID); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, item.offset); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, item.size); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, item.crc); err != nil {
		return
	}
	if _, err = buff.Write(item.data); err != nil {
		return
	}
	result = buff.Bytes()
	return
}