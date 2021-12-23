package datanode

import (
	"bytes"
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