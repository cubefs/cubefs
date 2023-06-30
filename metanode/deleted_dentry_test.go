package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestNewDeletedDentry(t *testing.T) {
	var parentID uint64 = 1
	name := "d1"
	timestamp := time.Now().UnixNano() / 1000
	dd := newPrimaryDeletedDentry(parentID, name, timestamp, 0)
	if dd.Name != name {
		t.FailNow()
	}

	if dd.ParentId != parentID {
		t.FailNow()
	}

	if dd.Timestamp != timestamp {
		t.FailNow()
	}
}

func compareDeletedDentry(dd1, dd2 *DeletedDentry) bool {
	if dd1.ParentId != dd2.ParentId {
		return false
	}

	if dd1.Type != dd2.Type {
		return false
	}

	if dd1.Inode != dd2.Inode {
		return false
	}

	if dd1.Name != dd2.Name {
		return false
	}

	if dd1.Timestamp != dd2.Timestamp {
		return false
	}
	return true
}

func TestDeletedDentry_Marshal(t *testing.T) {
	var parentID uint64 = 1
	var id uint64 = 10240
	var dtype uint32 = 1
	name := "d1"
	timestamp := time.Now().UnixNano() / 1000
	dd := newPrimaryDeletedDentry(parentID, name, timestamp, 0)
	dd.Inode = id
	dd.Type = dtype

	data, err := dd.Marshal()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	newDD := new(DeletedDentry)
	err = newDD.Unmarshal(data)
	if err != nil {
		t.Errorf("failed to unmarshal for unit test, err: %v", err.Error())
		t.FailNow()
	}

	if compareDeletedDentry(dd, newDD) == false {
		t.FailNow()
	}
}

func TestDeletedDentry_Reset(t *testing.T) {
	tree := NewBtree()
	var ino uint64
	for ino = 2; ino < 10*10000; ino++ {
		d := newPrimaryDeletedDentry(ino, "t", ts, 0)
		tree.ReplaceOrInsert(d, false)
	}

	cnt := 0
	go func() {
		var item btree.Item
		tree.Ascend(func(i BtreeItem) bool {
			item = tree.Delete(i)
			if item == nil {
				t.Logf("d:%v", i.(*DeletedDentry))
				t.FailNow()
			}
			log.LogDebugf("item: %v", item.(*DeletedDentry))
			fmt.Printf("item: %v\n", item.(*DeletedDentry))
			cnt++
			return true
		})
		//t.Logf("last item: %v", item.(*DeletedDentry))
		fmt.Printf("count: %v\n", cnt)
		log.LogDebugf("count: %v\n", cnt)
	}()
	//time.Sleep(3*time.Second)
	//log.LogFlush()
	t.Logf("count: %v", cnt)
	//tree.Reset()
}

func TestDeletedDentry_Copy(t *testing.T) {
	var parentID uint64 = 1
	var id uint64 = 10240
	var dtype uint32 = 1
	name := "d1"
	timestamp := time.Now().UnixNano() / 1000
	dd := newPrimaryDeletedDentry(parentID, name, timestamp, 0)
	dd.Inode = id
	dd.Type = dtype

	newDD := dd.Copy().(*DeletedDentry)
	dd.ParentId++
	if newDD.ParentId == dd.ParentId {
		t.FailNow()
	}
}

func TestDeletedDentry_Less(t *testing.T) {
	var parentID uint64 = 10
	var id uint64 = 10240
	var dtype uint32 = proto.Mode(os.ModeDir)
	name := "d1"
	timestamp := time.Now().UnixNano() / 1000
	dd := newPrimaryDeletedDentry(parentID, name, timestamp, 0)
	dd.Inode = id
	dd.Type = dtype

	dentry := new(Dentry)
	if dd.Less(dentry) != false {
		t.Errorf("[%v] [%v]", dd, dentry)
		t.FailNow()
	}

	newDD := dd.Copy().(*DeletedDentry)
	if dd.Less(newDD) != false {
		t.Errorf("[%v] [%v]", dd, newDD)
		t.FailNow()
	}

	newDD.ParentId++
	if dd.Less(newDD) == false {
		t.Errorf("[%v] [%v]", dd, newDD)
		t.FailNow()
	}
	newDD.ParentId -= 2
	if dd.Less(newDD) == true {
		t.Errorf("[%v] [%v]", dd, newDD)
		t.FailNow()
	}

	newDD = dd.Copy().(*DeletedDentry)
	newDD.Name = "d0"
	if dd.Less(newDD) == true {
		t.Errorf("[%v] [%v]", dd, newDD)
		t.FailNow()
	}
	newDD.Name = "d2"
	if dd.Less(newDD) == false {
		t.Errorf("[%v] [%v]", dd, newDD)
		t.FailNow()
	}

	newDD = dd.Copy().(*DeletedDentry)
	newDD.Timestamp--
	if dd.Less(newDD) == true {
		t.Errorf("[%v] [%v]", dd, newDD)
		t.FailNow()
	}
	newDD.Timestamp += 2
	if dd.Less(newDD) == false {
		t.Errorf("[%v] [%v]", dd, newDD)
		t.FailNow()
	}
}

func TestDeletedDentry_Less2(t *testing.T) {
	den := newPrimaryDeletedDentry(10, "f1", ts, 100)
	start := newPrimaryDeletedDentry(10, "", 0, 0)
	end:= newPrimaryDeletedDentry(10+1, "", 0, 0)
	if den.Less(start) {
		t.Errorf("den: %v, start: %v", den, start)
		t.FailNow()
	}
	if !den.Less(end) {
		t.Errorf("den: %v, end: %v", den, end)
		t.FailNow()
	}
}

func TestDeletedDentry_EncodeBinary(t *testing.T) {
	delDentryExpect := newDeletedDentry(&Dentry{
		ParentId: 10,
		Name:     "f1",
		Inode:    100,
		Type:     460,
	}, ts, "127.0.0.1")
	data := make([]byte, delDentryExpect.BinaryDataLen())
	_, _ = delDentryExpect.EncodeBinary(data)

	delDentry := new(DeletedDentry)
	if err := delDentry.Unmarshal(data); err != nil {
		t.Errorf("unmarshal failed:%v", err)
	}

	assert.Equal(t, delDentryExpect, delDentry)
}
