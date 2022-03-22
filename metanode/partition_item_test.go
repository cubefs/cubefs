package metanode

import (
	"testing"
	"time"
)

//deep equal is false, because k is not nil after decode
func TestMetaItem_EncodeAndDecodeBinaryWithVersionCase01(t *testing.T) {
	item := NewMetaItem(opFSMInsertInnerData, nil, nil)
	item.TrashEnable = false
	item.From = "127.0.0.1:6008"
	item.Timestamp = time.Now().UnixNano() / 1000
	item.V = []byte("testMetaItemEncodeBinary")
	bytes, err := item.MarshalBinaryWithVersion(ItemMarshalBinaryV1)
	if err != nil {
		t.Logf("encode binary with version %v failed:%v", ItemMarshalBinaryV1, err)
		t.FailNow()
	}

	itemDecode := NewMetaItem(0, nil, nil)
	if err = itemDecode.UnmarshalBinaryWithVersion(ItemMarshalBinaryV1, bytes); err != nil {
		t.Logf("decode binary with version %v failed:%v", ItemMarshalBinaryV1, err)
		t.FailNow()
	}

	if !equal(item, itemDecode) {
		t.Logf("item mismatch, expect:\n%v\n, actual:\n%v\n", item, itemDecode)
		t.FailNow()
	}
}

func TestMetaItem_UnmarshalRaftMsgCase01(t *testing.T) {
	item := NewMetaItem(opFSMInsertInnerData, nil, nil)
	item.TrashEnable = false
	item.From = "127.0.0.1:6008"
	item.Timestamp = time.Now().UnixNano() / 1000
	item.V = []byte("testMetaItemEncodeBinary")
	//encode
	bytes, err := item.MarshalBinaryWithVersion(ItemMarshalBinaryV1)
	if err != nil {
		t.Logf("marshal binary with version %v failed:%v", ItemMarshalBinaryV1, err)
		t.FailNow()
	}

	msg := &MetaItem{}
	if err = msg.UnmarshalRaftMsg(bytes); err != nil {
		t.Logf("unmarshal raft log failed:%v", err)
		t.FailNow()
	}

	if !equal(item, msg) {
		t.Logf("item mismatch, expect:\n%v\n, actual:\n%v\n", item, msg)
		t.FailNow()
	}
}

func TestMetaItem_UnmarshalRaftMsgCase02(t *testing.T) {
	item := NewMetaItem(opFSMInsertInnerData, nil, nil)
	item.TrashEnable = false
	item.From = "127.0.0.1:6008"
	item.Timestamp = time.Now().UnixNano() / 1000
	item.V = []byte("testMetaItemEncodeBinary")
	//encode
	bytes, err := item.MarshalJson()
	if err != nil {
		t.Logf("marshal item failed:%v", err)
		t.FailNow()
	}

	msg := &MetaItem{}
	if err = msg.UnmarshalRaftMsg(bytes); err != nil {
		t.Logf("unmarshal raft log failed:%v", err)
		t.FailNow()
	}

	if !equal(item, msg) {
		t.Logf("item mismatch, expect:\n%v\n, actual:\n%v\n", item, msg)
		t.FailNow()
	}
}

func equal(v1, v2 *MetaItem) bool {
	if v1.Op != v2.Op {
		return false
	}

	if v1.Timestamp != v2.Timestamp {
		return false
	}

	if v1.From != v1.From {
		return false
	}

	if v1.TrashEnable != v2.TrashEnable {
		return false
	}

	if len(v1.K) != len(v2.K) {
		return false
	}

	for index, b := range v1.K {
		if v2.K[index] != b {
			return false
		}
	}

	if len(v1.V) != len(v2.V) {
		return false
	}

	for index, b := range v1.V {
		if v2.V[index] != b {
			return false
		}
	}

	return true
}