package metanode

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func SetXAttrInterTest(t *testing.T, leader, follower *metaPartition) {
	//create inodes
	inos, err := createInodesForTest(leader, follower, 10, 470, 0, 0)
	if err != nil {
		t.Error(err)
		return
	}
	//set xattr
	for _, ino := range inos {
		for index := 0; index < 5; index++ {
			req := &proto.SetXAttrRequest{
				Inode: ino,
				Key: fmt.Sprintf("test_%v", index),
				Value: fmt.Sprintf("test_%v_%v", index, ino),
			}
			packet := &Packet{}
			if err = leader.SetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("set xattr failed, req:%v, err:%v", req, err)
				return
			}
		}
	}

	//validate
	for _, ino := range inos {
		for index := 0; index < 5; index++ {
			req := &proto.GetXAttrRequest{
				Inode: ino,
				Key: fmt.Sprintf("test_%v", index),
			}

			//get xattr from leader
			packet := &Packet{}
			if err = leader.GetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("get xattr failed, req:%v, err:%v", req, err)
				return
			}
			respInLeader := &proto.GetXAttrResponse{}
			if err = packet.UnmarshalData(respInLeader); err != nil {
				t.Errorf("unmarshal get xattr response failed, req:%v, err:%v", req, err)
				return
			}

			//get xattr from follower
			packet = &Packet{}
			if err = follower.GetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("get xattr failed, req:%v, err:%v", req, err)
				return
			}
			respInFollower := &proto.GetXAttrResponse{}
			if err = packet.UnmarshalData(respInFollower); err != nil {
				t.Errorf("unmarshal get xattr response failed, req:%v, err:%v", req, err)
				return
			}

			if !reflect.DeepEqual(respInLeader, respInFollower) {
				t.Fatalf("resp mismatch, respInleader[inode:%v, key:%s, value:%s]," +
					" respInFollower[inode:%v, key:%s, value:%s]", respInLeader.Inode, respInLeader.Key, respInLeader.Value,
					respInFollower.Inode, respInFollower.Key, respInFollower.Value)
			}

			if strings.Compare(respInLeader.Value, fmt.Sprintf("test_%v_%v", index, ino)) != 0 {
				t.Fatalf("extend value mismatch, except:%s, actual:%s",
					fmt.Sprintf("test_%v_%v", index, ino), respInLeader.Value)
				return
			}
		}
	}
	return
}

func TestMetaPartition_SetXAttrCase01(t *testing.T) {
	//leader is mem mode
	dir := "set_xattr_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	SetXAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	SetXAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func BatchGetXAttrInterTest(t *testing.T, leader, follower *metaPartition) {
	//create inodes
	inos, err := createInodesForTest(leader, follower, 10, 470, 0, 0)
	if err != nil {
		t.Error(err)
		return
	}
	//set xattr
	keys := make([]string, 0)
	for _, ino := range inos {
		for index := 0; index < 5; index++ {
			req := &proto.SetXAttrRequest{
				Inode: ino,
				Key: fmt.Sprintf("test_%v", index),
				Value: fmt.Sprintf("test_%v_%v", index, ino),
			}
			keys = append(keys, req.Key)
			packet := &Packet{}
			if err = leader.SetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("set xattr failed, req:%v, err:%v", req, err)
				return
			}
		}
	}

	req := &proto.BatchGetXAttrRequest{
		Inodes: inos,
		Keys: keys,
	}
	packet := &Packet{}
	if err = leader.BatchGetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch get xattr error[%v] or resultCode mismatch, resulCode expect:OpOk(0xF0), actual:%v",
			err, packet.ResultCode)
		return
	}
	respFromLeader := &proto.BatchGetXAttrResponse{}
	if err = packet.UnmarshalData(respFromLeader); err != nil {
		t.Errorf("unmarshal batch get xattr response failed, req:%v, err:%v", req, err)
		return
	}

	packet = &Packet{}
	if err = follower.BatchGetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("batch get xattr error[%v] or resultCode mismatch, resulCode expect:OpOk(0xF0), actual:%v",
			err, packet.ResultCode)
		return
	}
	respFromFollower := &proto.BatchGetXAttrResponse{}
	if err = packet.UnmarshalData(respFromFollower); err != nil {
		t.Errorf("unmarshal batch get xattr response failed, req:%v, err:%v", req, err)
		return
	}

	//validate
	if !reflect.DeepEqual(respFromLeader, respFromFollower) {
		t.Fatalf("response from leader and follower mistmatch")
	}

	for _, xattr := range respFromLeader.XAttrs {
		inode := xattr.Inode
		for key, value := range xattr.XAttrs {
			if strings.Compare(value, fmt.Sprintf("%s_%v", key, inode)) != 0 {
				t.Errorf("different extend value, except:%s, actual:%s",
					fmt.Sprintf("%s_%v", key, inode), value)
				return
			}
		}
	}
	return
}

func TestMetaPartition_BatchGetXAttrCase01(t *testing.T) {
	//leader is mem mode
	dir := "batch_get_xattr_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	BatchGetXAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	BatchGetXAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func RemoveXAttrInterTest(t *testing.T, leader, follower *metaPartition) {
	//create inodes
	inos, err := createInodesForTest(leader, follower, 10, 470, 0, 0)
	if err != nil {
		t.Error(err)
		return
	}
	//set xattr
	keys := make([]string, 0)
	for _, ino := range inos {
		for index := 0; index < 5; index++ {
			req := &proto.SetXAttrRequest{
				Inode: ino,
				Key: fmt.Sprintf("test_%v", index),
				Value: fmt.Sprintf("test_%v_%v", index, ino),
			}
			keys = append(keys, req.Key)
			packet := &Packet{}
			if err = leader.SetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("set xattr error[%v] or resultCode mismatch, resultCode expect:OpOk(0xF0), actual:0x%X", err, packet.ResultCode)
				return
			}
		}
	}

	for _, ino := range inos {
		for _, key := range keys {
			req := &proto.RemoveXAttrRequest{
				Inode: ino,
				Key: key,
			}
			packet := &Packet{}
			if err = leader.RemoveXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("remove xattr error[%v] or resultCode mismatch, resultCode expect:OpOk(0xF0), actual:0x%X", err, packet.ResultCode)
				return
			}
		}
	}
	//validate
	for _, ino := range inos {
		for index := 0; index < 5; index++ {
			req := &proto.GetXAttrRequest{
				Inode: ino,
				Key: fmt.Sprintf("test_%v", index),
			}

			//get xattr from leader
			packet := &Packet{}
			if err = leader.GetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("get xattr failed, req:%v, err:%v", req, err)
				return
			}
			respInLeader := &proto.GetXAttrResponse{}
			if err = packet.UnmarshalData(respInLeader); err != nil {
				t.Errorf("unmarshal get xattr response failed, req:%v, err:%v", req, err)
				return
			}
			if respInLeader.Value != "" {
				t.Fatalf("valume mismatch, value expect:null string, actual:%s", respInLeader.Value)
			}

			//get xattr from follower
			packet = &Packet{}
			if err = follower.GetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("get xattr failed, req:%v, err:%v", req, err)
				return
			}
			respInFollower := &proto.GetXAttrResponse{}
			if err = packet.UnmarshalData(respInFollower); err != nil {
				t.Errorf("unmarshal get xattr response failed, req:%v, err:%v", req, err)
				return
			}
			if respInLeader.Value != "" {
				t.Fatalf("valume mismatch, value expect:null string, actual:%s", respInLeader.Value)
			}
		}
	}
}

func TestMetaPartition_RemoveXAttrCase01(t *testing.T) {
	//leader is mem mode
	dir := "remove_xattr_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	RemoveXAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	RemoveXAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func ListXAttrInterTest(t *testing.T, leader, follower *metaPartition) {
	//create inodes
	inos, err := createInodesForTest(leader, follower, 10, 470, 0, 0)
	if err != nil {
		t.Error(err)
		return
	}
	//set xattr
	keys := make([]string, 0)
	for _, ino := range inos {
		for index := 0; index < 5; index++ {
			req := &proto.SetXAttrRequest{
				Inode: ino,
				Key: fmt.Sprintf("test_%v", index),
				Value: fmt.Sprintf("test_%v_%v", index, ino),
			}
			keys = append(keys, req.Key)
			packet := &Packet{}
			if err = leader.SetXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
				t.Errorf("set xattr error[%v] or resultCode mismatch, resultCode expect:OpOk(0xF0), actual:0x%X", err, packet.ResultCode)
				return
			}
		}
	}
	for _, ino := range inos {
		req := &proto.ListXAttrRequest{
			Inode: ino,
		}
		packet := &Packet{}
		if err = leader.ListXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			t.Errorf("list xattr error[%v] or resultCode mismatch, resultCode expect:OpOk(0xF0), actual:0x%X",
				err, packet.ResultCode)
			return
		}

		respFromLeader := &proto.ListXAttrResponse{}
		if err = packet.UnmarshalData(respFromLeader); err != nil {
			t.Errorf("unmarshal list xattr response failed:%v", err)
			return
		}

		packet = &Packet{}
		if err = follower.ListXAttr(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			t.Errorf("list xattr error[%v] or resultCode mismatch, resultCode expect:OpOk(0xF0), actual:0x%X",
				err, packet.ResultCode)
			return
		}

		respFromFollower := &proto.ListXAttrResponse{}
		if err = packet.UnmarshalData(respFromFollower); err != nil {
			t.Errorf("unmarshal list xattr response failed:%v", err)
			return
		}

		if len(respFromLeader.XAttrs) != 5 {
			t.Errorf("xattr count mismatch, expect:5, actual:%v", len(respFromLeader.XAttrs))
			return
		}
		for _, xattr := range respFromLeader.XAttrs {
			indexStr := strings.Split(xattr, "_")[1]
			index, _ := strconv.Atoi(indexStr)
			if index > 5 {
				t.Fatalf("response mismatch")
			}
		}
	}
	return
}

func TestMetaPartition_ListXAttrCase01(t *testing.T) {
	//leader is mem mode
	dir := "list_xattr_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ListXAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)


	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ListXAttrInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}