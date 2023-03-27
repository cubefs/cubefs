package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/hashicorp/go-uuid"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func createTestMultiParts(leader, follower *metaPartition) (multiparts []*proto.MultipartInfo, err error) {
	multiparts = make([]*proto.MultipartInfo, 0, 1320)
	for index := 0; index < 330; index++ {
		req := &proto.CreateMultipartRequest{
			Path: fmt.Sprintf("test/%v", index),
		}
		packet := &Packet{}
		if err = leader.CreateMultipart(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			err = fmt.Errorf("create multipart[path:%s] failed, error[%v]", req.Path, err)
			return
		}
		resp := &proto.CreateMultipartResponse{}
		if err = packet.UnmarshalData(resp); err != nil {
			err = fmt.Errorf("unmarshal multipart[path:%s] response failed, error[%v]", req.Path, err)
			return
		}
		multiparts = append(multiparts, resp.Info)
	}

	for index := 0; index < 330; index++ {
		req := &proto.CreateMultipartRequest{
			Path: fmt.Sprintf("test/one/%v", index),
		}
		packet := &Packet{}
		if err = leader.CreateMultipart(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			err = fmt.Errorf("create multipart[path:%s] failed, error[%v]", req.Path, err)
			return
		}
		resp := &proto.CreateMultipartResponse{}
		if err = packet.UnmarshalData(resp); err != nil {
			err = fmt.Errorf("unmarshal multipart[path:%s] response failed, error[%v]", req.Path, err)
			return
		}
		multiparts = append(multiparts, resp.Info)
	}

	for index := 0; index < 330; index++ {
		req := &proto.CreateMultipartRequest{
			Path: fmt.Sprintf("test/two/%v", index),
		}
		packet := &Packet{}
		if err = leader.CreateMultipart(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			err = fmt.Errorf("create multipart[path:%s] failed, error[%v]", req.Path, err)
			return
		}
		resp := &proto.CreateMultipartResponse{}
		if err = packet.UnmarshalData(resp); err != nil {
			err = fmt.Errorf("unmarshal multipart[path:%s] response failed, error[%v]", req.Path, err)
			return
		}
		multiparts = append(multiparts, resp.Info)
	}

	for index := 0; index < 330; index++ {
		var path string
		path, err = uuid.GenerateUUID()
		if err != nil {
			continue
		}
		req := &proto.CreateMultipartRequest{
			Path: path,
		}
		packet := &Packet{}
		if err = leader.CreateMultipart(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			err = fmt.Errorf("create multipart[path:%s] failed, error[%v]", req.Path, err)
			return
		}
		resp := &proto.CreateMultipartResponse{}
		if err = packet.UnmarshalData(resp); err != nil {
			err = fmt.Errorf("unmarshal multipart[path:%s] response failed, error[%v]", req.Path, err)
			return
		}
		multiparts = append(multiparts, resp.Info)
	}

	//validate leader and follower info
	if leader.multipartTree.Count() != follower.multipartTree.Count() {
		err = fmt.Errorf("multipart tree count in leader and follower mp mismatch, leader:%v, actual:%v",
			leader.multipartTree.Count(), follower.multipartTree.Count())
		return
	}
	if err = follower.multipartTree.Range(nil, nil, func(multipart *Multipart) (bool, error) {
		m, _ := leader.multipartTree.Get(multipart.key, multipart.id)
		if m == nil {
			return false, fmt.Errorf("multipart[key:%s, id:%s] in leader and follower mismatch", multipart.key, multipart.id)
		}
		if m.key != multipart.key || m.id != multipart.id {
			return false, fmt.Errorf("multipart info mismatch, leader[key:%s, id:%s] follower[key:%s, id:%s]",
				m.key, m.id, multipart.key, multipart.id)
		}
		return true, nil
	}); err != nil {
		return
	}
	return
}

func createMultiPart(leader, follower *metaPartition, multipartPath string) (multipart *proto.MultipartInfo, err error) {
	req := &proto.CreateMultipartRequest{
		Path: multipartPath,
	}
	packet := &Packet{}
	if err = leader.CreateMultipart(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		err = fmt.Errorf("create multipart[path:%s] failed, error[%v]", req.Path, err)
		return
	}
	resp := &proto.CreateMultipartResponse{}
	if err = packet.UnmarshalData(resp); err != nil {
		err = fmt.Errorf("unmarshal multipart[path:%s] response failed, error[%v]", req.Path, err)
		return
	}
	multipart = resp.Info
	multipartFromLeader, _ := leader.multipartTree.Get(multipart.Path, multipart.ID)
	multipartFromFollower, _ := follower.multipartTree.Get(multipart.Path, multipart.ID)
	if multipartFromLeader == nil || multipartFromFollower == nil {
		err = fmt.Errorf("multipart info mismatch, leader:%v, follower:%v", multipartFromLeader, multipartFromFollower)
		return
	}
	if !reflect.DeepEqual(multipartFromLeader, multipartFromFollower) {
		err = fmt.Errorf("multipart info mismatch")
		return
	}
	if multipartFromLeader.key !=  multipartPath {
		err = fmt.Errorf("multipart key mismatch, expect:%s, actual:%s", multipartPath, multipartFromLeader.key)
	}
	return
}

func CreateMultipartInterTest01(t *testing.T, leader, follower *metaPartition) {
	multiparts, err := createTestMultiParts(leader, follower)
	if err != nil {
		t.Errorf("create multiparts failed:%v", err)
	}

	for _, multipart := range multiparts {
		req := &proto.GetMultipartRequest{
			Path: multipart.Path,
			MultipartId: multipart.ID,
		}
		packet := &Packet{}
		if err = leader.GetMultipart(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			t.Errorf("get multipart [path:%s, multipartId:%s] error[%v] or resultCode mismatch, expect:OpOk(0xF0), actual:0x%X",
				multipart.Path, multipart.ID, err, packet.ResultCode)
			return
		}

		resp := &proto.GetMultipartResponse{}
		if err = packet.UnmarshalData(resp); err != nil {
			t.Errorf("unmarshall GetMultipart response failed, multipart[path:%s, multipartId:%s], error[%v]",
				multipart.Path, multipart.ID, err)
			return
		}
		if resp.Info.Path != multipart.Path || resp.Info.ID != multipart.ID {
			t.Fatalf("multipart info mismatch, except[path:%s, id:%s], actual[path:%s, id:%s]",
				multipart.Path, multipart.ID, resp.Info.Path, resp.Info.ID)
			return
		}

		packet = &Packet{}
		if err = follower.GetMultipart(req, packet); err != nil || packet.ResultCode != proto.OpOk {
			t.Errorf("get multipart [path:%s, multipartId:%s] error[%v] or resultCode mismatch, expect:OpOk(0xF0), actual:0x%X",
				multipart.Path, multipart.ID, err, packet.ResultCode)
		}

		resp = &proto.GetMultipartResponse{}
		if err = packet.UnmarshalData(resp); err != nil {
			t.Errorf("unmarshall GetMultipart response failed, multipart[path:%s, multipartId:%s], error[%v]",
				multipart.Path, multipart.ID, err)
			return
		}
		if resp.Info.Path != multipart.Path || resp.Info.ID != multipart.ID {
			t.Fatalf("multipart info mismatch, except[path:%s, id:%s], actual[path:%s, id:%s]",
				multipart.Path, multipart.ID, resp.Info.Path, resp.Info.ID)
			return
		}
	}
}

func TestMetaPartition_CreateAndGetMultipartCase01(t *testing.T) {
	//leader is mem mode
	dir := "create_and_get_multipart_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	CreateMultipartInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	CreateMultipartInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func AppendMultipartInterTest01(t *testing.T, leader, follower *metaPartition) {
	//create multipart
	multipart, err := createMultiPart(leader, follower, "test/appendMultipart01")
	if err != nil {
		t.Fatal(err)
	}

	part := &proto.MultipartPartInfo{
		ID:         0,
		Inode:      1000,
		MD5:        "f7434eebf2339e9ee4af61fa67a77b78",
		Size:       128,
		UploadTime: time.Now(),
	}
	reqAppendPart := &proto.AddMultipartPartRequest{
		MultipartId: multipart.ID,
		Path: multipart.Path,
		Part: part,
	}
	packet := &Packet{}
	if err = leader.AppendMultipart(reqAppendPart, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("append part error[%v] or resultCode mismatch, expect:OpOk(0xF0), actual:0x%X", err, packet.ResultCode)
		return
	}

	//validate
	//leader
	m, err := leader.multipartTree.Get(multipart.Path, multipart.ID)
	if m == nil {
		t.Fatalf("multipart is null, mismatch")
	}

	if m.key != multipart.Path || m.id != multipart.ID {
		t.Fatalf("multipart info mismatch, except[path:%s, id:%s], actual[path:%s, id:%s]", multipart.Path, multipart.ID, m.key, m.id)
	}

	if len(m.parts) != 1 {
		t.Fatalf("part number mismatch, expect:1, actual:%v", len(m.parts))
	}

	if m.parts[0].ID != part.ID || m.parts[0].Inode != part.Inode || m.parts[0].Size != part.Size || m.parts[0].MD5 != part.MD5 {
		t.Fatalf("part info mismatch")
	}

	//follower
	m, err = follower.multipartTree.Get(multipart.Path, multipart.ID)
	if m == nil {
		t.Fatalf("multipart is null, mismatch")
	}

	if m.key != multipart.Path || m.id != multipart.ID {
		t.Fatalf("multipart info mismatch, except[path:%s, id:%s], actual[path:%s, id:%s]", multipart.Path, multipart.ID, m.key, m.id)
	}

	if len(m.parts) != 1 {
		t.Fatalf("part number mismatch, expect:1, actual:%v", len(m.parts))
	}

	if m.parts[0].ID != part.ID || m.parts[0].Inode != part.Inode || m.parts[0].Size != part.Size || m.parts[0].MD5 != part.MD5 {
		t.Fatalf("part info mismatch")
	}
}

func AppendMultipartInterTest02(t *testing.T, leader, follower *metaPartition) {
	multipart, err := createMultiPart(leader, follower, "test/appendMultipart02")
	if err != nil {
		t.Fatal(err)
	}

	part := &proto.MultipartPartInfo{
		ID:         0,
		Inode:      1000,
		MD5:        "f7434eebf2339e9ee4af61fa67a77b78",
		Size:       128,
		UploadTime: time.Now(),
	}
	reqAppendPart := &proto.AddMultipartPartRequest{
		MultipartId: multipart.ID,
		Path:        multipart.Path,
		Part:        part,
	}
	packet := &Packet{}
	if err = leader.AppendMultipart(reqAppendPart, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("append part error[%v] or resultCode mismatch, expect:OpOk(0xF0), actual:0x%X", err, packet.ResultCode)
		return
	}

	part = &proto.MultipartPartInfo{
		ID:         0,
		Inode:      1001,
		MD5:        "f7434eebf2339e9ee4af61fa67a77b78",
		Size:       128,
		UploadTime: time.Now(),
	}
	reqAppendPart = &proto.AddMultipartPartRequest{
		MultipartId: multipart.ID,
		Path:        multipart.Path,
		Part:        part,
	}
	packet = &Packet{}
	if err = leader.AppendMultipart(reqAppendPart, packet); err != nil || packet.ResultCode != proto.OpExistErr{
		t.Fatalf("append  repeat part error[%v] or resultCode mismatch,  resultCode expect:OpExistErr(0xFA), actual:0x%X", err, packet.ResultCode)
	}
	return
}

func AppendMultipartInterTest03(t *testing.T, leader, follower *metaPartition) {
	part := &proto.MultipartPartInfo{
		ID:         0,
		Inode:      1000,
		MD5:        "f7434eebf2339e9ee4af61fa67a77b78",
		Size:       128,
		UploadTime: time.Now(),
	}
	reqAppendPart := &proto.AddMultipartPartRequest{
		MultipartId: "10952628-b05d-4628-82f9-00eb20d85647",
		Path: "test/appendMultipart03",
		Part: part,
	}
	packet := &Packet{}
	if _ = leader.AppendMultipart(reqAppendPart, packet); packet.ResultCode != proto.OpNotExistErr {
		t.Errorf("append part to not exist multipart resultCode mismatch, expect:OpNotExistErr(0xF5), actual:0x%X", packet.ResultCode)
		return
	}
	return
}

func TestMetaPartition_AppendMultipartCase01(t *testing.T) {
	//leader is mem mode
	dir := "append_multipart_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	AppendMultipartInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	AppendMultipartInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_AppendMultipartCase02(t *testing.T) {
	//leader is mem mode
	dir := "append_multipart_test_02"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	AppendMultipartInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	AppendMultipartInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_AppendMultipartCase03(t *testing.T) {
	//leader is mem mode
	dir := "append_multipart_test_03"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	AppendMultipartInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	AppendMultipartInterTest03(t, leader, follower)
	releaseMp(leader, follower, dir)
}

//list multipart with prefix
func ListMultipartInterTest01(t *testing.T, leader, follower *metaPartition) {
	multiparts, err := createTestMultiParts(leader, follower)
	if err != nil {
		t.Fatal(err)
		return
	}

	expectResult := make([]*proto.MultipartInfo, 0, 330)
	for _, multipart := range multiparts {
		if strings.HasPrefix(multipart.Path, "test/one") {
			expectResult = append(expectResult, multipart)
		}
	}
	sort.Slice(expectResult, func(i, j int) bool {
		return expectResult[i].Path < expectResult[j].Path ||
			(expectResult[i].Path == expectResult[j].Path && expectResult[i].ID < expectResult[j].ID)
	})

	maxCount := 100
	req := &proto.ListMultipartRequest{
		Max: uint64(maxCount + 1),
		Prefix: "test/one",
		Marker: "",
		MultipartIdMarker: "",
	}

	index := 0
	resultCount := 0
	for {
		var listMultipartResult []*proto.MultipartInfo
		if listMultipartResult, err = listMultipart(leader, follower, req); err != nil {
			t.Fatalf("list multipart error:%v", err)
			return
		}

		if len(listMultipartResult) > maxCount + 1 {
			t.Errorf("error count, multipart count greater than %v", req.Max)
			return
		}

		if len(listMultipartResult) <= maxCount {
			resultCount += len(listMultipartResult)
			for _, multipart := range listMultipartResult {
				if multipart.Path != expectResult[index].Path || multipart.ID != expectResult[index].ID {
					t.Fatalf("multipart info mismatch, expect[path:%s, id:%s], actual[path:%s, id:%s]",
						expectResult[index].Path, expectResult[index].ID, multipart.Path, multipart.ID)
				}
				index++
			}
			//end break
			break
		}

		resultCount += maxCount
		nextKeyMarker := listMultipartResult[maxCount].Path
		nextIDMarker := listMultipartResult[maxCount].ID
		req = &proto.ListMultipartRequest{
			Max: uint64(maxCount + 1),
			Prefix: "test/one",
			Marker: nextKeyMarker,
			MultipartIdMarker: nextIDMarker,
		}

		listMultipartResult = listMultipartResult[:maxCount]
		for _, multipart := range listMultipartResult {
			if multipart.Path != expectResult[index].Path || multipart.ID != expectResult[index].ID {
				t.Fatalf("multipart info mismatch, expect[path:%s, id:%s], actual[path:%s, id:%s]",
					expectResult[index].Path, expectResult[index].ID, multipart.Path, multipart.ID)
			}
			index++
		}
	}

	if len(expectResult) != resultCount {
		t.Errorf("error multipart count, [expect:%v, atual:%v]", len(expectResult), resultCount)
		return
	}
}

//list all multipart
func ListMultipartInterTest02(t *testing.T, leader, follower *metaPartition) {
	multiparts, err := createTestMultiParts(leader, follower)
	if err != nil {
		t.Fatal(err)
		return
	}

	expectResult := multiparts
	sort.Slice(expectResult, func(i, j int) bool {
		return expectResult[i].Path < expectResult[j].Path ||
			(expectResult[i].Path == expectResult[j].Path && expectResult[i].ID < expectResult[j].ID)
	})

	maxCount := 100
	req := &proto.ListMultipartRequest{
		Max: uint64(maxCount + 1),
		Prefix: "",
		Marker: "",
		MultipartIdMarker: "",
	}

	index := 0
	resultCount := 0
	for {
		var listMultipartResult []*proto.MultipartInfo
		if listMultipartResult, err = listMultipart(leader, follower, req); err != nil {
			t.Fatalf("list multipart error:%v", err)
			return
		}

		if len(listMultipartResult) > maxCount + 1 {
			t.Errorf("error count, multipart count greater than %v", req.Max)
			return
		}

		if len(listMultipartResult) <= maxCount {
			resultCount += len(listMultipartResult)
			for _, multipart := range listMultipartResult {
				if multipart.Path != expectResult[index].Path || multipart.ID != expectResult[index].ID {
					t.Fatalf("multipart info mismatch, expect[path:%s, id:%s], actual[path:%s, id:%s]",
						expectResult[index].Path, expectResult[index].ID, multipart.Path, multipart.ID)
				}
				index++
			}
			//end break
			break
		}

		resultCount += maxCount
		nextKeyMarker := listMultipartResult[maxCount].Path
		nextIDMarker := listMultipartResult[maxCount].ID
		req = &proto.ListMultipartRequest{
			Max: uint64(maxCount + 1),
			Prefix: "",
			Marker: nextKeyMarker,
			MultipartIdMarker: nextIDMarker,
		}

		listMultipartResult = listMultipartResult[:maxCount]
		for _, multipart := range listMultipartResult {
			if multipart.Path != expectResult[index].Path || multipart.ID != expectResult[index].ID {
				t.Fatalf("multipart info mismatch, expect[path:%s, id:%s], actual[path:%s, id:%s]",
					expectResult[index].Path, expectResult[index].ID, multipart.Path, multipart.ID)
			}
			index++
		}
	}

	if len(expectResult) != resultCount {
		t.Errorf("error multipart count, [expect:%v, atual:%v]", len(expectResult), resultCount)
		return
	}
}

func listMultipart(leader, follower *metaPartition, req *proto.ListMultipartRequest) (result []*proto.MultipartInfo, err error) {
	packetForLeader := &Packet{}
	if err = leader.ListMultipart(req, packetForLeader); err != nil || packetForLeader.ResultCode != proto.OpOk {
		err = fmt.Errorf("list multipart error[%v] or resultCode mismatch, expect:OpOk(0xF0), actual:0x%X", err, packetForLeader.ResultCode)
		return
	}

	packetForFollower := &Packet{}
	if err = follower.ListMultipart(req, packetForFollower); err != nil || packetForFollower.ResultCode != proto.OpOk {
		err = fmt.Errorf("list multipart error[%v] or resultCode mismatch, expect:OpOk(0xF0), actual:0x%X", err, packetForFollower.ResultCode)
		return
	}

	respFromLeader := &proto.ListMultipartResponse{}
	if err = packetForLeader.UnmarshalData(respFromLeader); err != nil {
		err = fmt.Errorf("ummarshal listMultipart response failed, error[%v]", err)
		return
	}

	respFromFollower := &proto.ListMultipartResponse{}
	if err = packetForLeader.UnmarshalData(respFromFollower); err != nil {
		err = fmt.Errorf("ummarshal listMultipart response failed, error[%v]", err)
		return
	}

	if len(respFromLeader.Multiparts) != len(respFromFollower.Multiparts) {
		err = fmt.Errorf("multipart count mismatch, [leader:%v, follower:%v]", len(respFromLeader.Multiparts), len(respFromFollower.Multiparts))
		return
	}
	for index, multipart := range respFromLeader.Multiparts {
		if multipart.ID != respFromFollower.Multiparts[index].ID || multipart.Path != respFromFollower.Multiparts[index].Path {
			err = fmt.Errorf("different result, [mem:%v, rocks[%v]]", multipart, respFromFollower.Multiparts[index])
			return
		}
	}
	result = respFromLeader.Multiparts
	return
}

func TestMetaPartition_ListMultipartCase01(t *testing.T) {
	//leader is mem mode
	dir := "list_multipart_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ListMultipartInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ListMultipartInterTest01(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func TestMetaPartition_ListMultipartCase02(t *testing.T) {
	//leader is mem mode
	dir := "list_multipart_test_02"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	ListMultipartInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	ListMultipartInterTest02(t, leader, follower)
	releaseMp(leader, follower, dir)
}

func RemoverMultipartInterTest(t *testing.T, leader, follower *metaPartition) {
	multipart, err := createMultiPart(leader, follower, "test/removeMultipart")
	if err != nil {
		t.Fatal(err)
	}

	//remove
	req := &proto.RemoveMultipartRequest{
		Path: multipart.Path,
		MultipartId: multipart.ID,
	}
	packet := &Packet{}
	if err = leader.RemoveMultipart(req, packet); err != nil || packet.ResultCode != proto.OpOk {
		t.Errorf("remover multipart failed:%v", err)
		return
	}

	//get
	var m *Multipart
	m, err = leader.multipartTree.Get(multipart.Path, multipart.ID)
	if err == nil && m != nil {
		t.Fatalf("get result mismatch, multipart should be null")
	}
	m, err = follower.multipartTree.Get(multipart.Path, multipart.ID)
	if err == nil && m != nil {
		t.Fatalf("get result mismatch, multipart should be null")
	}
	return
}

func TestMetaPartition_RemoveMultipartCase01(t *testing.T) {
	//leader is mem mode
	dir := "remove_multipart_test_01"
	leader, follower := mockMp(t, dir, proto.StoreModeMem)
	RemoverMultipartInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)

	//leader is rocksdb mode
	leader, follower = mockMp(t, dir, proto.StoreModeRocksDb)
	RemoverMultipartInterTest(t, leader, follower)
	releaseMp(leader, follower, dir)
}