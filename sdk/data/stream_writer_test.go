package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/unit"
	"golang.org/x/net/context"
)

func init() {
	log.InitLog(".", "test", log.DebugLevel, nil)
}

type HTTPReply struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func handleAdminGetIP(w http.ResponseWriter, r *http.Request) {
	cInfo := &proto.ClusterInfo{
		Cluster: "test",
		Ip:      "127.0.0.1",
	}
	data, _ := json.Marshal(cInfo)

	reply := &HTTPReply{
		Code: 0,
		Msg:  "Success",
		Data: data,
	}

	httpReply, _ := json.Marshal(reply)

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(httpReply)))
	w.Write(httpReply)
}

func handleAdminGetVol(w http.ResponseWriter, r *http.Request) {
	volView := &proto.SimpleVolView{
		Name: "test",
	}
	data, _ := json.Marshal(volView)

	reply := &HTTPReply{
		Code: 0,
		Msg:  "Success",
		Data: data,
	}

	httpReply, _ := json.Marshal(reply)

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(httpReply)))
	w.Write(httpReply)
}

func handleClientDataPartitions(w http.ResponseWriter, r *http.Request) {
	dv := &proto.DataPartitionsView{
		DataPartitions: []*proto.DataPartitionResponse{
			{
				PartitionID: 2,
				Hosts:       []string{"127.0.0.1:9999", "127.0.0.1:9999", "127.0.0.1:9999"},
				ReplicaNum:  3,
				LeaderAddr:  "127.0.0.1:9999",
			},
			{
				PartitionID: 3,
				Hosts:       []string{"127.0.0.1:8888", "127.0.0.1:8888", "127.0.0.1:8888"},
				ReplicaNum:  3,
				LeaderAddr:  "127.0.0.1:8888",
			},
		},
	}
	data, _ := json.Marshal(dv)

	reply := &HTTPReply{
		Code: 0,
		Msg:  "Success",
		Data: data,
	}

	httpReply, _ := json.Marshal(reply)

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(httpReply)))
	w.Write(httpReply)
}
func handleAdminGetCluster(w http.ResponseWriter, r *http.Request) {
	cv := &proto.ClusterView{}
	data, _ := json.Marshal(cv)

	reply := &HTTPReply{
		Code: 0,
		Msg:  "Success",
		Data: data,
	}

	httpReply, _ := json.Marshal(reply)

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(httpReply)))
	w.Write(httpReply)
}

func TestStreamer_UsePreExtentHandler(t *testing.T) {
	type fields struct {
		client     *ExtentClient
		inode      uint64
		status     int32
		refcnt     int
		idle       int
		traversed  int
		extents    *ExtentCache
		once       sync.Once
		handler    *ExtentHandler
		dirtylist  *DirtyExtentList
		dirty      bool
		request    chan interface{}
		done       chan struct{}
		tinySize   int
		extentSize int
		writeLock  sync.Mutex
	}
	type args struct {
		offset uint64
		size   int
	}

	var err error
	http.HandleFunc(proto.AdminGetIP, handleAdminGetIP)
	http.HandleFunc(proto.AdminGetVol, handleAdminGetVol)
	http.HandleFunc(proto.ClientDataPartitions, handleClientDataPartitions)
	http.HandleFunc(proto.AdminGetCluster, handleAdminGetCluster)

	go func() {
		if err = http.ListenAndServe(":9999", nil); err != nil {
			t.Errorf("Start pprof err(%v)", err)
			t.FailNow()
		}
	}()

	for {
		conn, err := net.Dial("tcp", "127.0.0.1:9999")
		if err == nil {
			conn.Close()
			break
		}
	}

	testClient := new(ExtentClient)
	if testClient.dataWrapper, err = NewDataPartitionWrapper("test", []string{"127.0.0.1:9999"}); err != nil {
		t.Errorf("prepare test falied, err(%v)", err)
		t.FailNow()
	}

	ek1 := proto.ExtentKey{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 1024}
	ek2 := proto.ExtentKey{FileOffset: 2048, PartitionId: 2, ExtentId: 1002, ExtentOffset: 0, Size: 1024}
	ek3 := proto.ExtentKey{FileOffset: 5120, PartitionId: 3, ExtentId: 1003, ExtentOffset: 0, Size: 1024}
	ek4 := proto.ExtentKey{FileOffset: 7168, PartitionId: 4, ExtentId: 1004, ExtentOffset: 0, Size: 1024}
	ek5 := proto.ExtentKey{FileOffset: 10240, PartitionId: 5, ExtentId: 1005, ExtentOffset: 0, Size: 1024 * 1024 * 128}

	testExtentCache := NewExtentCache(1)
	testExtentCache.root.Insert(nil, ek1, testExtentCache.inode)
	testExtentCache.root.Insert(nil, ek2, testExtentCache.inode)
	testExtentCache.root.Insert(nil, ek3, testExtentCache.inode)
	testExtentCache.root.Insert(nil, ek4, testExtentCache.inode)
	testExtentCache.root.Insert(nil, ek5, testExtentCache.inode)

	testFields := fields{
		client:     testClient,
		extents:    testExtentCache,
		dirtylist:  NewDirtyExtentList(),
		extentSize: unit.ExtentSize,
	}

	testFieldsWithNilExtents := testFields
	testFieldsWithNilExtents.extents = NewExtentCache(1)

	testFieldsWithDirtyList := testFields
	testFieldsWithDirtyList.dirtylist = NewDirtyExtentList()
	testFieldsWithDirtyList.dirtylist.Put(&ExtentHandler{})

	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "success",
			fields: testFields,
			args:   args{offset: 3072, size: 1024},
			want:   true,
		},
		{
			name:   "preEk == nil",
			fields: testFieldsWithNilExtents,
			args:   args{offset: 3072, size: 1024},
			want:   false,
		},
		{
			name:   "s.dirtylist.Len() != 0",
			fields: testFieldsWithDirtyList,
			args:   args{offset: 3072, size: 1024},
			want:   false,
		},
		{
			name:   "IsTinyExtent(preEk.ExtentId)",
			fields: testFields,
			args:   args{offset: 1024, size: 1024},
			want:   false,
		},
		{
			name:   "preEk.Size >= unit.ExtentSize",
			fields: testFields,
			args:   args{offset: 10240 + 1024*1024*128, size: 1024},
			want:   false,
		},
		{
			name:   "reEk.FileOffset+uint64(preEk.Size) != uint64(offset)",
			fields: testFields,
			args:   args{offset: 4096, size: 1024},
			want:   false,
		},
		{
			name:   "int(preEk.Size)+size > unit.ExtentSize",
			fields: testFields,
			args:   args{offset: 3072, size: 1024 * 1024 * 128},
			want:   false,
		},
		{
			name:   "GetDataPartition failed",
			fields: testFields,
			args:   args{offset: 8192, size: 1024},
			want:   false,
		},
		{
			name:   "GetConnect(dp.Hosts[0]) failed",
			fields: testFields,
			args:   args{offset: 6144, size: 1024},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Streamer{
				client:     tt.fields.client,
				inode:      tt.fields.inode,
				status:     tt.fields.status,
				refcnt:     tt.fields.refcnt,
				idle:       tt.fields.idle,
				traversed:  tt.fields.traversed,
				extents:    tt.fields.extents,
				once:       tt.fields.once,
				handler:    tt.fields.handler,
				dirtylist:  tt.fields.dirtylist,
				dirty:      tt.fields.dirty,
				request:    tt.fields.request,
				done:       tt.fields.done,
				tinySize:   tt.fields.tinySize,
				extentSize: tt.fields.extentSize,
				writeLock:  tt.fields.writeLock,
			}
			if got := s.usePreExtentHandler(tt.args.offset, tt.args.size); got != tt.want {
				t.Errorf("usePreExtentHandler() = %v, want %v, name %v", got, tt.want, tt.name)
			}
		})
	}
}

func creatHelper(t *testing.T) (mw *meta.MetaWrapper, ec *ExtentClient, err error) {
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        ltptestVolume,
		Masters:       strings.Split(ltptestMaster, ","),
		ValidateOwner: true,
		Owner:         ltptestVolume,
	}); err != nil {
		t.Fatalf("NewMetaWrapper failed: err(%v) vol(%v)", err, ltptestVolume)
	}
	if ec, err = NewExtentClient(&ExtentConfig{
		Volume:            ltptestVolume,
		Masters:           strings.Split(ltptestMaster, ","),
		FollowerRead:      false,
		OnInsertExtentKey: mw.InsertExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		TinySize:          NoUseTinyExtent,
	}); err != nil {
		t.Fatalf("NewExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
	}
	return mw, ec, nil
}

func getStreamer(t *testing.T, file string, ec *ExtentClient, appendWriteBuffer bool, readAhead bool) *Streamer {
	info, err := os.Stat(file)
	if err != nil {
		t.Fatalf("Stat failed: err(%v) file(%v)", err, file)
	}
	sysStat := info.Sys().(*syscall.Stat_t)
	streamMap := ec.streamerConcurrentMap.GetMapSegment(sysStat.Ino)
	return NewStreamer(ec, sysStat.Ino, streamMap, false, false)
}

func TestROW(t *testing.T) {
	var (
		testROWFilePath = "/cfs/mnt/testROW.txt"
		originData      = "Origin test ROW file"
		writeData       = "ROW is Writing......"
		mw              *meta.MetaWrapper
		ec              *ExtentClient
		err             error
	)
	ctx := context.Background()
	mw, ec, err = creatHelper(t)
	if err != nil {
		t.Fatalf("create help metaWrapper and extentClient failed: err(%v), metaWrapper(%v), extentclient(%v)",
			err, mw, ec)
	}
	ROWFile, err := os.Create(testROWFilePath)
	if err != nil {
		t.Fatalf("create ROW testFile failed: err(%v), file(%v)", err, testROWFilePath)
	}
	defer func() {
		ROWFile.Close()
		os.Remove(testROWFilePath)
		log.LogFlush()
	}()
	writeBytes := []byte(originData)
	writeOffset := int64(0)
	_, err = ROWFile.WriteAt(writeBytes, writeOffset)
	if err != nil {
		t.Fatalf("write ROW testFile failed: err(%v), file(%v)", err, testROWFilePath)
	}
	ROWFile.Sync()
	beforeRow, _ := ioutil.ReadFile(testROWFilePath)
	fmt.Printf("before ROW: %v\n", string(beforeRow))
	var fInfo os.FileInfo
	if fInfo, err = os.Stat(testROWFilePath); err != nil {
		t.Fatalf("stat ROW testFile failed: err(%v), file(%v)", err, testROWFilePath)
	}
	inode := fInfo.Sys().(*syscall.Stat_t).Ino
	streamMap := ec.streamerConcurrentMap.GetMapSegment(inode)
	streamer := NewStreamer(ec, inode, streamMap, false, false)
	_, _, eks, err := mw.GetExtents(ctx, inode)
	if err != nil {
		t.Fatalf("GetExtents filed: err(%v) inode(%v)", err, inode)
	}
	for _, ek := range eks {
		req := &ExtentRequest{
			FileOffset: ek.FileOffset,
			Size:       int(ek.Size),
			Data:       []byte(writeData),
			ExtentKey:  &ek,
		}
		_, err = streamer.doROW(ctx, req, false)
		if err != nil {
			t.Fatalf("doROW failed: err(%v), req(%v)", err, req)
		}
	}
	//ROWFile, _ = os.Open(testROWFilePath)
	//readBytes := make([]byte, len(writeBytes))
	//readOffset := int64(0)
	//_, err = ROWFile.ReadAt(readBytes, readOffset)
	readBytes, err := ioutil.ReadFile(testROWFilePath)
	if err != nil {
		t.Errorf("read ROW testFile failed: err(%v)", err)
	}
	if string(readBytes) != writeData {
		t.Fatalf("ROW is failed: err(%v), read data(%v)", err, string(readBytes))
	}
	fmt.Printf("after ROW : %v\n", string(readBytes))
	streamer.done <- struct{}{}
	if err = ec.Close(context.Background()); err != nil {
		t.Errorf("close ExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
	}
}

func TestWrite_DataConsistency(t *testing.T) {
	var (
		testFile = "/cfs/mnt/write.txt"
		fInfo    os.FileInfo
		dp       *DataPartition
		ek       proto.ExtentKey
		err      error
	)
	file, err := os.Create(testFile)
	if err != nil {
		t.Fatalf("create testFile failed: err(%v), file(%v)", err, testFile)
	}
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	// append write
	var fileOffset uint64 = 0
	for i := 0; i < 3; i++ {
		n, _ := file.WriteAt([]byte(" aaaa aaaa"), int64(fileOffset))
		fileOffset += uint64(n)
	}
	// append write at 30~50
	_, err = file.WriteAt([]byte(" aaaa aaaa aaaa aaaa"), int64(fileOffset))
	if err != nil {
		t.Fatalf("first append write failed: err(%v)", err)
	}
	file.Sync()
	//overwrite
	_, err = file.WriteAt([]byte("overwrite is writing"), int64(fileOffset))
	if err != nil {
		t.Fatalf("overwrite failed: err(%v)", err)
	}
	file.Sync()
	//truncate
	if err = file.Truncate(int64(fileOffset)); err != nil {
		t.Fatalf("truncate file failed: err(%v)", err)
	}
	file.Sync()
	//append write again
	size, err := file.WriteAt([]byte("lastTime appendWrite"), int64(fileOffset))
	if err != nil {
		t.Fatalf("last append write failed: err(%v)", err)
	}
	file.Sync()

	mw, ec, err := creatHelper(t)

	if fInfo, err = os.Stat(testFile); err != nil {
		t.Fatalf("stat file: err(%v) file(%v)", err, testFile)
	}
	sysStat := fInfo.Sys().(*syscall.Stat_t)
	streamMap := ec.streamerConcurrentMap.GetMapSegment(sysStat.Ino)
	streamer := NewStreamer(ec, sysStat.Ino, streamMap, false, false)
	if _, _, eks, err := mw.GetExtents(context.Background(), sysStat.Ino); err != nil {
		t.Fatalf("GetExtents filed: err(%v) inode(%v)", err, sysStat.Ino)
	} else {
		for _, ek = range eks {
			if ek.FileOffset == uint64(fileOffset) {
				break
			}
		}
	}
	fmt.Printf("------ek's FileOffset(%v)\n", ek.FileOffset)
	if dp, err = streamer.client.dataWrapper.GetDataPartition(ek.PartitionId); err != nil {
		t.Fatalf("GetDataPartition err(%v), pid(%v)", err, ek.PartitionId)
	}
	sc := NewStreamConn(dp, false)
	host := sortByStatus(sc.dp, sc.dp.Hosts[len(sc.dp.Hosts)-2])
	if host[len(host)-1] != sc.dp.Hosts[len(sc.dp.Hosts)-2] {
		t.Fatalf("TestWrite_DataConsistency failed: expect host(%v) at the end but hosts(%v)", sc.dp.Hosts[len(sc.dp.Hosts)-2], host)
	}
	data := make([]byte, size)
	req := NewExtentRequest(fileOffset, size, data, &ek)
	reqPacket := NewReadPacket(context.Background(), &ek, int(ek.ExtentOffset), req.Size, streamer.inode, req.FileOffset, true)
	// read from three replicas, check if same
	readMap := make(map[string]string)
	for _, addr := range host {
		fmt.Printf("read from (%v), reqPacket(%v)\n", addr, reqPacket)
		sc.currAddr = addr
		_, _, _, readErr := dp.sendReadCmdToDataPartition(sc, reqPacket, req)
		if readErr == nil {
			readMap[addr] = string(req.Data)
		} else {
			readMap[addr] = readErr.Error()
		}
		want := "lastTime appendWrite"
		if readMap[addr] != want {
			t.Errorf("Inconsistent data: readAddr(%v), readWords(%v), want(%v)\n", addr, readMap[addr], want)
		}
	}

	streamer.done <- struct{}{}
	if err = ec.Close(context.Background()); err != nil {
		t.Errorf("close ExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
	}
}

// One client insert ek1 at some position, another client insert ek2 at the same position with ROW.
// Then ek1 will be replaced by ek2, all following ek insertion of extent1 because of usePreExtentHandler should be rejected.
func TestStreamer_UsePreExtentHandler_ROWByOtherClient(t *testing.T) {
	testFile := "/cfs/mnt/TestStreamer_UsePreExtentHandler_ROWByOtherClient"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	_, ec, err := creatHelper(t)
	streamer := getStreamer(t, testFile, ec, false, false)
	ctx := context.Background()
	length := 1024
	data := make([]byte, length)
	_, _, err = streamer.write(ctx, data, 0, length, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	err = streamer.flush(ctx, true)
	if err != nil {
		t.Fatalf("flush failed: err(%v)", err)
	}

	_, ec1, err := creatHelper(t)
	streamer1 := getStreamer(t, testFile, ec1, false, false)
	requests, _ := streamer1.extents.PrepareRequests(0, length, data)
	_, err = streamer1.doROW(ctx, requests[0], false)
	if err != nil {
		t.Fatalf("doROW failed: err(%v)", err)
	}

	_, _, err = streamer.write(ctx, data, uint64(length), length, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	err = streamer.flush(ctx, true)
	if err == nil {
		t.Fatalf("usePreExtentHandler should fail when the extent has removed by other clients")
	}
}

func TestHandler_Recover(t *testing.T) {
	testFile := "/cfs/mnt/TestHandler_Recover"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	var err error
	_, ec, _ := creatHelper(t)
	streamer := getStreamer(t, testFile, ec, false, false)
	ctx := context.Background()
	length := 1024
	data := make([]byte, length*2)
	_, _, err = streamer.write(ctx, data, 0, length, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	err = streamer.flush(ctx, true)
	if err != nil {
		t.Fatalf("flush failed: err(%v)", err)
	}
	suc := streamer.handler.setClosed()
	if !suc {
		t.Fatalf("setClosed failed")
	}
	suc = streamer.handler.setRecovery()
	if !suc {
		t.Fatalf("setRecovery failed")
	}
	streamer.handler.setDebug(true)

	_, _, err = streamer.write(ctx, data, uint64(length), length, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	err = streamer.GetExtents(ctx)
	if err != nil {
		t.Fatalf("GetExtents failed: err(%v)", err)
	}
	read, _, err := streamer.read(ctx, data, 0, length*2)
	if err != nil || read != length*2 {
		t.Fatalf("read failed: expect(%v) read(%v) err(%v)", length*2, read, err)
	}
}

func TestHandler_AppendWriteBuffer_Recover(t *testing.T) {
	testFile := "/cfs/mnt/TestHandler_AppendWriteBuffer_Recover"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	var err error
	_, ec, _ := creatHelper(t)
	streamer := getStreamer(t, testFile, ec, true, false)
	ctx := context.Background()
	length := 1024
	data := make([]byte, length)
	_, _, err = streamer.write(ctx, data, 0, length, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	suc := streamer.handler.setClosed()
	if !suc {
		t.Fatalf("setClosed failed")
	}
	suc = streamer.handler.setRecovery()
	if !suc {
		t.Fatalf("setRecovery failed")
	}
	err = streamer.flush(ctx, true)
	if err != nil {
		t.Fatalf("flush failed: err(%v)", err)
	}
}

// Handler should be closed in truncate operation, otherwise dirty ek which has been formerly truncated, will be inserted again.
func TestStreamer_Truncate_CloseHandler(t *testing.T) {
	testFile := "/cfs/mnt/TestStreamer_Truncate_CloseHandler"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	var err error
	_, ec, _ := creatHelper(t)
	streamer := getStreamer(t, testFile, ec, false, false)
	ctx := context.Background()
	length := 1024
	data := make([]byte, length*2)
	_, _, err = streamer.write(ctx, data, 0, length*2, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	err = streamer.truncate(ctx, uint64(length))
	if err != nil {
		t.Fatalf("truncate failed: err(%v)", err)
	}
	_, _, err = streamer.write(ctx, data, uint64(length)*2, length, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	requests, _ := streamer.extents.PrepareRequests(uint64(length), length, data)
	if requests[0].ExtentKey != nil {
		t.Fatalf("dirty ek after truncate")
	}
}

// Handler should be closed in ROW operation, otherwise dirty ek which has been formerly removed, will be inserted again.
func TestStreamer_ROW_CloseHandler(t *testing.T) {
	testFile := "/cfs/mnt/TestStreamer_ROW_CloseHandler"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	var err error
	_, ec, _ := creatHelper(t)
	streamer := getStreamer(t, testFile, ec, false, false)
	ctx := context.Background()
	length := 1024
	data := make([]byte, length*2)
	_, _, err = streamer.write(ctx, data, 0, length*2, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	requests, _ := streamer.extents.PrepareRequests(uint64(length), length*2, data)
	_, err = streamer.doROW(ctx, requests[0], false)
	if err != nil {
		t.Fatalf("doROW failed: err(%v)", err)
	}
	_, _, err = streamer.write(ctx, data, uint64(length)*2, length, false, false)
	if err != nil {
		t.Fatalf("write failed: err(%v)", err)
	}
	requests, _ = streamer.extents.PrepareRequests(0, length*2, data)
	if len(requests) != 2 || (requests[0].ExtentKey.PartitionId == requests[1].ExtentKey.PartitionId && requests[0].ExtentKey.ExtentId == requests[1].ExtentKey.ExtentId) {
		t.Fatalf("dirty ek after ROW")
	}
}
