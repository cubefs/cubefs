package data

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/util"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/chubaofs/chubaofs/proto"
)

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

func TestStreamer_usePreExtentHandler(t *testing.T) {
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
		offset int
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

	ek1 := &proto.ExtentKey{FileOffset: 0, PartitionId: 1, ExtentId: 1, ExtentOffset: 0, Size: 1024}
	ek2 := &proto.ExtentKey{FileOffset: 2048, PartitionId: 2, ExtentId: 1002, ExtentOffset: 0, Size: 1024}
	ek3 := &proto.ExtentKey{FileOffset: 5120, PartitionId: 3, ExtentId: 1003, ExtentOffset: 0, Size: 1024}
	ek4 := &proto.ExtentKey{FileOffset: 7168, PartitionId: 4, ExtentId: 1004, ExtentOffset: 0, Size: 1024}
	ek5 := &proto.ExtentKey{FileOffset: 10240, PartitionId: 5, ExtentId: 1005, ExtentOffset: 0, Size: 1024 * 1024 * 128}

	testExtentCache := NewExtentCache(1)
	testExtentCache.root.ReplaceOrInsert(ek1)
	testExtentCache.root.ReplaceOrInsert(ek2)
	testExtentCache.root.ReplaceOrInsert(ek3)
	testExtentCache.root.ReplaceOrInsert(ek4)
	testExtentCache.root.ReplaceOrInsert(ek5)

	testFields := fields{
		client:     testClient,
		extents:    testExtentCache,
		dirtylist:  NewDirtyExtentList(),
		extentSize: util.ExtentSize,
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
			name:   "preEk.Size >= util.ExtentSize",
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
			name:   "int(preEk.Size)+size > util.ExtentSize",
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
