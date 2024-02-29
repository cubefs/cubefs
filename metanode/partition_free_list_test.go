package metanode

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/topology"
	"github.com/stretchr/testify/assert"
	"io"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const TestMasterHost = "127.0.0.1:19201"

var specialInodes = make(map[uint64]byte, 0)
var normalInodes = make(map[uint64]byte, 0)
var specialExtentsExpectCount = uint64(0)
var specialExtentsActualCount = uint64(0)
var mockMasterClient = master.NewMasterClient([]string{TestMasterHost}, false)

type MockDataNode struct {
	tcpListener net.Listener
}

func (s *MockDataNode) handleRequest(packet *proto.Packet, conn net.Conn) {
	switch packet.Opcode {
	case proto.OpBatchDeleteExtent:
		log.LogDebugf("receive batch mark delete request\n")
		var keys []*proto.InodeExtentKey
		if err := json.Unmarshal(packet.Data[0:packet.Size], &keys); err == nil {
			for _, key := range keys {
				if key.InodeId == 0 {
					atomic.AddUint64(&specialExtentsActualCount, 1)
				}
			}
		}
	case proto.OpMarkDelete:
		log.LogDebugf("receive mark delete request\n")
		ext := new(proto.InodeExtentKey)
		if unmarshalErr := json.Unmarshal(packet.Data, ext); unmarshalErr == nil {
		}
	default:
	}

	packet.ResultCode = proto.OpOk
	_ = packet.WriteToNoDeadLineConn(conn)
}

func (s *MockDataNode) serveConn(conn net.Conn) {
	defer conn.Close()
	c := conn.(*net.TCPConn)
	_ = c.SetKeepAlive(true) // Ignore error
	_ = c.SetNoDelay(true)   // Ignore error
	remoteAddr := conn.RemoteAddr().String()
	for {
		p := &proto.Packet{}
		if err := p.ReadFromConn(conn, 300); err != nil {
			if err != io.EOF {
				log.LogErrorf("conn (remote: %v) serve MetaNode: %v", remoteAddr, err.Error())
			}
			return
		}
		s.handleRequest(p, conn)
	}
}

func (s *MockDataNode) start() (err error) {
	var l net.Listener
	l, err = net.Listen("tcp", ":19320")
	if err != nil {
		err = fmt.Errorf("failed to listen, err: %v", err)
		return
	}
	s.tcpListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[startTCPService] failed to accept, err:%s", err.Error())
				time.Sleep(time.Second * 5)
				continue
			}
			log.LogDebugf("action[startTCPService] accept connection from %s.", conn.RemoteAddr().String())
			go s.serveConn(conn)
		}
	}(l)
	return
}

func mockDataNodeServer() (err error) {
	mockDataNode := &MockDataNode{}
	return mockDataNode.start()
}

func mockMaster() {
	fmt.Println("init mock master")
	profNetListener, err := net.Listen("tcp", TestMasterHost)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		_ = http.Serve(profNetListener, http.DefaultServeMux)
	}()
	go func() {
		http.HandleFunc(proto.ClientDataPartitions, fakeClientDataPartitions)
	}()
}


func send(w http.ResponseWriter, r *http.Request, reply []byte) {
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
		return
	}
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	return
}

func fakeClientDataPartitions(w http.ResponseWriter, r *http.Request) {
	var err error
	var reply = &proto.HTTPReply{
		Code: proto.ErrCodeSuccess,
		Msg:  "OK",
		Data: nil,
	}
	var dataPartitionsView = &proto.DataPartitionsView{}
	var respData []byte
	defer func() {
		if err != nil {
			reply.Code = proto.ErrCodeParamError
			reply.Msg = err.Error()
			reply.Data = nil
		} else {
			reply.Data = dataPartitionsView
		}
		respData, err = json.Marshal(reply)
		if err != nil {
			http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
			return
		}
		send(w, r, respData)
	}()

	if err = r.ParseForm(); err != nil {
		return
	}

	volName := r.FormValue("name")
	if volName == "" {
		err = fmt.Errorf("vol name is needed")
		return
	}

	ids := make([]uint64, 0)
	idsStr := r.FormValue("ids")
	if idsStr != "" {
		idStrArr := strings.Split(idsStr, ",")
		if len(idStrArr) == 0 {
			return
		}

		ids = make([]uint64, 0, len(idStrArr))
		for _, idStr := range idStrArr {
			var id uint64
			id, err = strconv.ParseUint(idStr, 10, 64)
			if err != nil {
				return
			}
			ids = append(ids, id)
		}
	}

	dataPartitionsView.DataPartitions = make([]*proto.DataPartitionResponse, 0, len(ids))
	for _, dpID := range ids {
		dataPartitionsView.DataPartitions = append(dataPartitionsView.DataPartitions, &proto.DataPartitionResponse{
			PartitionID: dpID,
			Hosts:       []string{"127.0.0.1:19320"},
		})
	}
	return
}

func genTinyExtentKey(fileOffset uint64) proto.ExtentKey {
	rand.Seed(time.Now().UnixMilli())
	return proto.ExtentKey{
		FileOffset:   fileOffset,
		PartitionId:  uint64(rand.Intn(100)),
		ExtentId:     uint64(rand.Intn(64)),
		ExtentOffset: 0,
		Size:         uint32(rand.Intn(1024)),
		CRC:          0,
	}
}

func genNormalExtentKey(fileOffset uint64) proto.ExtentKey {
	rand.Seed(time.Now().UnixMilli())
	extentID := uint64(rand.Intn(10000))
	if int(extentID) - 1024 <= 0 {
		extentID = uint64(1025)
	}
	return proto.ExtentKey{
		FileOffset:   fileOffset,
		PartitionId:  uint64(rand.Intn(100)),
		ExtentId:     extentID,
		ExtentOffset: 0,
		Size:         uint32(rand.Intn(1048576)),
		CRC:          0,
	}
}

func TestMetaPartition_FreeInode(t *testing.T) {
	mockMaster()
	if err := mockDataNodeServer(); err != nil {
		t.Errorf("mock data node server failed: %v", err)
		return
	}

	rand.Seed(time.Now().UnixNano())
	//mock meta partition
	mp, err := mockMetaPartition(1, 1, proto.StoreModeMem, "./test_free_inode", ApplyMock)
	if err != nil {
		t.Errorf("mock meta partition failed: %v", err)
		return
	}
	mp.topoManager = topology.NewTopologyManager(0, 1, mockMasterClient, mockMasterClient,
		false, false)
	mp.config.VolName = "test"
	mp.config.ConnPool = connpool.NewConnectPool()
	defer releaseMetaPartition(mp)
	go mp.deleteWorker()
	dataPartitionIDMap := make(map[uint64]byte, 0)
	//create inode and set special oss xAttr
	genEkMap := make(map[string]byte, 0)
	for index := 0; index < 128*4; index++ {
		inode := NewInode(uint64(index*10+285), 0)
		fileOffset := uint64(0)
		var ekCount = 0
		if index%4 == 0 {
			ekCount = 64
		}

		if index%4 == 1 {
			ekCount = 128
		}

		if index%4 == 2 {
			ekCount = 192
		}

		if index%4 == 3 {
			ekCount = 256
		}

		for ekIndex := 0; ekIndex <ekCount; {
			var ek proto.ExtentKey
			if ekIndex == 0 {
				ek = genTinyExtentKey(fileOffset)
			} else {
				ek = genNormalExtentKey(fileOffset)
			}
			if _, ok := genEkMap[fmt.Sprintf("%v_%v_%v_%v", ek.PartitionId, ek.ExtentId, ek.ExtentOffset, ek.Size)]; ok {
				continue
			}
			genEkMap[fmt.Sprintf("%v_%v_%v_%v", ek.PartitionId, ek.ExtentId, ek.ExtentOffset, ek.Size)] = 0
			dataPartitionIDMap[ek.PartitionId] = 0
			inode.Extents.Append(context.Background(), ek, inode.Inode)
			ekIndex++
			fileOffset += uint64(ek.Size)
		}
		_, _, _ = mp.inodeTree.Create(nil, inode, true)

		//set xAttr for special inode
		if index%2 == 0 {
			keys := []string{proto.XAttrKeyOSSMultipart}
			values := []string{proto.XAttrKeyOSSMultipart + "_value"}
			if err = setXAttr(inode.Inode, keys, values, mp); err != nil {
				t.Errorf("set xAttr for inode(%v) failed: %v", inode.Inode, err)
				return
			}
			specialInodes[inode.Inode] = 0
			specialExtentsExpectCount += uint64(inode.Extents.Len())
			continue
		}
		normalInodes[inode.Inode] = 0
	}

	//add data node to topology
	_ = mp.topoManager.Start()
	mp.topoManager.AddVolume("test")
	for dpID := range dataPartitionIDMap {
		mp.topoManager.FetchDataPartitionView("test", dpID)
	}

	//start free list schedule
	_ = mp.inodeTree.Range(nil, nil, func(i *Inode) (bool, error) {
		i.SetDeleteMark()
		delInode := NewDeletedInode(i, time.Now().UnixNano() / 1000)
		delInode.setExpired()
		mp.inodeDeletedTree.Create(nil, delInode, false)
		mp.freeList.Push(i.Inode)
		return true, nil
	})

	fmt.Printf("start check free list\n")
	for {
		if mp.freeList.Len() == 0 {
			break
		}
		fmt.Printf("free list count: %v\n", mp.freeList.Len())
		time.Sleep(time.Second*10)
	}
	assert.Equal(t, specialExtentsExpectCount, specialExtentsActualCount)
}