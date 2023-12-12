package topology

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"math/rand"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

const TestMasterHost = "127.0.0.1:19201"

var mockMasterServer *mockMaster

type mockMaster struct {
	MasterAddr string
}

func init() {
	NewMockMaster()
}

func NewMockMaster() {
	fmt.Println("init mock master")
	mockMasterServer = &mockMaster{
		MasterAddr: TestMasterHost,
	}
	profNetListener, err := net.Listen("tcp", TestMasterHost)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		_ = http.Serve(profNetListener, http.DefaultServeMux)
	}()
	go func() {
		http.HandleFunc(proto.ClientDataPartitions, fakeClientDataPartitions)
		http.HandleFunc(proto.AdminGetDataPartition, fakeDataPartitionInfo)
		http.HandleFunc(proto.AdminListVols, fakeVolList)
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

	dps, ok := volumes[volName]
	if !ok {
		err = fmt.Errorf("vol %s not exist", volName)
	}

	if len(ids) == 0 {
		dataPartitionsView.DataPartitions = make([]*proto.DataPartitionResponse, 0, len(dps))
		for _, dp := range dps {
			dataPartitionsView.DataPartitions = append(dataPartitionsView.DataPartitions, &proto.DataPartitionResponse{
				PartitionID: dp.PartitionID,
				Hosts:       dp.Hosts,
			})
		}
		return
	}

	dataPartitionsView.DataPartitions = make([]*proto.DataPartitionResponse, 0, len(ids))
	for _, dpID := range ids {
		for _, dp := range dps {
			if dpID == dp.PartitionID {
				dataPartitionsView.DataPartitions = append(dataPartitionsView.DataPartitions, &proto.DataPartitionResponse{
					PartitionID: dpID,
					Hosts:       dp.Hosts,
				})
				break
			}
		}
	}

	return
}

func fakeDataPartitionInfo(w http.ResponseWriter, r *http.Request) {
	var err error
	var reply = &proto.HTTPReply{
		Code: proto.ErrCodeSuccess,
		Msg:  "OK",
		Data: nil,
	}
	var dataPartitionInfo = &proto.DataPartitionInfo{}
	var respData []byte
	defer func() {
		if err != nil {
			reply.Code = proto.ErrCodeParamError
			reply.Msg = err.Error()
			reply.Data = nil
		} else {
			reply.Data = dataPartitionInfo
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

	idStr := r.FormValue("id")
	if idStr == "" {
		err = fmt.Errorf("data partition id is needed")
		return
	}

	var id uint64
	id, err = strconv.ParseUint(idStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("parse data partition id failed")
		return
	}

	dps, ok := volumes[volName]
	if !ok {
		err = fmt.Errorf("vol %s not exist", volName)
	}
	for _, dataPartition := range dps {
		if id == dataPartition.PartitionID {
			dataPartitionInfo = &proto.DataPartitionInfo{
				PartitionID: id,
				Hosts:       dataPartition.Hosts,
			}
			return
		}
	}
	return
}

func fakeVolList(w http.ResponseWriter, r *http.Request) {
	var err error
	var reply = &proto.HTTPReply{
		Code: proto.ErrCodeSuccess,
		Msg:  "OK",
		Data: nil,
	}
	var volsConf []*proto.VolInfo
	var respData []byte
	defer func() {
		if err != nil {
			reply.Code = proto.ErrCodeParamError
			reply.Msg = err.Error()
			reply.Data = nil
		} else {
			reply.Data = volsConf
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

	volsConf = make([]*proto.VolInfo, 0, len(VolsConf))
	for _, volConf := range VolsConf {
		volsConf = append(volsConf, volConf)
	}
	return
}

var masterClient = master.NewMasterClient([]string{TestMasterHost}, false)

var maxDataPartitionID = uint64(6)

func genPartitionID() uint64 {
	maxDataPartitionID += 1
	return maxDataPartitionID
}

var DataNodeList = []string{
	"192.0.0.1:6000",
	"192.0.0.2:6000",
	"192.0.0.3:6000",
	"192.0.0.4:6000",
	"192.0.0.5:6000",
	"192.0.0.6:6000",
}

func selectReplaceHost(oldHosts []string, selectCount int) (newHosts []string) {
	if selectCount > len(oldHosts) {
		selectCount = len(oldHosts)
	}
	if selectCount > len(DataNodeList)-len(oldHosts) {
		selectCount = len(DataNodeList) - len(oldHosts)
	}

	selectHosts := make([]string, 0, selectCount)
	for _, dataNode := range DataNodeList {
		if includeInHost(dataNode, oldHosts) {
			continue
		}
		selectHosts = append(selectHosts, dataNode)
		if len(selectHosts) == selectCount {
			break
		}
	}
	oldHosts = oldHosts[:len(oldHosts)-selectCount]
	oldHosts = append(oldHosts, selectHosts...)
	return oldHosts
}

func selectNewHost(count int) (hosts []string) {
	hosts = make([]string, 0, count)
	rand.Seed(time.Now().UnixMilli())
	nums := make([]int, 0)
	for count > 0 {
		num := rand.Intn(len(DataNodeList))
		isExist := false
		for _, existNum := range nums {
			if num == existNum {
				isExist = true
				break
			}
		}
		if isExist {
			continue
		}
		nums = append(nums, num)
		count--
	}

	for _, num := range nums {
		hosts = append(hosts, DataNodeList[num])
	}
	return
}

func includeInHost(addr string, hosts []string) bool {
	include := false
	for _, host := range hosts {
		if addr == host {
			include = true
		}
	}
	return include
}

var volumes = map[string][]*DataPartition{
	"test_vol1": {
		{
			PartitionID: 1,
			Hosts:       []string{"192.0.0.1:6000", "192.0.0.2:6000", "192.0.0.4:6000"},
		},
		{
			PartitionID: 2,
			Hosts:       []string{"192.0.0.1:6000", "192.0.0.3:6000", "192.0.0.4:6000"},
		},
		{
			PartitionID: 3,
			Hosts:       []string{"192.0.0.2:6000", "192.0.0.3:6000", "192.0.0.4:6000"},
		},
	},
	"test_vol2": {
		{
			PartitionID: 4,
			Hosts:       []string{"192.0.0.1:6000", "192.0.0.3:6000", "192.0.0.4:6000"},
		},
		{
			PartitionID: 5,
			Hosts:       []string{"192.0.0.2:6000", "192.0.0.3:6000", "192.0.0.4:6000"},
		},
		{
			PartitionID: 6,
			Hosts:       []string{"192.0.0.1:6000", "192.0.0.2:6000", "192.0.0.3:6000"},
		},
	},
}

func addDataPartitionForVol(name string, partition *DataPartition) {
	if _, ok := volumes[name]; !ok {
		return
	}

	dps := volumes[name]
	for _, dp := range dps {
		if dp.PartitionID == partition.PartitionID {
			return
		}
	}
	volumes[name] = append(volumes[name], partition)
}

func changeDataPartitionHostInfo(name string, dpID uint64, newHost []string) {
	if _, ok := volumes[name]; !ok {
		return
	}

	dps := volumes[name]
	for _, dp := range dps {
		if dp.PartitionID == dpID {
			dp.Hosts = newHost
		}
	}
}

var VolsConf = map[string]*proto.VolInfo{
	"test_vol1": {
		Name:                          "test_vol1",
		TrashRemainingDays:            3,
		ChildFileMaxCnt:               1000*10000,
		TrashCleanInterval:            5,
		BatchInodeDelCnt:              64,
		DelInodeInterval:              5,
		EnableBitMapAllocator:         true,
		EnableRemoveDupReq:            true,
		CleanTrashMaxDurationEachTime: 5,
		CleanTrashMaxCountEachTime:    1000,
		TruncateEKCountEveryTime:      1024,
	},
	"test_vol2": {
		Name:                          "test_vol2",
		TrashRemainingDays:            0,
		ChildFileMaxCnt:               0,
		TrashCleanInterval:            0,
		BatchInodeDelCnt:              0,
		DelInodeInterval:              0,
		EnableBitMapAllocator:         false,
		EnableRemoveDupReq:            false,
		CleanTrashMaxDurationEachTime: 0,
		CleanTrashMaxCountEachTime:    0,
		TruncateEKCountEveryTime:      0,
	},
	"test_vol3": {
		Name:                          "test_vol3",
		TrashRemainingDays:            0,
		ChildFileMaxCnt:               0,
		TrashCleanInterval:            0,
		BatchInodeDelCnt:              0,
		DelInodeInterval:              0,
		EnableBitMapAllocator:         false,
		EnableRemoveDupReq:            false,
		CleanTrashMaxDurationEachTime: 0,
		CleanTrashMaxCountEachTime:    0,
		TruncateEKCountEveryTime:      0,
	},
	"test_vol4": {
		Name:                          "test_vol4",
		TrashRemainingDays:            0,
		ChildFileMaxCnt:               0,
		TrashCleanInterval:            0,
		BatchInodeDelCnt:              0,
		DelInodeInterval:              0,
		EnableBitMapAllocator:         false,
		EnableRemoveDupReq:            false,
		CleanTrashMaxDurationEachTime: 0,
		CleanTrashMaxCountEachTime:    0,
		TruncateEKCountEveryTime:      0,
	},
}

func TestFetchTopologyManager_FetchDPView(t *testing.T) {
	topoManager := NewTopologyManager(0, 1, masterClient, masterClient,
		true, false)
	for volName := range volumes {
		topoManager.AddVolume(volName)
	}
	topoManager.Start()
	defer topoManager.Stop()

	time.Sleep(time.Second * 1)

	//get from cache
	if partition := topoManager.GetPartitionFromCache("test_vol1", 1); partition == nil {
		t.Errorf("get partition from cache failed, expect get success\n")
		t.FailNow()
	}

	maxPartitionID := maxDataPartitionID
	if partition := topoManager.GetPartitionFromCache("test_vol1", maxPartitionID+1); partition != nil {
		t.Errorf("get partition from cache success, expect nil\n")
		t.FailNow()
	}

	fmt.Printf("start add new partition for test_vol1\n")
	//add data partition
	newDataPartition := &DataPartition{
		PartitionID: genPartitionID(),
		Hosts:       selectNewHost(3),
	}
	addDataPartitionForVol("test_vol1", newDataPartition)

	//get and for fetch
	if partition := topoManager.GetPartitionFromCache("test_vol1", maxPartitionID+1); partition == nil {
		topoManager.FetchDataPartitionView("test_vol1", newDataPartition.PartitionID)
	}

	time.Sleep(time.Duration(topoManager.forceFetchTimerIntervalSec) * time.Second)

	//get
	if partition := topoManager.GetPartitionFromCache("test_vol1", maxPartitionID+1); partition == nil {
		t.Errorf("get partition from cache failed, expect get success\n")
		t.FailNow()
	}

	fmt.Printf("start change partition 2 host\n")
	partition := topoManager.GetPartitionFromCache("test_vol1", 2)
	if partition == nil {
		t.Errorf("get partition from cache failed, expect get success\n")
		t.FailNow()
	}
	fmt.Printf("partition 2 old host: %v\n", partition.Hosts)

	newHosts := selectReplaceHost(partition.Hosts, 1)
	fmt.Printf("partition 2 new host change to: %v\n", newHosts)
	changeDataPartitionHostInfo("test_vol1", 2, newHosts)

	var err error
	partition, err = topoManager.GetPartitionFromMaster("test_vol1", 2)
	if err != nil {
		t.Errorf("get partition from master failed, expect get success\n")
		t.FailNow()
	}

	if !reflect.DeepEqual(newHosts, partition.Hosts) {
		t.Errorf("partition 2 hosts expect: %v, actual: %v\n", newHosts, partition.Hosts)
		t.FailNow()
	}

	partition = topoManager.GetPartitionFromCache("test_vol1", 2)
	if partition == nil {
		t.Errorf("get partition from cache failed, expect get success\n")
		t.FailNow()
	}

	if !reflect.DeepEqual(newHosts, partition.Hosts) {
		t.Errorf("partition 2 hosts expect: %v, actual: %v\n", newHosts, partition.Hosts)
		t.FailNow()
	}

	fmt.Printf("start add new partition for test_vol2\n")
	//add data partition
	newDataPartition = &DataPartition{
		PartitionID: genPartitionID(),
		Hosts:       selectNewHost(3),
	}
	addDataPartitionForVol("test_vol2", newDataPartition)
	if partition, err = topoManager.GetPartition("test_vol2", newDataPartition.PartitionID); err != nil || partition == nil {
		t.Errorf("get partition failed, expect get success\n")
		t.FailNow()
	}

	//test force fetch ch
	rand.Seed(time.Now().UnixMilli())
	fmt.Printf("start force fetch dp view\n")
	for index := 0; index < 2048; index++ {
		volName := "test_vol1"
		if index%4 == 0 {
			volName = "test_vol2"
		}
		topoManager.FetchDataPartitionView(volName, uint64(rand.Intn(int(maxDataPartitionID))))
	}
	fmt.Printf("force fetch dp view finish\n")
}

func TestFetchTopologyManager_UpdateVolConf(t *testing.T) {
	topoManager := NewTopologyManager(0, 0, masterClient, masterClient, false, true)
	for volName := range volumes {
		topoManager.AddVolume(volName)
	}
	topoManager.Start()
	defer topoManager.Stop()

	time.Sleep(time.Second * 1)
	vol1ExpectConf, _ := VolsConf["test_vol1"]

	volTopo := topoManager.GetVolume("test_vol1")
	if volTopo.config == nil {
		t.Errorf("get vol config failed, expect not null")
		t.FailNow()
	}

	if volTopo.config.GetBatchDelInodeCount() != vol1ExpectConf.BatchInodeDelCnt {
		t.Errorf("test_vol1 batch delete inode count expect: %v, actual: %v",
			volTopo.config.GetBatchDelInodeCount(), vol1ExpectConf.BatchInodeDelCnt)
		t.FailNow()
	}

	if volTopo.config.GetEnableBitMapFlag() != vol1ExpectConf.EnableBitMapAllocator {
		t.Errorf("test_vol1 batch delete inode count expect: %v, actual: %v",
			volTopo.config.GetEnableBitMapFlag(), vol1ExpectConf.EnableBitMapAllocator)
		t.FailNow()
	}

	if volTopo.config.GetCleanTrashItemMaxCount()  != vol1ExpectConf.CleanTrashMaxCountEachTime {
		t.Errorf("test_vol1 clean trash item max count expect: %v, actual: %v",
			volTopo.config.GetCleanTrashItemMaxCount(), vol1ExpectConf.CleanTrashMaxCountEachTime)
		t.FailNow()
	}

	if volTopo.config.GetCleanTrashItemMaxDuration() != vol1ExpectConf.CleanTrashMaxDurationEachTime {
		t.Errorf("test_vol1 clean trash item max duration expect: %v, actual: %v",
			volTopo.config.GetCleanTrashItemMaxDuration(), vol1ExpectConf.CleanTrashMaxDurationEachTime)
		t.FailNow()
	}

	if volTopo.config.GetDelInodeInterval() != vol1ExpectConf.DelInodeInterval {
		t.Errorf("test_vol1 del Inode interval expect: %v, actual: %v",
			volTopo.config.GetDelInodeInterval(), vol1ExpectConf.DelInodeInterval)
		t.FailNow()
	}

	if volTopo.config.GetChildFileMaxCount() != vol1ExpectConf.ChildFileMaxCnt {
		t.Errorf("test_vol1 child file max count expect: %v, actual: %v",
			volTopo.config.GetChildFileMaxCount(), vol1ExpectConf.ChildFileMaxCnt)
		t.FailNow()
	}

	if volTopo.config.GetEnableRemoveDupReqFlag() != vol1ExpectConf.EnableRemoveDupReq {
		t.Errorf("test_vol1 enable remove dup req expect: %v, actual: %v",
			volTopo.config.GetEnableRemoveDupReqFlag(), vol1ExpectConf.EnableRemoveDupReq)
		t.FailNow()
	}

	if volTopo.config.GetTrashDays() != int32(vol1ExpectConf.TrashRemainingDays) {
		t.Errorf("test_vol1 trash remain days expect: %v, actual: %v",
			volTopo.config.GetTrashDays(), vol1ExpectConf.TrashRemainingDays)
		t.FailNow()
	}

	if volTopo.config.GetTruncateEKCount() != vol1ExpectConf.TruncateEKCountEveryTime {
		t.Errorf("test_vol1 truncate ek count expect: %v, actual: %v",
			volTopo.config.GetTruncateEKCount(), vol1ExpectConf.TruncateEKCountEveryTime)
		t.FailNow()
	}

	if volTopo.config.GetTrashCleanInterval() != vol1ExpectConf.TrashCleanInterval {
		t.Errorf("test_vol1 trash clean interval expect: %v, actual: %v",
			volTopo.config.GetTrashCleanInterval(), vol1ExpectConf.TrashCleanInterval)
		t.FailNow()
	}

	volTopo = topoManager.GetVolume("test_vol3")
	if volTopo.config != nil {
		t.Errorf("get vol config failed, expect is null")
		t.FailNow()
	}
}
