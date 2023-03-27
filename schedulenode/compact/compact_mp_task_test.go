package compact

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"strings"
	"testing"
	"time"
)

var (
	masterClient = master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
)
var mpTask *CmpMpTask

var task = &proto.Task{
	TaskId:     0,
	TaskType:   proto.WorkerTypeCompact,
	Cluster:    clusterName,
	VolName:    "volumeName",
	DpId:       0,
	MpId:       1,
	WorkerAddr: "127.0.0.1:17321",
}

func TestMpInitTask(t *testing.T) {
	mpTask = NewMpCmpTask(task, masterClient, vol)
	mpTask.InitTask()
	if mpTask.State != proto.MPCmpGetMPInfo {
		t.Fatalf("MpCmpTask state expect:%v, actual:%v", proto.MPCmpGetMPInfo, mpTask.State)
	}
	fmt.Printf("MpCmpTask name:%v", mpTask.GetTaskName())
}

func TestGetMpInfo(t *testing.T) {
	time.Sleep(time.Minute * 2)
	mpTask = NewMpCmpTask(task, masterClient, vol)
	err := mpTask.GetMpInfo()
	if err != nil {
		t.Fatalf("GetMpInfo err(%v)", err)
	}
	fmt.Printf("mpTask leader:%v\n", mpTask.leader)
	if mpTask.State != proto.MPCmpGetProfPort {
		t.Fatalf("MpCmpTask state expect:%v, actual:%v", proto.MPCmpGetProfPort, mpTask.State)
	}
}

func TestGetProfPort(t *testing.T) {
	TestGetMpInfo(t)
	err := mpTask.GetProfPort()
	if err != nil {
		t.Fatalf("GetProfPort err(%v)", err)
	}
	fmt.Printf("mpTask http ip:port %v\n", mpTask.leader)
	if mpTask.State != proto.MPCmpListAllIno {
		t.Fatalf("MpCmpTask state expect:%v, actual:%v", proto.MPCmpListAllIno, mpTask.State)
	}
}

func TestListAllIno(t *testing.T) {
	TestGetProfPort(t)
	err := mpTask.ListAllIno()
	if err != nil {
		t.Fatalf("ListAllIno err(%v)", err)
	}
	if mpTask.GetTaskState() != uint8(proto.MPCmpGetCmpInodes) {
		t.Fatalf("MpCmpTask state expect:%v, actual:%v", proto.MPCmpGetCmpInodes, mpTask.State)
	}
}
