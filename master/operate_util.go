package master

import (
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/ump"
)

func newCreateDataPartitionRequest(partitionType, volName string, ID uint64) (req *proto.CreateDataPartitionRequest) {
	req = &proto.CreateDataPartitionRequest{
		PartitionType: partitionType,
		PartitionId:   ID,
		PartitionSize: util.DefaultDataPartitionSize,
		VolumeId:      volName,
	}
	return
}

func newDeleteDataPartitionRequest(ID uint64) (req *proto.DeleteDataPartitionRequest) {
	req = &proto.DeleteDataPartitionRequest{
		PartitionId: ID,
	}
	return
}

func newLoadDataPartitionMetricRequest(partitionType string, ID uint64) (req *proto.LoadDataPartitionRequest) {
	req = &proto.LoadDataPartitionRequest{
		PartitionType: partitionType,
		PartitionId:   ID,
	}
	return
}

func UnmarshalTaskResponse(task *proto.AdminTask) (err error) {
	bytes, err := json.Marshal(task.Response)
	if err != nil {
		return
	}
	var response interface{}
	switch task.OpCode {
	case proto.OpDataNodeHeartbeat:
		response = &proto.DataNodeHeartBeatResponse{}
	case proto.OpCreateDataPartition:
		response = &proto.CreateDataPartitionResponse{}
	case proto.OpDeleteDataPartition:
		response = &proto.DeleteDataPartitionResponse{}
	case proto.OpLoadDataPartition:
		response = &proto.LoadDataPartitionResponse{}
	case proto.OpDeleteFile:
		response = &proto.DeleteFileResponse{}
	case proto.OpMetaNodeHeartbeat:
		response = &proto.MetaNodeHeartbeatResponse{}
	case proto.OpCreateMetaPartition:
		response = &proto.CreateMetaPartitionResponse{}
	case proto.OpDeleteMetaPartition:
		response = &proto.DeleteMetaPartitionResponse{}
	case proto.OpUpdateMetaPartition:
		response = &proto.UpdateMetaPartitionResponse{}
	case proto.OpLoadMetaPartition:
		response = task.Response.(*proto.LoadMetaPartitionMetricResponse)
	case proto.OpOfflineMetaPartition:
		response = task.Response.(*proto.MetaPartitionOfflineResponse)

	default:
		log.LogError(fmt.Sprintf("unknown operate code(%v)", task.OpCode))
	}

	if response == nil {
		return fmt.Errorf("UnmarshalTaskResponse failed")
	}
	if err = json.Unmarshal(bytes, response); err != nil {
		return
	}
	task.Response = response
	return
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}

func Warn(clusterID, msg string) {
	umpKey := fmt.Sprintf("%s_%s", clusterID, UmpModuleName)
	WarnBySpecialUmpKey(umpKey, msg)
}

func WarnBySpecialUmpKey(umpKey, msg string) {
	log.LogWarn(msg)
	ump.Alarm(umpKey, msg)
}
