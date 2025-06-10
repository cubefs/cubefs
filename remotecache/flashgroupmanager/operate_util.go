package flashgroupmanager

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

func Warn(clusterID, msg string) {
	key := fmt.Sprintf("%s_%s", clusterID, ModuleName)
	WarnBySpecialKey(key, msg)
}

// WarnBySpecialKey provides warnings when exits
func WarnBySpecialKey(key, msg string) {
	log.LogWarn(msg)
	exporter.Warning(msg)
}

func notFoundMsg(name string) (err error) {
	return errors.NewErrorf("%v not found", name)
}

func unmatchedKey(name string) (err error) {
	return errors.NewErrorf("parameter %v not match", name)
}

func keyNotFound(name string) (err error) {
	return errors.NewErrorf("parameter %v not found", name)
}

func contains(arr []string, element string) (ok bool) {
	if len(arr) == 0 {
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

func unmarshalTaskResponse(task *proto.AdminTask) (err error) {
	bytes, err := json.Marshal(task.Response)
	if err != nil {
		return
	}
	var response interface{}
	switch task.OpCode {
	case proto.OpFlashNodeHeartbeat:
		response = &proto.FlashNodeHeartbeatResponse{}
	case proto.OpFlashNodeScan:
		response = &proto.FlashNodeManualTaskResponse{}

	default:
		log.LogError(fmt.Sprintf("unknown operate code(%v)", task.OpCode))
	}

	if response == nil {
		return fmt.Errorf("unmarshalTaskResponse failed")
	}
	if err = json.Unmarshal(bytes, response); err != nil {
		return
	}
	task.Response = response
	return
}
