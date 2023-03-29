package mysql

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"strings"
	"testing"
)

func TestFlowControl(t *testing.T) {
	// add multiple flow controls
	var (
		err   error
		flow1 *proto.FlowControl
		flow2 *proto.FlowControl
		flow3 *proto.FlowControl
	)
	flow1, err = proto.NewFlowControl(proto.WorkerTypeSmartVolume, proto.FlowTypeCluster, "spark", 1000)
	if err != nil {
		t.Fatalf(err.Error())
	}
	flow2, err = proto.NewFlowControl(proto.WorkerTypeSmartVolume, proto.FlowTypeCluster, "test", 1000)
	if err != nil {
		t.Fatalf(err.Error())
	}
	flow3, err = proto.NewFlowControl(proto.WorkerTypeCompact, proto.FlowTypeWorker, "192.168.0.101", 10)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// clean test data
	err = DeleteFlowControl(flow1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = DeleteFlowControl(flow2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = DeleteFlowControl(flow3)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// add new flow control
	err = AddFlowControl(flow1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = AddFlowControl(flow2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = AddFlowControl(flow3)
	if err != nil {
		t.Fatalf(err.Error())
	}
	flows, err := SelectFlowControls()
	if err != nil {
		t.Fatalf(err.Error())
	}

	// select all flow controls
	fmt.Println("list flow controls...")
	for _, f := range flows {
		fmt.Printf("workerType(%v), flowType(%v), flowValue(%v), maxNums(%v)\n", proto.WorkerTypeToName(f.WorkerType), f.FlowType, f.FlowValue, f.MaxNums)
	}

	// update max value
	flow1.MaxNums = 19
	err = UpdateFlowControl(flow1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// select all flow controls
	fmt.Println("list flow controls...")
	flowsNew, err := SelectFlowControls()
	if err != nil {
		t.Fatalf(err.Error())
	}
	for _, f := range flowsNew {
		fmt.Printf("workerType(%v), flowType(%v), flowValue(%v), maxNums(%v)\n", proto.WorkerTypeToName(f.WorkerType), f.FlowType, f.FlowValue, f.MaxNums)
	}

	// select via worker type
	flows, err = SelectFlowControlsViaType(proto.WorkerTypeSmartVolume)
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestFlowControlUnique(t *testing.T) {
	// add multiple flow controls
	flow := &proto.FlowControl{
		WorkerType: proto.WorkerTypeSmartVolume,
		FlowType:   proto.FlowTypeCluster,
		FlowValue:  "spark",
		MaxNums:    1000,
	}

	var err error
	// clean test data
	err = DeleteFlowControl(flow)
	if err != nil {
		t.Fatalf(err.Error())
	}

	// add new flow control
	err = AddFlowControl(flow)
	if err != nil {
		t.Fatalf(err.Error())
	}
	err = AddFlowControl(flow)
	if err != nil && !strings.Contains(err.Error(), "Duplicate entry") {
		t.Fatalf(err.Error())
	}
}
