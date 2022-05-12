package proto

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/statistics"
	"strings"
	"testing"
	"time"
)

func TestCheckLayerPolicy(t *testing.T) {
	var err error
	cluster := "spark"
	volName := "smartVol"
	smartRules := []string{"actionMetrics:dp:read:count:minute:1000:5:hdd",
		"actionMetrics:dp:appendWrite:size:hour:999999:9:hdd",
		"dpCreateTime:timestamp:1653486964:hdd",
		"dpCreateTime:days:30:ec"}
	err = CheckLayerPolicy(cluster, volName, smartRules)
	if err != nil {
		t.Fatalf(err.Error())
	}

	smartRulesException := []string{"actionMetrics:dp:read:count:minute:1000:5:hdd", "actionMetrics:dp:appendWrite:size:hour:999999:9:ec"}
	err = CheckLayerPolicy(cluster, volName, smartRulesException)
	if err == nil {
		t.Fatalf("expected exception: all action metrics target medium type not same")
	}
	if !strings.Contains(err.Error(), "all action metrics target medium type not same") {
		t.Fatalf(err.Error())
	}
}

func TestParseLayerType(t *testing.T) {
	ruleActionMetrics := "actionMetrics:dp:read:count:minute:2000:5:hdd"
	lt, err := ParseLayerType(ruleActionMetrics)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lt != LayerTypeActionMetrics {
		t.Fatalf("layer type is not expected action metrics")
	}

	ruleDPCreateTime1 := "dpCreateTime:timestamp:1653486964:HDD"
	lt1, err := ParseLayerType(ruleDPCreateTime1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lt1 != LayerTypeDPCreateTime {
		t.Fatalf("layer type is not expected action metrics")
	}

	ruleDPCreateTime2 := "dpCreateTime:days:30:HDD"
	lt2, err := ParseLayerType(ruleDPCreateTime2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lt2 != LayerTypeDPCreateTime {
		t.Fatalf("layer type is not expected action metrics")
	}
}

func TestParseLayerPolicy(t *testing.T) {
	var (
		err error
		ok  bool
		lt  LayerType
		lp  interface{}
	)
	cluster := "spark"
	volName := "smartVol"
	amRule1 := "actionMetrics:dp:read:count:minute:1000:5:hdd"
	lt, lp, err = ParseLayerPolicy(cluster, volName, amRule1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lt != LayerTypeActionMetrics {
		t.Fatalf("expected layer type is action metrics")
	}
	_, ok = lp.(*LayerPolicyActionMetrics)
	if !ok {
		t.Fatalf("expected layer policy instance is action metrics")
	}

	amRule2 := "actionMetrics:dp:appendWrite:size:hour:999999:9:hdd"
	lt, lp, err = ParseLayerPolicy(cluster, volName, amRule2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lt != LayerTypeActionMetrics {
		t.Fatalf("expected layer type is action metrics")
	}
	_, ok = lp.(*LayerPolicyActionMetrics)
	if !ok {
		t.Fatalf("expected layer policy instance is action metrics")
	}

	dpcRule1 := "dpCreateTime:timestamp:1653486964:hdd"
	lt, lp, err = ParseLayerPolicy(cluster, volName, dpcRule1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lt != LayerTypeDPCreateTime {
		t.Fatalf("expected layer type is data partition create time")
	}
	_, ok = lp.(*LayerPolicyDPCreateTime)
	if !ok {
		t.Fatalf("expected layer policy instance is data partition create time")
	}

	dpcRule2 := "dpCreateTime:days:30:ec"
	lt, lp, err = ParseLayerPolicy(cluster, volName, dpcRule2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lt != LayerTypeDPCreateTime {
		t.Fatalf("expected layer type is data partition create time")
	}
	_, ok = lp.(*LayerPolicyDPCreateTime)
	if !ok {
		t.Fatalf("expected layer policy instance is data partition create time")
	}
}

func TestNewLayerPolicyActionMetrics(t *testing.T) {
	ruleActionMetrics1 := "actionMetrics:dp:read:count:minute:2000:5:hdd"
	lp1 := NewLayerPolicyActionMetrics("spark", "smartVolume")
	fmt.Printf("LayerPolicyActionMetricsString: %s\n", lp1.String())
	err := lp1.Parse(ruleActionMetrics1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lp1.Action != statistics.ActionRead {
		t.Fatalf("parsed action is not expected")
	}
	if lp1.LessCount != 5 {
		t.Fatalf("parsed action is not expected")
	}
	if lp1.TimeType != ActionMetricsTimeTypeMinute {
		t.Fatalf("parsed time type is not expected")
	}
	if lp1.Threshold != 2000 {
		t.Fatalf("parsed threshold is not expected")
	}

	ruleActionMetrics2 := "actionMetrics:dp:appendWrite:count:hour:50000:10:ec"
	lp2 := NewLayerPolicyActionMetrics("spark", "smartVolume")
	err = lp2.Parse(ruleActionMetrics2)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if lp2.Action != statistics.ActionAppendWrite {
		t.Fatalf("parsed action is not expected")
	}
	if lp2.LessCount != 10 {
		t.Fatalf("parsed action is not expected")
	}
	if lp2.TimeType != ActionMetricsTimeTypeHour {
		t.Fatalf("parsed time type is not expected")
	}
	if lp2.Threshold != 50000 {
		t.Fatalf("parsed threshold is not expected")
	}
}

func TestActionMetricsParse(t *testing.T) {
	cluster := "spark"
	volName := "smartVolume"
	smartRules := "actionMetrics : dp : read : count : minute : 2000 : 5 : hdd"
	smartRules = strings.ReplaceAll(smartRules, " ", "")
	items := strings.Split(smartRules, ":")
	if len(items) != 8 {
		t.Fatalf("invalid rule")
	}

	lp := NewLayerPolicyActionMetrics(cluster, volName)
	if err := lp.ParseModuleType(items[1]); err != nil {
		t.Fatalf("invalid module type")
	}
	if err := lp.ParseAction(items[2]); err != nil {
		t.Fatalf("invalid action")
	}
	if err := lp.ParseDataType(items[3]); err != nil {
		t.Fatalf("invalid data type")
	}
	if err := lp.ParseTimeType(items[4]); err != nil {
		t.Fatalf("invalid time type")
	}
	if err := lp.ParseThreshold(items[5]); err != nil {
		t.Fatalf("invalid threshold")
	}
	if err := lp.ParseLessCount(items[6]); err != nil {
		t.Fatalf("invalid less count")
	}
	if err := lp.ParseTargetMedium(items[7]); err != nil {
		t.Fatalf("invalid target medium")
	}
	if lp.ModuleType != ModuleTypeData {
		t.Fatalf("module type is not expected")
	}
	if lp.Action != statistics.ActionRead {
		t.Fatalf("action is not expected")
	}
	if lp.DataType != DataTypeCount {
		t.Fatalf("data type is not expected")
	}
	if lp.TimeType != ActionMetricsTimeTypeMinute {
		t.Fatalf("time type is not expected")
	}
	if lp.Threshold != 2000 {
		t.Fatalf("threshold is not expected")
	}
	if lp.LessCount != 5 {
		t.Fatalf("less count is not expected")
	}
	if lp.TargetMedium != MediumHDD {
		t.Fatalf("target medium is not expected")
	}
}

func TestActionMetricsCheckMigrated(t *testing.T) {
	ruleActionMetrics1 := "actionMetrics:dp:read:count:minute:2000:5:hdd"
	lp := NewLayerPolicyActionMetrics("spark", "smartVolume")
	fmt.Printf("LayerPolicyActionMetricsString: %s\n", lp.String())
	err := lp.Parse(ruleActionMetrics1)
	if err != nil {
		t.Fatalf(err.Error())
	}

	dpHDD := &DataPartitionResponse{
		PartitionID: 100,
		MediumType:  MediumHDDName,
	}
	metrics := []*HBaseMetricsData{
		{
			Time: "20220311163200",
			Num:  84,
			Size: 1680,
		},
		{
			Time: "20220311163300",
			Num:  109,
			Size: 2080,
		},
		{
			Time: "20220311163400",
			Num:  67,
			Size: 279,
		},
	}

	res := lp.CheckDPMigrated(dpHDD, metrics)
	if res {
		t.Fatalf("dpHDD shuold not be migrated")
	}

	dpSSD := &DataPartitionResponse{
		PartitionID: 100,
		MediumType:  MediumSSDName,
	}
	res = lp.CheckDPMigrated(dpSSD, metrics)
	if !res {
		t.Fatalf("dpSSD shuold be migrated")
	}

	metrics2 := []*HBaseMetricsData{
		{
			Time: "20220311163200",
			Num:  2084,
			Size: 1680,
		},
		{
			Time: "20220311163300",
			Num:  109,
			Size: 2080,
		},
		{
			Time: "20220311163400",
			Num:  67,
			Size: 279,
		},
	}
	res = lp.CheckDPMigrated(dpSSD, metrics2)
	if res {
		t.Fatalf("dpSSD with metrics2 shuold not be migrated")
	}
}

func TestLayerPolicyDPCreateTime_Parse(t *testing.T) {
	lp := NewLayerPolicyDPCreateTime("spark", "smartVolume")
	fmt.Printf("LayerPolicyDPCreateTimeString: %s\n", lp.String())
	originRule := "dpCreateTime:timestamp:1653486964:HDD"

	items := strings.Split(originRule, ":")
	var err error
	if err = lp.ParseTimeType(items[1]); err != nil {
		t.Fatalf(err.Error())
	}
	if err = lp.ParseTimeValue(items[2]); err != nil {
		t.Fatalf(err.Error())
	}
	if err = lp.ParseTargetMedium(items[3]); err != nil {
		t.Fatalf(err.Error())
	}
	if lp.TimeType != DPCreateTimeTypeTimestamp {
		t.Fatalf("parsed time type is not expected")
	}
	if lp.TimeValue != 1653486964 {
		t.Fatalf("parsed time value is not expected")
	}
	if lp.TargetMedium != MediumHDD {
		t.Fatalf("parsed target medium is not expected")
	}
}

func TestLayerPolicyDPCreateTime_CheckDPMigrated(t *testing.T) {
	lp := NewLayerPolicyDPCreateTime("spark", "smartTest")
	err := lp.Parse("dpCreateTime:timestamp:1653486964:HDD")
	if err != nil {
		t.Errorf("parse dp create time layer policy failed, err(%v)", err)
	}
	dp := &DataPartitionResponse{
		PartitionID: 100,
		CreateTime:  time.Now().Unix(),
		MediumType:  MediumHDDName,
	}
	res := lp.CheckDPMigrated(dp, nil)
	if res {
		t.Fatalf("dp shuold not be migrated")
	}

	dp = &DataPartitionResponse{
		PartitionID: 100,
		CreateTime:  time.Now().Unix(),
		MediumType:  MediumSSDName,
	}
	res = lp.CheckDPMigrated(dp, nil)
	if res {
		t.Fatalf("dp shuold not be migrated")
	}

	dp = &DataPartitionResponse{
		PartitionID: 100,
		CreateTime:  1653486800,
		MediumType:  MediumSSDName,
	}
	res = lp.CheckDPMigrated(dp, nil)
	if !res {
		t.Fatalf("dp shuold be migrated")
	}

	err = lp.Parse("dpCreateTime:days:20:HDD")
	if err != nil {
		t.Errorf("parse dp create time layer policy failed, err(%v)", err)
	}
	dp = &DataPartitionResponse{
		PartitionID: 100,
		CreateTime:  1650854158, // 2022-04-25 10:35:58
		MediumType:  MediumSSDName,
	}
	res = lp.CheckDPMigrated(dp, nil)
	if !res {
		t.Fatalf("dp shuold be migrated")
	}

	dp = &DataPartitionResponse{
		PartitionID: 100,
		CreateTime:  time.Now().Unix(),
		MediumType:  MediumSSDName,
	}
	res = lp.CheckDPMigrated(dp, nil)
	if res {
		t.Fatalf("dp shuold not be migrated")
	}
}

func TestWorkerNode_ContainTask(t *testing.T) {
	tasks := []*Task{
		{
			TaskId: 10,
		},
		{
			TaskId: 11,
		},
		{
			TaskId: 12,
		},
	}
	workerNode := WorkerNode{
		Tasks: tasks,
	}

	task1 := &Task{TaskId: 11}
	res1 := workerNode.ContainTaskByTaskId(task1)
	fmt.Printf("res: %v\n", res1)
	if !res1 {
		t.Fatalf("result is wrong")
	}

	task2 := &Task{TaskId: 15}
	res := workerNode.ContainTaskByTaskId(task2)
	fmt.Printf("res: %v\n", res)
	if res {
		t.Fatalf("result is wrong")
	}
}

func TestWorkerNode_RemoveTaskFromHead(t *testing.T) {
	tasks := []*Task{
		{
			TaskId: 10,
		},
		{
			TaskId: 11,
		},
		{
			TaskId: 12,
		},
		{
			TaskId: 13,
		},
		{
			TaskId: 14,
		},
	}
	workerNode := WorkerNode{
		Tasks: tasks,
	}

	task1 := &Task{TaskId: 10}
	workerNode.RemoveTask(task1)
	res1 := workerNode.ContainTaskByTaskId(task1)
	if res1 {
		t.Fatalf("result is wrong")
	}
	for _, task := range workerNode.Tasks {
		fmt.Println(task.TaskId)
	}
}

func TestWorkerNode_RemoveTaskFromMiddle(t *testing.T) {
	tasks := []*Task{
		{
			TaskId: 10,
		},
		{
			TaskId: 11,
		},
		{
			TaskId: 12,
		},
		{
			TaskId: 13,
		},
		{
			TaskId: 14,
		},
	}
	workerNode := WorkerNode{
		Tasks: tasks,
	}

	task1 := &Task{TaskId: 12}
	workerNode.RemoveTask(task1)
	res1 := workerNode.ContainTaskByTaskId(task1)
	if res1 {
		t.Fatalf("result is wrong")
	}
	for _, task := range workerNode.Tasks {
		fmt.Println(task.TaskId)
	}
}

func TestWorkerNode_RemoveTaskFromEnd(t *testing.T) {
	tasks := []*Task{
		{
			TaskId: 10,
		},
		{
			TaskId: 11,
		},
		{
			TaskId: 12,
		},
		{
			TaskId: 13,
		},
		{
			TaskId: 14,
		},
	}
	workerNode := WorkerNode{
		Tasks: tasks,
	}

	task1 := &Task{TaskId: 14}
	workerNode.RemoveTask(task1)
	res1 := workerNode.ContainTaskByTaskId(task1)
	if res1 {
		t.Fatalf("result is wrong")
	}
	for _, task := range workerNode.Tasks {
		fmt.Println(task.TaskId)
	}
}
