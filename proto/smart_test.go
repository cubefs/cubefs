package proto

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util/statistics"
	stringutil "github.com/chubaofs/chubaofs/util/string"
	"strings"
	"testing"
	"time"
)

func TestNewActionMetricsTaskInfo(t *testing.T) {
	lp := NewLayerPolicyActionMetrics("spark", "smartTest")
	err := lp.Parse("actionMetrics:dp:read:count:minute:2000:5:hdd")
	if err != nil {
		t.Errorf("parse action metrics layer policy failed")
	}
	hosts := []string{"10.119.20.87", "10.119.20.88", "10.119.20.89"}
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

	taskInfo, err := NewActionMetricsTaskInfo(lp, metrics, hosts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(taskInfo)

	index := strings.Index(taskInfo, ":")
	if index <= 0 {
		t.Fatalf("parse task info failed, index is less then zero")
	}
	lpType := stringutil.SubString(taskInfo, 0, index)
	if len(lpType) <= 0 {
		t.Fatalf("parse task info layer type failed")
	}
	if lpType != LayerTypeActionMetricsString {
		t.Fatalf("parse task info layer type failed")
	}
	taskInfoSubStr := stringutil.SubString(taskInfo, index+1, len(taskInfo))
	if len(taskInfoSubStr) <= 0 {
		t.Fatalf("parse task info sub string failed")
	}
	am, err := UnmarshallActionMetricsTaskInfo(taskInfoSubStr)
	if err != nil {
		t.Fatalf("unmarshall action metrics task info failed")
	}
	if am == nil {
		t.Fatalf("unmarshall action metrics task info failed")
	}
	if am.Action != statistics.ActionRead {
		t.Fatalf("parsed action is not expected, value(%v), excpected(%v)", am.Action, statistics.ActionRead)
	}
	if am.ModuleType != "dp" {
		t.Fatalf("parsed module type is not expected, value(%v), excpected(%v)", am.ModuleType, "dp")
	}
}

func TestNewDPCreateTimeTaskInfo(t *testing.T) {
	lp := NewLayerPolicyDPCreateTime("spark", "smartTest")
	err := lp.Parse("dpCreateTime:timestamp:1653486964:HDD")
	if err != nil {
		t.Errorf("parse dp create time layer policy failed, err(%v)", err)
	}
	hosts := []string{"10.119.20.87", "10.119.20.88", "10.119.20.89"}

	taskInfo, err := NewDPCreateTimeTaskInfo(lp, time.Now().Unix(), hosts)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(taskInfo)

	index := strings.Index(taskInfo, ":")
	if index <= 0 {
		t.Fatalf("parse task info failed, index is less then zero")
	}
	lpType := stringutil.SubString(taskInfo, 0, index)
	if len(lpType) <= 0 {
		t.Fatalf("parse task info layer type failed")
	}
	if lpType != LayerTypeDPCreateTimeString {
		t.Fatalf("parse task info layer type is not expected")
	}
	taskInfoSubStr := stringutil.SubString(taskInfo, index+1, len(taskInfo))
	if len(taskInfoSubStr) <= 0 {
		t.Fatalf("parse task info sub string failed")
	}
	dc, err := UnmarshallDPCreateTimeTaskInfo(taskInfoSubStr)
	if err != nil {
		t.Fatalf("unmarshall dp create time task info failed")
	}
	if dc == nil {
		t.Fatalf("unmarshall dp create time task info failed")
	}
	if dc.TimeType != DPCreateTimeTypeTimestamp {
		t.Fatalf("parsed dp create time task info time type is not expected")
	}
	if dc.TimeValue != 1653486964 {
		t.Fatalf("parsed dp create time task info time value is not expected")
	}
}
