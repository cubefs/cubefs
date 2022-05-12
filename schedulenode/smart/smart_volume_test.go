package smart

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"testing"
	"time"
)

func TestParseConfig(t *testing.T) {
	cfgJSON := `{
		"role": "schedulenode",
		"localIP": "11.97.57.231",
		"prof": "17330",
		"workerTaskPeriod": 5,
		"logDir": "/export/Logs/chubaofs/scheduleNode1/",
		"logLevel": "debug",
		"mysql": {
			"url": "11.97.57.230",
			"userName": "root",
			"password": "123456",
			"database": "smart",
			"port": 3306
		},
		"hBaseUrl": "api.storage.hbase.jd.local/",
		"clusterAddr": {
			"smart_vol_test": [
				"172.20.81.30:17010",
				"172.20.81.31:17010",
				"11.7.139.135:17010"
			],
			"smart_vol_test2": [
				"172.20.81.30:17010",
				"172.20.81.31:17010",
				"11.7.139.135:17010"
			]
		}
    }`
	cfg := config.LoadConfigString(cfgJSON)
	smart := NewSmartVolumeWorker()
	err := smart.parseConfig(cfg)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if smart.LocalIp != "11.97.57.231" {
		t.Errorf("invalid local ip")
	}
	if smart.MysqlConfig.Url != "11.97.57.230" {
		t.Errorf("invalid mysql url")
	}
	if _, ok := smart.masterAddr["smart_vol_test"]; !ok {
		t.Errorf("not contain special cluster")
	}
}

func TestParseLayerPolicy(t *testing.T) {
	smart := NewSmartVolumeWorker()
	amRule1 := "actionMetrics:dp:read:count:minute:2000:5:hdd"
	amRule2 := "actionMetrics:dp:appendWrite:count:minute:1000:5:hdd"
	dcRule1 := "dpCreateTime:timestamp:1653486964:HDD"
	dcRule2 := "dpCreateTime:days:30:HDD"
	smartRules := []string{amRule1, amRule2, dcRule1, dcRule2}
	smartVolume := &proto.SmartVolume{
		ClusterId:     "spark",
		Name:          "testVolume",
		SmartRules:    smartRules,
		LayerPolicies: make(map[proto.LayerType][]interface{}),
	}
	smart.parseLayerPolicy(smartVolume)
	fmt.Println(len(smartVolume.LayerPolicies))
	if len(smartVolume.LayerPolicies) != 2 {
		t.Errorf("parsed smart rules is not expected")
	}
	for lt, lps := range smartVolume.LayerPolicies {
		switch lt {
		case proto.LayerTypeActionMetrics:
			for _, lp := range lps {
				am, ok := lp.(*proto.LayerPolicyActionMetrics)
				if !ok {
					t.Fatalf("invalid action metrics layer policy")
				}
				fmt.Printf("Threshold(%v), TargetMedium(%v), LessCount(%v), Action(%v), OriginRule(%v)\n",
					am.Threshold, am.TargetMedium, am.LessCount, am.Action, am.OriginRule)
			}
		case proto.LayerTypeDPCreateTime:
			for _, lp := range lps {
				dc, ok := lp.(*proto.LayerPolicyDPCreateTime)
				if !ok {
					t.Fatalf("invalid dp craete time layer policy")
				}
				fmt.Printf("TargetMedium(%v), TimeValue(%v), TimeType(%v), OriginRule(%v)\n",
					dc.TargetMedium, dc.TimeValue, dc.TimeType, dc.OriginRule)
			}
		default:
			t.Fatalf("invalid layer type")
		}
	}
}

func TestIsDPSufficientMigrated(t *testing.T) {
	cluster := "spark"
	volumeCommon1 := "smartVolumeCommon1"
	volumeCommon2 := "smartVolumeCommon2"
	volumeCommon3 := "smartVolumeCommon3"
	volumeException1 := "smartVolumeException1"
	volumeException2 := "smartVolumeException2"
	volumeException3 := "smartVolumeException3"

	dataPartitionIdStartValue := 1000
	createDP := func(num, writableDPNum int) (dps []*proto.DataPartitionResponse) {
		var status, writableNum int
		for i := 0; i < num; i++ {
			if writableNum < writableDPNum {
				status = proto.ReadWrite
				writableNum++
			} else {
				status = proto.ReadOnly
			}
			dp := &proto.DataPartitionResponse{
				PartitionID: uint64(dataPartitionIdStartValue),
				Status:      int8(status),
			}
			dps = append(dps, dp)
			dataPartitionIdStartValue++
		}
		return
	}

	smartVolumes := []*proto.SmartVolume{
		{
			ClusterId:      cluster,
			Name:           volumeCommon1,
			DataPartitions: createDP(10, 6),
		},
		{
			ClusterId:      cluster,
			Name:           volumeCommon2,
			DataPartitions: createDP(30, 10),
		},
		{
			ClusterId:      cluster,
			Name:           volumeCommon3,
			DataPartitions: createDP(110, 30),
		},
		{
			ClusterId:      cluster,
			Name:           volumeException1,
			DataPartitions: createDP(10, 3),
		},
		{
			ClusterId:      cluster,
			Name:           volumeException2,
			DataPartitions: createDP(30, 3),
		},
		{
			ClusterId:      cluster,
			Name:           volumeException3,
			DataPartitions: createDP(110, 1),
		},
	}
	smartVolumeView := proto.NewSmartVolumeView(cluster, smartVolumes)
	smartWorker := NewSmartVolumeWorker()
	smartWorker.svv = make(map[string]*proto.SmartVolumeView)
	smartWorker.svv[cluster] = smartVolumeView

	var (
		dp1, dp2, dp3, dp4, dp5, dp6       *proto.DataPartitionResponse
		res1, res2, res3, res4, res5, res6 bool
		err                                error
	)

	dp1 = &proto.DataPartitionResponse{
		PartitionID: 100,
		Status:      proto.ReadWrite,
	}
	dp2 = &proto.DataPartitionResponse{
		PartitionID: 100,
		Status:      proto.ReadWrite,
	}
	dp3 = &proto.DataPartitionResponse{
		PartitionID: 100,
		Status:      proto.ReadWrite,
	}
	dp4 = &proto.DataPartitionResponse{
		PartitionID: 100,
		Status:      proto.ReadWrite,
	}
	dp5 = &proto.DataPartitionResponse{
		PartitionID: 100,
		Status:      proto.ReadWrite,
	}
	dp6 = &proto.DataPartitionResponse{
		PartitionID: 100,
		Status:      proto.ReadWrite,
	}

	res1, err = smartWorker.isDPSufficientMigrated(cluster, volumeCommon1, dp1)
	if err != nil {
		t.Errorf(err.Error())
	}
	if !res1 {
		t.Errorf("result is not expected")
	}
	res2, err = smartWorker.isDPSufficientMigrated(cluster, volumeCommon2, dp2)
	if err != nil {
		t.Errorf(err.Error())
	}
	if !res2 {
		t.Errorf("result is not expected")
	}
	res3, err = smartWorker.isDPSufficientMigrated(cluster, volumeCommon3, dp3)
	if err != nil {
		t.Errorf(err.Error())
	}
	if !res3 {
		t.Errorf("result is not expected")
	}

	res4, err = smartWorker.isDPSufficientMigrated(cluster, volumeException1, dp4)
	if err != nil {
		t.Errorf(err.Error())
	}
	if res4 {
		t.Errorf("result is not expected")
	}
	res5, err = smartWorker.isDPSufficientMigrated(cluster, volumeException2, dp5)
	if err != nil {
		t.Errorf(err.Error())
	}
	if res5 {
		t.Errorf("result is not expected")
	}
	res6, err = smartWorker.isDPSufficientMigrated(cluster, volumeException3, dp6)
	if err != nil {
		t.Errorf(err.Error())
	}
	if res6 {
		t.Errorf("result is not expected")
	}
}

func TestGetStartTime2(t *testing.T) {
	timeValue := time.Now()
	fmt.Printf("day : %v\n", timeValue.Day())
	fmt.Printf("Hour : %v\n", timeValue.Hour())
	fmt.Printf("Minute : %v\n", timeValue.Minute())
	fmt.Printf("Second : %v\n", timeValue.Second())
}

func TestGetStartTime(t *testing.T) {
	lessCount := 20
	_, resMinute, err := getStartTime(proto.ActionMetricsTimeTypeMinute, lessCount)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(resMinute)

	_, resHour, err := getStartTime(proto.ActionMetricsTimeTypeHour, lessCount)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(resHour)

	_, resDay, err := getStartTime(proto.ActionMetricsTimeTypeDay, lessCount)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(resDay)
}

func TestGetStopTime(t *testing.T) {
	resMinute, err := getStopTime(proto.ActionMetricsTimeTypeMinute)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(resMinute)

	resHour, err := getStopTime(proto.ActionMetricsTimeTypeHour)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(resHour)

	resDay, err := getStopTime(proto.ActionMetricsTimeTypeDay)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Println(resDay)
}

func TestGetStandardMinuteStartTime(t *testing.T)  {
	lessCountOrigin1 := 10
	lessCountOrigin2 := 13
	lessCount1 := getStandardMinuteLessCount(lessCountOrigin1)
	fmt.Printf("lessCount1: %v\n", lessCount1)
	if lessCount1 != 10 {
		t.Fatalf("expected 10, but value is %v", lessCount1)
	}

	lessCount2 := getStandardMinuteLessCount(lessCountOrigin2)
	fmt.Printf("lessCount2: %v\n", lessCount2)
	if lessCount2 != 15 {
		t.Fatalf("expected 10, but value is %v", lessCount2)
	}
}

func TestCalcAuthKey(t *testing.T) {
	fmt.Println(CalcAuthKey("root"))
}
