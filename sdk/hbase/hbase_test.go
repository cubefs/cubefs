package hbase

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"testing"
)

var hBaseClient *HBaseClient

func init() {
	hBaseHost := "api.storage.hbase.jd.local"
	hConfig := config.NewHBaseConfig(hBaseHost)
	hBaseClient = NewHBaseClient(hConfig)
}

func TestHBaseClient_SelectDataPartitionMinuteMetrics(t *testing.T) {
	if hBaseClient == nil {
		t.Fatalf("hBase client is empty")
	}
	clusterId := "smart_vol_test"
	volName := "smart_test"
	dpId := uint64(10)
	action := proto.ActionAppendWrite
	start := "20220325151800"
	end := "20230325172000"
	limit := 10

	request := proto.NewHBaseDPRequest(clusterId, volName, dpId, proto.ActionMetricsTimeTypeMinute, action, start, end, limit)
	metricsData, err := hBaseClient.SelectDataPartitionMetrics(request)
	if err != nil {
		t.Fatalf(err.Error())
	}
	for _, metric := range metricsData {
		fmt.Printf("time: %v, num: %v, size: %v \n", metric.Time, metric.Num, metric.Size)
	}
}

func TestHBaseClient_CheckSparkTaskRunning(t *testing.T) {
	if hBaseClient == nil {
		t.Fatalf("hBase client is empty")
	}
	clusterId := "smart_vol_test"
	res, err := hBaseClient.CheckSparkTaskRunning(clusterId)
	if err != nil {
		t.Fatalf(err.Error())
	}
	fmt.Printf("res : %v\n", res)
}
