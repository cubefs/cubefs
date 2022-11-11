package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/scheduler"
)

var (
	testDataPartitions  []*DataPartition
	reportDpMetrics		[]*proto.DataPartitionMetrics

	schedulerDomain 		= "127.0.0.1:10080"
	failedSchedulerDomain 	= "127.0.0.1:10081"
	testCluster				= "test"
	testVol					= "testVol"
)

func TestWrapper_SummaryAndSortReadDelay(t *testing.T) {
	wrapper := &Wrapper{
		clusterName:               testCluster,
		volName:                   testVol,
		dpFollowerReadDelayConfig: &proto.DpFollowerReadDelayConfig{EnableCollect: true},
		partitions: 				new(sync.Map),
	}

	testDataPartitions = make([]*DataPartition, 6)
	for i := 0; i < len(testDataPartitions); i++ {
		testDataPartitions[i] = &DataPartition{
			DataPartitionResponse: proto.DataPartitionResponse{
				Hosts: []string{"127.0.0.1:8080", "127.0.0.1:8081", "127.0.0.1:8082"},
			},
			ReadMetrics: &proto.ReadMetrics{
				SumFollowerReadHostDelay: map[string]int64{
					"127.0.0.1:8080": 6,
					"127.0.0.1:8081": 36,
				},
				FollowerReadOpNum: map[string]int64{
					"127.0.0.1:8080": 6,
					"127.0.0.1:8081": 6,
				},
				AvgFollowerReadHostDelay: make(map[string]int64),
			},
		}
		testDataPartitions[i].PartitionID = uint64(i)
		wrapper.partitions.Store(uint64(i), testDataPartitions[i])
	}
	wrapper.SummaryAndSortReadDelay()
	// check sorted ReadMetrics
	dp, _ := wrapper.partitions.Load(uint64(0))
	sortedHost :=  dp.(*DataPartition).ReadMetrics.SortedHost
	testForHostDelay := []struct {
		name string
		host string
	}{
		{
			name: "1st",
			host: "127.0.0.1:8080",
		},
		{
			name: "2nd",
			host: "127.0.0.1:8081",
		},
		{
			name: "3rd",
			host: "127.0.0.1:8082",
		},
	}
	for i, tt := range testForHostDelay {
		t.Run(tt.name, func(t *testing.T) {
			if sortedHost[i] != tt.host {
				t.Errorf("TestWrapper_SummaryAndSortReadDelay: test[%v],expect[%v],but[%v]", tt.name, tt.host, sortedHost[i])
				return
			}
		})
	}

}

func TestReportMetrics(t *testing.T) {
	initTestDataPartitionsMetricsHelper(true)
	LocalIP = "127.0.0.0"
	var err error
	wrapper := &Wrapper{
		clusterName: 			testCluster,
		volName: 				testVol,
		dpMetricsReportConfig: 	&proto.DpMetricsReportConfig{EnableReport: true},
	}
	wrapper.schedulerClient = scheduler.NewSchedulerClient(schedulerDomain, false)
	wrapper.dpSelector, err = newKFasterRandomSelector(&DpSelectorParam{kValue: "50", quorum: 3})
	if err != nil {
		t.Fatalf("TestReportMetrics: new kfaster err(%v)", err)
	}
	wrapper.refreshDpSelector(testDataPartitions)
	time.Sleep(3 * time.Second)
	// check report
	wrapper.reportMetrics()
	time.Sleep(5 * time.Second)
	for i, m := range reportDpMetrics {
		fmt.Printf("reportDpMetrics: m(%v)\n", m)
		if m.PartitionId == 0 || m.SumWriteLatencyNano != int64(m.PartitionId)*1e9 || m.WriteOpNum != int64(m.PartitionId) {
			t.Errorf("TestReportMetrics: report metrics uncorrect i(%v) metric(%v)", i, m)
		}
	}
	// check fetch
	wrapper.refreshMetrics()
	for _, dp := range testDataPartitions {
		fmt.Printf("checkDpMetrics: m(%v)\n", dp.Metrics)
		if dp.Metrics.SumWriteLatencyNano != 0 || dp.Metrics.WriteOpNum != 0 {
			t.Errorf("TestReportMetrics: expect sum(0) but sum(%v) num(%v)", dp.Metrics.SumWriteLatencyNano, dp.Metrics.WriteOpNum)
		}
		if dp.PartitionID == 0 {
			if dp.Metrics.AvgWriteLatencyNano != 0 {
				t.Errorf("TestReportMetrics: expect avg(0) but(%v)", dp.Metrics.AvgWriteLatencyNano)
			}
			continue
		}
		if dp.Metrics.AvgWriteLatencyNano != 1e8 {
			t.Errorf("TestReportMetrics: expect avg(1e8) but(%v)", dp.Metrics.AvgWriteLatencyNano)
		}
	}
}

func TestRemoteFail(t *testing.T) {
	initTestDataPartitionsMetricsHelper(false)
	LocalIP = "127.0.0.0"
	var err error
	wrapper := &Wrapper{
		clusterName: 			testCluster,
		volName: 				testVol,
		dpMetricsReportConfig: 	&proto.DpMetricsReportConfig{EnableReport: true},
	}
	wrapper.schedulerClient = scheduler.NewSchedulerClient(failedSchedulerDomain, false)
	wrapper.dpSelector, err = newKFasterRandomSelector(&DpSelectorParam{kValue: "50", quorum: 3})
	if err != nil {
		t.Fatalf("TestReportMetrics: new kfaster err(%v)", err)
	}
	wrapper.refreshDpSelector(testDataPartitions)
	// test fetch fail
	for i := 0; i < 4; i++ {
		wrapper.refreshMetrics()
	}
	// check report
	wrapper.reportMetrics()
	for _, dp := range testDataPartitions {
		fmt.Printf("checkDpMetrics: m(%v)\n", dp.Metrics)
		if dp.PartitionID == 0 {
			continue
		}
		if dp.Metrics.SumWriteLatencyNano == 0 || dp.Metrics.WriteOpNum == 0 {
			t.Errorf("TestReportMetrics: expect sum is not 0 but sum(%v) num(%v)", dp.Metrics.SumWriteLatencyNano, dp.Metrics.WriteOpNum)
		}
	}
	// check fetch
	wrapper.refreshMetrics()
	for _, dp := range testDataPartitions {
		fmt.Printf("checkDpMetrics: m(%v)\n", dp.Metrics)
		if dp.Metrics.SumWriteLatencyNano != 0 || dp.Metrics.WriteOpNum != 0 {
			t.Errorf("TestReportMetrics: expect avg(0) but sum(%v) num(%v)", dp.Metrics.SumWriteLatencyNano, dp.Metrics.WriteOpNum)
		}
		if dp.PartitionID == 0 {
			if dp.Metrics.AvgWriteLatencyNano != 0 {
				t.Errorf("TestReportMetrics: expect avg(0) but(%v)", dp.Metrics.AvgWriteLatencyNano)
			}
			continue
		}
		if dp.Metrics.AvgWriteLatencyNano != 1e8 {
			t.Errorf("TestReportMetrics: expect avg(1e8) but(%v)", dp.Metrics.AvgWriteLatencyNano)
		}
	}
}

func initTestDataPartitionsMetricsHelper(startServer bool) {
	testDataPartitions = make([]*DataPartition, 6)
	for i := 0; i < len(testDataPartitions); i++ {
		testDataPartitions[i] = &DataPartition{
			Metrics: &proto.DataPartitionMetrics{
				SumWriteLatencyNano: 	int64(i * 1e9),
				WriteOpNum: 			int64(i),
			},
		}
		testDataPartitions[i].PartitionID = uint64(i)
	}
	if !startServer {
		return
	}
	http.HandleFunc(proto.ReportUrl, reportHandler)
	http.HandleFunc(proto.FetchUrl, fetchHandler)

	go func() {
		fmt.Println("init scheduler domain: ", schedulerDomain)
		err := http.ListenAndServe(schedulerDomain, nil)
		fmt.Println("init http err: ", err)
	}()

	return
}

func reportHandler(w http.ResponseWriter, r *http.Request) {
	if err := checkParams(r); err != nil {
		fmt.Printf("reportHandler: err(%v)", err)
		sendReply(w, 1)
		return
	}
	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("reportHandler: err(%v)", err)
		sendReply(w, 1)
		return
	}
	fmt.Println("reportHandler parse bytes length: ", len(bytes))
	m := &proto.DataPartitionMetricsMessage{}
	if err = m.DecodeBinary(bytes); err != nil {
		fmt.Printf("reportHandler: err(%v)", err)
		sendReply(w, 1)
		return
	}
	fmt.Println("reportHandler parse metrics: ", m.DpMetrics)
	reportDpMetrics = m.DpMetrics
	sendReply(w, 0)
	return
}

func fetchHandler(w http.ResponseWriter, r *http.Request) {
	if err := checkParams(r); err != nil {
		fmt.Printf("fetchHandler: err(%v)", err)
		return
	}
	m := &proto.DataPartitionMetricsMessage{DpMetrics: reportDpMetrics}
	fmt.Println("fetchHandler send metrics: ", m.DpMetrics)
	reply := m.EncodeBinary()
	fmt.Println("fetchHandler send reply length: ", len(reply))
	w.Header().Set("content-type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	w.Write(reply)
	return
}

func checkParams(r *http.Request) (err error) {
	var (
		version 	uint64
		cluster		string
		vol			string
		ip			string
		timestamp	int64
	)
	fmt.Println("check params enter")
	if verStr := r.FormValue("version"); verStr == "" {
		return fmt.Errorf("key version not fount")
	} else {
		if version, err = strconv.ParseUint(verStr, 10, 8); err != nil || version != 0 {
			return fmt.Errorf("err(%v) key version(%v) not correct", err, version)
		}
	}
	if cluster = r.FormValue("cluster"); cluster != testCluster {
		return fmt.Errorf("key cluster(%v) not correct", cluster)
	}
	if vol = r.FormValue("vol"); vol != testVol {
		return fmt.Errorf("key vol(%v) not correct", vol)
	}
	if ip = r.FormValue("ip"); ip != LocalIP {
		return fmt.Errorf("key ip(%v) not correct", ip)
	}
	if timeStr := r.FormValue("time"); timeStr == "" {
		return fmt.Errorf("key time not found")
	} else {
		timestamp, err = strconv.ParseInt(timeStr, 10, 64)
		if err != nil || (time.Now().Unix() - timestamp) > 10 {
			return fmt.Errorf("err(%v) key time(%v) not correct", err, timestamp)
		}
	}
	fmt.Printf("check params exit: version(%v) cluster(%v) vol(%v) ip(%v) timestamp(%v)", version, cluster, vol, ip, timestamp)
	return
}

func sendReply(w http.ResponseWriter, code int32) {
	httpReply := &HTTPReply{Code: code}
	reply, err := json.Marshal(httpReply)
	if err != nil {
		fmt.Printf("sendReply: err(%v)", err)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		fmt.Printf("sendReply: err(%v)", err)
	}
	return
}