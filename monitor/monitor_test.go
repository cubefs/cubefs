package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/statistics"
	"io/ioutil"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	monitorUrl = "127.0.0.1:81"
)

var (
	monitorServer = createMonitorServer()
)

func createMonitorServer() *Monitor {
	cfgConfig := `{
			  "role": "monitor",
			  "listen": "81",
			  "prof": "7001",
			  "logDir": "/cfs/log",
			  "logLevel": "debug",
			  "namespace": "test",
			  "cluster": "chubaofs01",
			  "thriftAddr": "127.0.0.1:9091",
			  "producerNum": "10",
			  "splitRegion": [
				"cfs01:0",
				"cfs02:1",
				"mysql:2",
				"spark:6"
			  ]
			}`
	cfg := config.LoadConfigString(cfgConfig)
	server := NewServer()
	if err := server.parseConfig(cfg); err != nil {
		panic(fmt.Sprintf("TestParseConfig: parse config err(%v)", err))
	}
	server.stopC = make(chan bool)
	server.startHTTPService()
	return server
}

func TestCollect(t *testing.T) {
	reqURL := fmt.Sprintf("http://%v%v", monitorUrl, statistics.MonitorCollect)
	reportInfo := &statistics.ReportInfo{
		Cluster: "chubaofs01",
		Addr:    "127.0.0.1",
		Module:  statistics.ModelDataNode,
		Infos: []*statistics.MonitorData{
			{
				VolName:     "ltptest",
				PartitionID: 1,
				Action:      proto.ActionRead,
				Size:        128,
				Count:       2,
				ReportTime:  time.Now().Unix(),
				DiskPath:    "/data1",
			},
		},
	}
	data, err := json.Marshal(reportInfo)
	if err != nil {
		t.Fatalf("TestCollect: json marshal err(%v)", err)
	}
	if err = post(reqURL, data); err != nil {
		t.Fatalf("TestCollect: post err(%v)", err)
	}
}

func TestSetCluster(t *testing.T) {
	setCluster := "cfs1,cfs2"
	reqURL := fmt.Sprintf("http://%v%v?cluster=%v", monitorUrl, statistics.MonitorCluster, setCluster)
	if err := post(reqURL, nil); err != nil {
		t.Fatalf("TestSetCluster: post err(%v)", err)
	}
	if strings.Join(monitorServer.clusters, ",") != setCluster {
		t.Fatalf("TestSetCluster: expect cluster(%v) but(%v)", setCluster, monitorServer.clusters)
	}
}

func TestGetPartitionQueryTable(t *testing.T) {
	tests := []struct {
		name        string
		startTime   string
		endTime     string
		expectTable string
	}{
		{
			name:        "test01",
			startTime:   "20220411120000",
			endTime:     "20220411123000",
			expectTable: "cfs_monitor_20220411120000",
		},
		{
			name:        "test02",
			startTime:   "20220411125500",
			endTime:     "20220411130600",
			expectTable: "cfs_monitor_20220411130000",
		},
		{
			name:        "test03",
			startTime:   "20220411125000",
			endTime:     "20220411130300",
			expectTable: "cfs_monitor_20220411120000",
		},
		{
			name:        "test04",
			startTime:   "20220411125000",
			endTime:     "20220411140300",
			expectTable: "cfs_monitor_20220411120000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table, err := getPartitionQueryTable(tt.startTime, tt.endTime)
			if err != nil || table != tt.expectTable {
				t.Errorf("TestGetPartitionQueryTable: name(%v) err(%v) expect table(%v) but(%v)", tt.name, err, tt.expectTable, table)
			}
		})
	}
}

func TestSplitKey(t *testing.T) {
	// 2022-04-11 14:00:00
	loc, _ := time.LoadLocation("Local")
	initTime, err := time.ParseInLocation("20060102150000", "20220411140000", loc)
	if err != nil {
		t.Fatalf("TestSplitKey: parse location time err(%v)", err)
	}
	expectTableName := "CFS_MONITOR_20220411140000"
	expectSplitKeys := []string{"cfs02,datanode,20220411140000", "cfs02,metanode",
		"mysql,datanode,20220411140000", "mysql,datanode,20220411143000", "mysql,metanode",
		"spark,datanode,20220411140000", "spark,datanode,20220411141000", "spark,datanode,20220411142000",
		"spark,datanode,20220411143000", "spark,datanode,20220411144000", "spark,datanode,20220411145000", "spark,metanode"}
	curTableName, splitKeys := monitorServer.getCreateTableInfo(initTime.Unix())
	if curTableName != expectTableName {
		t.Errorf("TestSplitKey: expect table name(%v) but(%v)", expectTableName, curTableName)
	}
	sort.Strings(splitKeys)
	for i, key := range splitKeys {
		if i >= len(expectSplitKeys) || key != expectSplitKeys[i] {
			t.Errorf("TestSplitKey: expect split key(%v) but(%v)", expectSplitKeys[i], key)
		}
	}
}

func post(reqURL string, data []byte) (err error) {

	var req *http.Request
	reader := bytes.NewReader(data)
	if req, err = http.NewRequest(http.MethodPost, reqURL, reader); err != nil {
		return
	}

	var resp *http.Response
	client := http.DefaultClient
	client.Timeout = 4 * time.Second
	if resp, err = client.Do(req); err != nil {
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("status code[%v]", resp.StatusCode)
		return
	}

	reply := &proto.HTTPReply{}
	if err = json.Unmarshal(body, reply); err != nil {
		return
	}
	if reply.Code != 0 {
		err = fmt.Errorf("request failed, code[%v], msg[%v], data[%v]", reply.Code, reply.Msg, reply.Data)
		return
	}
	return
}
