package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/statistics"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net/http"
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
			  "cluster": "chubaofs01",
			  "producerNum": "10"
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
		Zone:    "default",
		Infos: []*statistics.ReportData{
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
	reqURL := fmt.Sprintf("http://%v%v?cluster=%v", monitorUrl, statistics.MonitorClusterSet, setCluster)
	assert.Equal(t, nil, post(reqURL, nil), "send set cluster")
	assert.Equal(t, setCluster, strings.Join(monitorServer.clusters, ","), "set cluster")

	addCluster := "test01"
	reqURL = fmt.Sprintf("http://%v%v?cluster=%v", monitorUrl, statistics.MonitorClusterAdd, addCluster)
	assert.Equal(t, nil, post(reqURL, nil), "send add cluster")
	assert.Equal(t, "cfs1,cfs2,test01", strings.Join(monitorServer.clusters, ","), "add cluster")

	delCluster := "cfs1"
	reqURL = fmt.Sprintf("http://%v%v?cluster=%v", monitorUrl, statistics.MonitorClusterDel, delCluster)
	assert.Equal(t, nil, post(reqURL, nil), "send del cluster")
	assert.Equal(t, "cfs2,test01", strings.Join(monitorServer.clusters, ","), "del cluster")

	reqURL = fmt.Sprintf("http://%v%v", monitorUrl, statistics.MonitorClusterGet)
	assert.Equal(t, nil, post(reqURL, nil), "send get cluster")
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
