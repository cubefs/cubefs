package statistics

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type Partition struct {
	monitorData []*MonitorData
	PartitionId uint64
	opAction    string
}

var partition = []*Partition{
	{PartitionId: 0, monitorData: InitMonitorData(ModelMetaNode)}, //meta
	{PartitionId: 1, monitorData: InitMonitorData(ModelMetaNode)}, //meta
	{PartitionId: 2, monitorData: InitMonitorData(ModelMetaNode)}, //meta

}
var (
	dataSize  uint64 = 100
	collector []*ReportInfo
)

func SummaryMonitorData(reportTime int64) []*MonitorData {
	dataList := make([]*MonitorData, 0)
	for _, p := range partition {
		for i := 0; i < len(p.monitorData); i++ {
			if atomic.LoadUint64(&p.monitorData[i].Count) == 0 {
				continue
			}
			data := &MonitorData{
				PartitionID: p.PartitionId,
				Action:      i,
				Size:        atomic.SwapUint64(&p.monitorData[i].Size, 0),
				Count:       atomic.SwapUint64(&p.monitorData[i].Count, 0),
				ReportTime:  reportTime,
				ActionStr:   p.opAction,
			}
			dataList = append(dataList, data)
		}
	}
	return dataList
}

func collectHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err   error
		bytes []byte
	)
	if bytes, err = ioutil.ReadAll(r.Body); err != nil {
		return
	}
	reportInfo := &ReportInfo{}
	if err = json.Unmarshal(bytes, reportInfo); err != nil {
		return
	} else {
		if collector != nil {
			collector = nil
		}
		collector = append(collector, reportInfo)
	}
	reply := &HTTPReply{
		Code: 0,
		Msg:  "Success",
	}
	httpReply, _ := json.Marshal(reply)
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(httpReply)))
	w.Write(httpReply)
}

func TestStatistics(t *testing.T) {
	conn, err := net.Listen("tcp", "127.0.0.1:8800")
	if err != nil {
		t.Fatal("failed", err)
	}
	http.HandleFunc("/collect", collectHandler)
	go http.Serve(conn, nil)

	cfgJson := `{
		"monitorAddr": "127.0.0.1:8800"
	}`
	cfg := config.LoadConfigString(cfgJson)
	InitStatistics(cfg, "test", "metaNode", "127.0.0.1", SummaryMonitorData)

	helper(1, t)
	time.Sleep(time.Second * 1)
	helper(3, t)
}

func helper(times int, t *testing.T) {
	for i := 0; i < times; i++ {
		partition[0].monitorData[proto.ActionMetaCreateInode].UpdateData(dataSize)
		partition[1].monitorData[proto.ActionMetaLookup].UpdateData(2 * dataSize)
		partition[2].monitorData[proto.ActionMetaExtentsAdd].UpdateData(3 * dataSize)
	}
	time.Sleep(time.Second * time.Duration(StatisticsModule.GetMonitorReportTime()))
	for _, cc := range collector {
		var size [3]uint64
		var count [3]uint64
		for _, monitordata := range cc.Infos {
			size[monitordata.PartitionID] += monitordata.Size
			count[monitordata.PartitionID] += monitordata.Count
		}
		for i, _ := range size {
			if size[i] == uint64(i+1)*uint64(times)*dataSize && count[i] == uint64(times) {
				continue
			} else {
				t.Errorf("partiton(%v) collected is error: Size(%v), Count(%v)\n", i, size[i], count[i])
			}
		}
	}
}
