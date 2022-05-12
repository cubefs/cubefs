package hbase

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/exporter"
	"github.com/chubaofs/chubaofs/util/log"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

const (
	QueryMetrics    = "/queryJson/cfsDataPartitionMetrics"
	QueryTaskStatus = "/queryJson/cfsDataPartitionMetricTaskStatus"
)

const (
	DefaultHBaseRequestTimeout = 60 * time.Second
)

type HBaseClient struct {
	host       string
	timeout    time.Duration
	httpClient *HttpBaseClent
}

func NewHBaseClient(hc *config.HBaseConfig) *HBaseClient {
	hBaseClient := &HBaseClient{
		host:    hc.Host,
		timeout: DefaultHBaseRequestTimeout,
	}
	httpClient := NewHttpBaseClient(hBaseClient.timeout)
	hBaseClient.httpClient = httpClient
	return hBaseClient
}

func (hc *HBaseClient) SelectDataPartitionMetrics(request *proto.HBaseRequest) (metricsData []*proto.HBaseMetricsData, err error) {
	var (
		resp      *http.Response
		timeout   bool
		respData  []byte
		stateCode int
		params    map[string]string
	)

	metrics := exporter.NewTPCnt(proto.MonitorHBaseSelectDPMetrics)
	defer metrics.Set(err)

	header := make(map[string]string)
	if params, err = request.ToParam(); err != nil {
		log.LogErrorf("[SelectDataPartitionMetrics] transfer hBase request to param failed, host(%v), err(%v)", hc.host, err)
		return nil, errors.New("transfer hBase request to param failed")
	}

	var url = fmt.Sprintf("http://%s%s", hc.host, QueryMetrics)
	resp, err, timeout = hc.httpClient.HttpRequest(http.MethodGet, url, params, header, nil)
	if err != nil {
		log.LogErrorf("[SelectDataPartitionMetrics] hBase request failed: host(%v), timeout(%v), err(%v)", hc.host, hc.timeout, err)
		return nil, err
	}
	if timeout {
		log.LogErrorf("[SelectDataPartitionMetrics] send hBase request timeout, host(%v), timeout(%v), err(%v)", hc.host, hc.timeout, err)
		return nil, errors.New("hBase request timeout")
	}

	stateCode = resp.StatusCode
	respData, err = ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	log.LogDebugf("[SelectDataPartitionMetrics] params(%v), respData(%v)", params, string(respData))
	if err != nil {
		log.LogErrorf("[SelectDataPartitionMetrics] read http response body fail: err(%v)", err)
		return nil, err
	}
	if stateCode != http.StatusOK {
		log.LogErrorf("[SelectDataPartitionMetrics] response status code not ok, stateCode(%v), err(%v)", stateCode, err)
		return nil, errors.New("response status code not ok")
	}
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err := json.Unmarshal(respData, body); err != nil {
		log.LogErrorf("[SelectDataPartitionMetrics] unmarshall response body failed, respData(%v), err(%v)", string(respData), err)
		return nil, fmt.Errorf("unmarshal response body err:%v", err)
	}
	if body.Code != 0 {
		log.LogErrorf("[SelectDataPartitionMetrics] request failed, code(%v), msg(%v), data(%v)", body.Code, body.Msg, body.Data)
		return nil, proto.ParseErrorCode(body.Code)
	}

	metricsData = make([]*proto.HBaseMetricsData, 0)
	if err = json.Unmarshal(body.Data, &metricsData); err != nil {
		log.LogErrorf("[SelectDataPartitionMetrics] unmarshall response body data failed, respData(%v), err(%v)", string(respData), err)
		return nil, err
	}
	return metricsData, nil
}

func (hc *HBaseClient) CheckSparkTaskRunning(cluster string) (res bool, err error) {
	var (
		resp      *http.Response
		timeout   bool
		respData  []byte
		stateCode int
	)

	metrics := exporter.NewTPCnt(proto.MonitorHBaseCheckSparkTaskRunning)
	defer metrics.Set(err)

	param := make(map[string]string)
	param[proto.ParamNameCluster] = cluster
	param[proto.ParamNameMilliSecond] = strconv.FormatInt(time.Now().Unix(), 10)
	var url = fmt.Sprintf("http://%s%s", hc.host, QueryTaskStatus)
	resp, err, timeout = hc.httpClient.HttpRequest(http.MethodGet, url, param, nil, nil)
	if err != nil {
		log.LogErrorf("[CheckSparkTaskRunning] hBase request failed: host(%v), timeout(%v), err(%v)", hc.host, hc.timeout, err)
		return false, err
	}
	if timeout {
		log.LogErrorf("[CheckSparkTaskRunning] send hBase request timeout, host(%v), timeout(%v), err(%v)", hc.host, hc.timeout, err)
		return false, errors.New("hBase request timeout")
	}

	stateCode = resp.StatusCode
	respData, err = ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	log.LogDebugf("[CheckSparkTaskRunning] respData(%v)", string(respData))
	if err != nil {
		log.LogErrorf("[CheckSparkTaskRunning] read http response body fail: err(%v)", err)
		return false, err
	}
	if stateCode != http.StatusOK {
		log.LogErrorf("[CheckSparkTaskRunning] response status code not ok, stateCode(%v), err(%v)", stateCode, err)
		return false, errors.New("response status code not ok")
	}
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err := json.Unmarshal(respData, body); err != nil {
		log.LogErrorf("[CheckSparkTaskRunning] unmarshall response body failed, respData(%v), err(%v)", string(respData), err)
		return false, fmt.Errorf("unmarshal response body err:%v", err)
	}
	if body.Code != 0 {
		log.LogErrorf("[CheckSparkTaskRunning] request failed, code(%v), msg(%v), data(%v)", body.Code, body.Msg, body.Data)
		return false, proto.ParseErrorCode(body.Code)
	}

	var taskStatus []*struct {
		Count int `json:"COUNT"`
	}
	if err = json.Unmarshal(body.Data, &taskStatus); err != nil {
		log.LogErrorf("[CheckSparkTaskRunning] unmarshall response body data failed, respData(%v), err(%v)", string(respData), err)
		return false, err
	}
	if taskStatus == nil || len(taskStatus) == 0 {
		log.LogErrorf("[CheckSparkTaskRunning] parsed result is empty, respData(%v)", string(respData))
		return false, err
	}
	return taskStatus[0].Count > 0, nil
}
