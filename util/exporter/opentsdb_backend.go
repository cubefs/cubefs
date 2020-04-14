// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package exporter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	TSDBBackendName       = "tsdb"
	TSDBCollectPeriod     = 15        // second
	TSDBPendingChSize     = 32 * 1024 //
	DataPointsPostMaxSize = 50        //max batch size which datapoints can be sent by once

	TSDBConfigKey           = TSDBBackendName
	TSDBConfigPushPeriodKey = "pushPeriod"
	TSDBConfigAppNameKey    = "appName"
	TSDBConfigDpTypeKey     = "dpType"
	TSDBDPTypeDefault       = "opentsdb"
	TSDBDPTagHostIPKey      = "hostip"
	TSDBDPTagClusterKey     = "cluster"
	TSDBDPTagRoleKey        = "role"
)

var (
	tsdb_replacer = strings.NewReplacer("_", ".", "-", ".")
)

// opentsdb datapoint
type DataPoint struct {
	Metric    string            `json:"metric"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags,omitempty"` //
}

// new datapoint with name and tags
func NewDataPoint(name string, tags map[string]string) *DataPoint {
	return &DataPoint{
		Metric: name,
		Tags:   tags,
	}
}

func (dp *DataPoint) SetValue(val float64) {
	dp.Value = val
}

func (dp *DataPoint) SetTS(interval int64) {
	ts := time.Now().Unix()
	dp.Timestamp = ts - (ts % interval)
}

func (d *DataPoint) Key() string {
	return stringMD5(fmt.Sprintf("{%s: %s}", d.Metric, stringMapToString(d.Tags)))
}

func (dp *DataPoint) SetCommonTags() {
	if dp.Tags == nil {
		dp.Tags = make(map[string]string)
	}
	dp.Tags[TSDBDPTagHostIPKey] = HostIP()
	dp.Tags[TSDBDPTagRoleKey] = Role()
	dp.Tags[TSDBDPTagClusterKey] = ClusterName()
}

type TSDBReq struct {
	AppCode     string      `json:"appCode"`
	ServiceCode string      `json:"serviceCode"`
	DataCenter  string      `json:"dataCenter,omitempty"`
	Region      string      `json:"region,omitempty"`
	ResourceId  string      `json:"resourceId"`
	DataPoints  []DataPoint `json:"dataPoints"`
}

type TSDBResp struct {
	Failed    int64  `json:"failed"`
	Success   int64  `json:"success"`
	RequestId string `json:"requestId"`
}

// opentsdb config
type TSDBConfig struct {
	AppCode     string            `json:"appCode"`
	AppName     string            `json:"appName,omitempty"`
	DpType      string            `json:"dpType,omitempty"`
	ServiceCode string            `json:"serviceCode"`
	DataCenter  string            `json:"dataCenter"`
	ResourceId  string            `json:"resourceId"`
	Url         string            `json:"url"`
	PushPeriod  int64             `json:"pushPeriod,omitempty"`
	Metrics     map[string]string `json:"metrics,omitempty"` //
}

//parse opentsdb config
func ParseTSDBConfig(cfg *config.Config) (opentsdbCfg *TSDBConfig) {
	if data := cfg.GetKeyRaw(TSDBConfigKey); data != nil {
		opentsdbCfg = new(TSDBConfig)
		if err := json.Unmarshal(data, opentsdbCfg); err != nil {
			opentsdbCfg = nil
			log.LogErrorf("parse opentsdb config error: %v", cfg)
		}
		if opentsdbCfg.PushPeriod == 0 {
			opentsdbCfg.PushPeriod = TSDBCollectPeriod
		}
		if opentsdbCfg.AppName == "" {
			opentsdbCfg.AppName = appName
		}
		if opentsdbCfg.DpType == "" {
			opentsdbCfg.DpType = TSDBDPTypeDefault
		}
	}

	return
}

//
type TSDBBackend struct {
	stopC    chan NULL       //stop signal channel
	pendingC chan *DataPoint //metrics which would be push out
	config   *TSDBConfig     //tsdb backend config
}

func NewTSDBBackend(config *TSDBConfig) *TSDBBackend {
	b := new(TSDBBackend)
	b.stopC = make(chan NULL, 1)
	b.pendingC = make(chan *DataPoint, TSDBPendingChSize)
	b.config = config

	return b
}

// push metric to opentsdb
func (b *TSDBBackend) Publish(m Metric) {
	name := m.Name()
	if len(b.config.Metrics) > 0 {
		if newName, ok := b.config.Metrics[name]; ok {
			name = newName
		} else {
			//log.LogDebugf("exporter: unknown metric name: %v", name)
			return
		}
	}
	name = b.metricName(name)
	dp := NewDataPoint(name, m.Labels())
	dp.SetCommonTags()
	dp.SetValue(m.Val())
	dp.SetTS(b.config.PushPeriod)

	//
	select {
	case b.pendingC <- dp:
	default:
	}
}

// start opentsdb backend
func (b *TSDBBackend) Start() {
	ticker := time.NewTicker(time.Duration(b.config.PushPeriod) * time.Second)
	defer func() {
		if err := recover(); err != nil {
			ticker.Stop()
			log.LogErrorf("exporter: start opentsdb collector error,err[%v]", err)
		}
	}()
	client := &http.Client{}
	defer client.CloseIdleConnections()

	//push metrics
	go func() {
		for {
			select {
			case <-b.stopC:
				log.LogInfo("exporter: opentsdb collector backend stopped.")
				return
			case <-ticker.C:
				dps := make(map[string]*DataPoint)
				pending_empty := false
				for !pending_empty {
					for len(dps) < DataPointsPostMaxSize {
						select {
						case d := <-b.pendingC:
							dps[d.Key()] = d
						default:
							pending_empty = true
							goto BatchSend
						}
					}
				BatchSend:
					if len(dps) > 0 {
						req := b.makeReq(dps)
						resp, e := client.Do(req)
						if e != nil {
							log.LogErrorf("exporter: opentsdb_backend sent error %v %v", resp, e)
						}
						if resp != nil {
							resp.Body.Close()
						}
					}
				}
			}
		}
	}()

	log.LogInfof("exporter: opentsdb backend started")
}

// stop opentsdb backend
func (b *TSDBBackend) Stop() {
	log.LogInfo("exporter: opentsdb collector backend stopping.")
	b.stopC <- null
}

func (b *TSDBBackend) metricName(name string) string {
	return tsdb_replacer.Replace(fmt.Sprintf("%v.%v.%v", b.config.AppName, Role(), name))
}

func (b *TSDBBackend) makeReq(dps map[string]*DataPoint) (req *http.Request) {
	var (
		reqBytes []byte
		err      error
	)
	monitorReq := &TSDBReq{
		AppCode:     b.config.AppCode,
		ServiceCode: b.config.ServiceCode,
		DataCenter:  b.config.DataCenter,
		ResourceId:  b.config.ResourceId,
	}
	for _, dp := range dps {
		monitorReq.DataPoints = append(monitorReq.DataPoints, *dp)
	}
	if b.config.DpType == TSDBDPTypeDefault {
		reqBytes, err = json.Marshal(monitorReq.DataPoints)
	} else {
		reqBytes, err = json.Marshal(monitorReq)
	}
	if err != nil {
		log.LogErrorf("exporter: marshal req: %v, error: %v", monitorReq, err.Error())
		return nil
	}
	req, err = http.NewRequest(http.MethodPost, b.config.Url, bytes.NewBuffer(reqBytes))
	if err != nil {
		log.LogErrorf("exporter: new request error, %v", err.Error())
		return nil
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	log.LogDebugf("exporter: request: %v %v", len(dps), monitorReq)

	return
}
