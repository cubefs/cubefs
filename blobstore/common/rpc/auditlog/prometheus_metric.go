// Copyright 2022 The CubeFS Authors.
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

package auditlog

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

var Buckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

var MetricLabelNames = []string{
	"host", "service", "team", "tag", "api", "method", "code", "uid",
	"reqlength", "resplength", "deleteAfterDays", "idc", "xwarn", "country", "region", "isp",
}

type PrometheusConfig struct {
	Idc     string `json:"idc"`
	Service string `json:"service"`
	Tag     string `json:"tag"`
	Team    string `json:"team"`

	SetDefaultSwitch    bool            `json:"set_default_switch"`
	EnableHttpMethod    bool            `json:"enable_http_method"`
	DisableApi          bool            `json:"disable_api"`
	EnableReqLengthCnt  bool            `json:"enable_req_length_cnt"`
	EnableRespLengthCnt bool            `json:"enable_resp_length_cnt"`
	EnableRespDuration  bool            `json:"enable_resp_duration"`
	EnableXWarnCnt      bool            `json:"enable_xwarn_cnt"`
	MaxApiLevel         int             `json:"max_api_level"`
	XWarns              []string        `json:"xwarns"`
	ErrCodes            map[string]bool `json:"resp_err_codes"`
	SizeBuckets         []int64         `json:"size_buckets"`

	// UserLabels maps an audit log response header key (map key) to the
	// Prometheus label name (map value) added to every metric vector. The
	// header key is what callers write via ExtraHeader; the label name is what
	// appears as the metric dimension. They may differ so a caller-owned
	// header key never dictates the metric label surface.
	UserLabels map[string]string `json:"user_labels"`

	// registerer is nil in production and uses prometheus.DefaultRegisterer.
	// Tests set an isolated registry so a different label set does not collide
	// with metrics already registered in the process.
	registerer prometheus.Registerer `json:"-"`
}

type PrometheusSender struct {
	hostname                string
	logParser               func(string) (LogEntry, error)
	responseCodeCounter     *prometheus.CounterVec
	responseErrCodeCounter  *prometheus.CounterVec
	responseDurationCounter *prometheus.HistogramVec
	responseLengthCounter   *prometheus.CounterVec
	requestLengthCounter    *prometheus.CounterVec
	xwarnCounter            *prometheus.CounterVec

	sizeBuckets []string

	userLabelKeys  []string
	userLabelNames []string

	PrometheusConfig
}

func NewPrometheusSender(conf PrometheusConfig) (ps *PrometheusSender) {
	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("get hostname failed, err: %s", err.Error()))
	}

	subsystem := "service"
	constLabels := map[string]string{
		"idc":     conf.Idc,
		"host":    hostname,
		"service": conf.Service,
		"team":    conf.Team,
	}

	userLabelKeys, userLabelNames := sortedUserLabels(conf.UserLabels, constLabels)

	registerer := conf.registerer
	if registerer == nil {
		registerer = prometheus.DefaultRegisterer
	}
	responseCodeCounter := mustRegisterOrGet(registerer, getResponseCounterVec(subsystem, constLabels, userLabelNames)).(*prometheus.CounterVec)
	responseErrCodeCounter := mustRegisterOrGet(registerer, getResponseErrCounterVec(subsystem, constLabels, userLabelNames)).(*prometheus.CounterVec)
	responseDurationCounter := mustRegisterOrGet(registerer, getResponseDurationVec(subsystem, constLabels, userLabelNames)).(*prometheus.HistogramVec)
	responseLengthCounter := mustRegisterOrGet(registerer, getResponseLengthCounterVec(subsystem, constLabels, userLabelNames)).(*prometheus.CounterVec)
	requestLengthCounter := mustRegisterOrGet(registerer, getRequestLengthCounterVec(subsystem, constLabels, userLabelNames)).(*prometheus.CounterVec)
	xwarnCounter := mustRegisterOrGet(registerer, getXwarnCountVec(constLabels, userLabelNames)).(*prometheus.CounterVec)

	sizeBuckets := make([]string, 0, len(conf.SizeBuckets))
	for idx := range conf.SizeBuckets {
		if idx == len(conf.SizeBuckets)-1 {
			sizeBuckets = append(sizeBuckets, strconv.FormatInt(conf.SizeBuckets[idx], 10)+"_")
		} else {
			sizeBuckets = append(sizeBuckets, strconv.FormatInt(conf.SizeBuckets[idx], 10)+"_"+
				strconv.FormatInt(conf.SizeBuckets[idx+1], 10))
		}
	}

	ps = &PrometheusSender{
		hostname:                hostname,
		PrometheusConfig:        conf,
		logParser:               ParseReqlog,
		responseCodeCounter:     responseCodeCounter,
		responseErrCodeCounter:  responseErrCodeCounter,
		responseDurationCounter: responseDurationCounter,
		responseLengthCounter:   responseLengthCounter,
		requestLengthCounter:    requestLengthCounter,
		xwarnCounter:            xwarnCounter,
		sizeBuckets:             sizeBuckets,
		userLabelKeys:           userLabelKeys,
		userLabelNames:          userLabelNames,
	}
	return ps
}

// Send inherit from Sender
func (ps *PrometheusSender) Send(raw []byte) error {
	line := string(raw)
	ps.parseLine(line, ps.hostname)
	return nil
}

func (ps *PrometheusSender) parseLine(line, host string) {
	entry, err := ps.logParser(line)
	if err != nil {
		log.Debugf("logParser failed, %s, %s", line, err.Error())
		return
	}
	ps.SendEntry(entry)
}

func (ps *PrometheusSender) SendEntry(entry LogEntry) {
	method := entry.Method()
	service := entry.Service()
	api := ""
	if !ps.DisableApi {
		api = apiName(service, method, entry.Path(), ps.MaxApiLevel, entry.ApiName())
	}

	var tags []string
	if ps.Tag != "" {
		tags = strings.Split(ps.Tag, ",")
	}
	switch service {
	case "UP", "IO":
		tmp := genXlogTags(service, entry.Xlogs(), entry.RespLength())
		tags = append(tags, tmp...)
	default:

	}
	tags = sortAndUniq(tags)
	tag := strings.Join(tags, ",")
	statusCode := entry.Code()

	userVals := ps.userLabelValues(entry)
	ps.responseCodeCounter.WithLabelValues(append([]string{tag, api, method, statusCode}, userVals...)...).Inc()
	if ps.isErrCode(statusCode) {
		uid := strconv.FormatUint(uint64(entry.Uid()), 10)
		ps.responseErrCodeCounter.WithLabelValues(append([]string{tag, api, method, uid, statusCode}, userVals...)...).Inc()
	}

	requestLength := entry.ReqLength()
	responseLength := entry.RespLength()
	if ps.EnableReqLengthCnt {
		ps.requestLengthCounter.WithLabelValues(append([]string{tag, api, method, statusCode}, userVals...)...).Add(float64(requestLength))
	}
	if ps.EnableRespLengthCnt {
		ps.responseLengthCounter.WithLabelValues(append([]string{tag, api, method, statusCode}, userVals...)...).Add(float64(responseLength))
	}
	if ps.EnableRespDuration && (strings.HasPrefix(statusCode, "2") || statusCode == "499") {
		reqlengthTag := ps.getSizeTag(requestLength)
		resplengthTag := ps.getSizeTag(responseLength)
		respTimeMs := float64(entry.RespTime()) / 1e3
		ps.responseDurationCounter.WithLabelValues(append([]string{tag, api, method, statusCode, reqlengthTag, resplengthTag}, userVals...)...).Observe(respTimeMs)
	}
	if ps.EnableXWarnCnt {
		if xwarns := entry.XWarns(); len(xwarns) != 0 {
			for _, hit := range hitXWarns(xwarns, ps.XWarns) {
				ps.xwarnCounter.WithLabelValues(append([]string{hit, statusCode}, userVals...)...).Add(1)
			}
		}
	}
}

func (ps *PrometheusSender) userLabelValues(entry LogEntry) []string {
	if len(ps.userLabelKeys) == 0 {
		return nil
	}
	vals := make([]string, len(ps.userLabelKeys))
	for i, key := range ps.userLabelKeys {
		vals[i] = strings.Join(entry.RespHeader(key), ",")
	}
	return vals
}

func sortedUserLabels(userLabels map[string]string, constLabels map[string]string) (keys, names []string) {
	if len(userLabels) == 0 {
		return nil, nil
	}

	seen := make(map[string]struct{}, 0)
	for k := range constLabels {
		seen[k] = struct{}{}
	}
	for _, name := range MetricLabelNames {
		seen[name] = struct{}{}
	}

	all := make([]string, 0, len(userLabels))
	for key := range userLabels {
		all = append(all, key)
	}
	sort.Strings(all)
	keys = make([]string, 0, len(all))
	names = make([]string, 0, len(all))
	for _, key := range all {
		if key == "" {
			continue
		}
		label := userLabels[key]
		if label == "" {
			label = key
		}
		if !model.LabelName(label).IsValid() {
			continue
		}
		if _, ok := seen[label]; ok {
			panic(fmt.Sprintf("user_labels label %q conflicts", label))
		}
		seen[label] = struct{}{}
		keys = append(keys, key)
		names = append(names, label)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return keys, names
}

func (ps *PrometheusSender) isErrCode(code string) bool {
	return ps.ErrCodes[code]
}

func (ps *PrometheusSender) getSizeTag(size int64) string {
	if len(ps.SizeBuckets) == 0 {
		return ""
	}
	i := sort.Search(len(ps.SizeBuckets), func(i int) bool {
		return ps.SizeBuckets[i] >= size
	})
	if i == 0 {
		return "0"
	}
	if i > len(ps.SizeBuckets) {
		return ps.sizeBuckets[len(ps.sizeBuckets)-1]
	}
	return ps.sizeBuckets[i-1]
}

func getResponseCounterVec(logtype string, constLabels map[string]string, userLabels []string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        logtype + "_response_code",
			Help:        logtype + " response code",
			ConstLabels: constLabels,
		},
		metricLabels([]string{"tag", "api", "method", "code"}, userLabels),
	)
}

func getResponseErrCounterVec(logtype string, constLabels map[string]string, userLabels []string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        logtype + "_response_err_code",
			Help:        logtype + " response err code",
			ConstLabels: constLabels,
		},
		metricLabels([]string{"tag", "api", "method", "uid", "code"}, userLabels),
	)
}

func getRequestLengthCounterVec(logtype string, constLabels map[string]string, userLabels []string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        logtype + "_request_length",
			Help:        logtype + " request length",
			ConstLabels: constLabels,
		},
		metricLabels([]string{"tag", "api", "method", "code"}, userLabels),
	)
}

func getResponseLengthCounterVec(logtype string, constLabels map[string]string, userLabels []string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        logtype + "_response_length",
			Help:        logtype + " response length",
			ConstLabels: constLabels,
		},
		metricLabels([]string{"tag", "api", "method", "code"}, userLabels),
	)
}

func getResponseDurationVec(logtype string, constLabels map[string]string, userLabels []string) *prometheus.HistogramVec {
	buckets := Buckets
	return prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        logtype + "_response_duration_ms",
			Help:        logtype + " response duration ms",
			Buckets:     buckets,
			ConstLabels: constLabels,
		},
		metricLabels([]string{"tag", "api", "method", "code", "reqlength", "resplength"}, userLabels),
	)
}

func getXwarnCountVec(constLabels map[string]string, userLabels []string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "service_xwarn_count",
			Help:        "service xwarn count",
			ConstLabels: constLabels,
		},
		metricLabels([]string{"xwarn", "code"}, userLabels),
	)
}

// metricLabels returns base with userLabels appended. base is a fresh slice
// literal at every caller, so append never mutates shared state.
func metricLabels(base []string, userLabels []string) []string {
	if len(userLabels) == 0 {
		return base
	}
	return append(base, userLabels...)
}

func mustRegisterOrGet(registerer prometheus.Registerer, c prometheus.Collector) prometheus.Collector {
	if err := registerer.Register(c); err != nil {
		if are, ok := err.(prometheus.AlreadyRegisteredError); ok {
			return are.ExistingCollector
		}
		panic(err)
	}
	return c
}
