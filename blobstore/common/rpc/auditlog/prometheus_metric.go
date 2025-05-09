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
)

var Buckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

var MetricLabelNames = []string{
	"host", "service", "team", "tag", "api", "method", "code",
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
	responseCodeCounter := getResponseCounterVec(subsystem, constLabels)
	responseErrCodeCounter := getResponseErrCounterVec(subsystem, constLabels)
	responseDurationCounter := getResponseDurationVec(subsystem, constLabels)
	responseLengthCounter := getResponseLengthCounterVec(subsystem, constLabels)
	requestLengthCounter := getRequestLengthCounterVec(subsystem, constLabels)
	xwarnCounter := getXwarnCountVec(constLabels)

	prometheus.MustRegister(responseCodeCounter)
	prometheus.MustRegister(responseErrCodeCounter)
	prometheus.MustRegister(responseDurationCounter)
	prometheus.MustRegister(responseLengthCounter)
	prometheus.MustRegister(requestLengthCounter)
	prometheus.MustRegister(xwarnCounter)

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
	}
	tags = sortAndUniq(tags)
	tag := strings.Join(tags, ",")
	statusCode := entry.Code()

	ps.responseCodeCounter.WithLabelValues(tag, api, method, statusCode).Inc()
	if ps.isErrCode(statusCode) {
		uid := strconv.FormatUint(uint64(entry.Uid()), 10)
		ps.responseErrCodeCounter.WithLabelValues(tag, api, method, uid, statusCode).Inc()
	}

	requestLength := entry.ReqLength()
	responseLength := entry.RespLength()
	if ps.EnableReqLengthCnt {
		ps.requestLengthCounter.WithLabelValues(tag, api, method, statusCode).Add(float64(requestLength))
	}
	if ps.EnableRespLengthCnt {
		ps.responseLengthCounter.WithLabelValues(tag, api, method, statusCode).Add(float64(responseLength))
	}
	if ps.EnableRespDuration && (strings.HasPrefix(statusCode, "2") || statusCode == "499") {
		reqlengthTag := ps.getSizeTag(requestLength)
		resplengthTag := ps.getSizeTag(responseLength)
		respTimeMs := float64(entry.RespTime()) / 1e3
		ps.responseDurationCounter.WithLabelValues(tag, api, method, statusCode, reqlengthTag, resplengthTag).Observe(respTimeMs)
	}
	if ps.EnableXWarnCnt {
		if xwarns := entry.XWarns(); len(xwarns) != 0 {
			for _, hit := range hitXWarns(xwarns, ps.XWarns) {
				ps.xwarnCounter.WithLabelValues(hit, statusCode).Add(1)
			}
		}
	}
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

func getResponseCounterVec(logtype string, constLabels map[string]string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        logtype + "_response_code",
			Help:        logtype + " response code",
			ConstLabels: constLabels,
		},
		[]string{"tag", "api", "method", "code"},
	)
}

func getResponseErrCounterVec(logtype string, constLabels map[string]string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        logtype + "_response_err_code",
			Help:        logtype + " response err code",
			ConstLabels: constLabels,
		},
		[]string{"tag", "api", "method", "uid", "code"},
	)
}

func getRequestLengthCounterVec(logtype string, constLabels map[string]string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        logtype + "_request_length",
			Help:        logtype + " request length",
			ConstLabels: constLabels,
		},
		[]string{"tag", "api", "method", "code"},
	)
}

func getResponseLengthCounterVec(logtype string, constLabels map[string]string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        logtype + "_response_length",
			Help:        logtype + " response length",
			ConstLabels: constLabels,
		},
		[]string{"tag", "api", "method", "code"},
	)
}

func getResponseDurationVec(logtype string, constLabels map[string]string) *prometheus.HistogramVec {
	buckets := Buckets
	return prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:        logtype + "_response_duration_ms",
			Help:        logtype + " response duration ms",
			Buckets:     buckets,
			ConstLabels: constLabels,
		},
		[]string{"tag", "api", "method", "code", "reqlength", "resplength"},
	)
}

func getXwarnCountVec(constLabels map[string]string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "service_xwarn_count",
			Help:        "service xwarn count",
			ConstLabels: constLabels,
		},
		[]string{"xwarn", "code"},
	)
}
