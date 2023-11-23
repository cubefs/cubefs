// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc/auditlog"
	"github.com/cubefs/cubefs/util/log"
)

const (
	auditVersion = "1.0"
)

type AuditLogger interface {
	Name() string
	Send(data []byte) error
	Close() error
}

type AuditEntry struct {
	Version   string    `json:"Version"`
	Time      time.Time `json:"Time"`
	RequestID string    `json:"RequestID"`
	Requester string    `json:"Requester"`
	AccessKey string    `json:"AccessKey"`
	Owner     string    `json:"Owner"`

	Request struct {
		API      string            `json:"API,omitempty"`
		Bucket   string            `json:"Bucket,omitempty"`
		Object   string            `json:"Object,omitempty"`
		Method   string            `json:"Method,omitempty"`
		Proto    string            `json:"Proto,omitempty"`
		Path     string            `json:"Path,omitempty"`
		Query    map[string]string `json:"Query,omitempty"`
		Header   map[string]string `json:"Header,omitempty"`
		Host     string            `json:"Host,omitempty"`
		RemoteIP string            `json:"RemoteIP,omitempty"`
	} `json:"Request"`

	Response struct {
		Status     string            `json:"Status,omitempty"`
		StatusCode int               `json:"StatusCode,omitempty"`
		Header     map[string]string `json:"Header,omitempty"`
		Error      string            `json:"Error,omitempty"`
	} `json:"Response"`

	BytesRequest  int64  `json:"BytesRequest,omitempty"`
	BytesResponse int64  `json:"BytesResponse,omitempty"`
	Duration      string `json:"Duration,omitempty"`
	DurationNS    string `json:"DurationNS,omitempty"`
}

type AuditLogConfig struct {
	Local *auditlog.Config `json:"local,omitempty"`
	// The key of map is a unique identifier of audit
	Kafka   map[string]KafkaAuditConfig   `json:"kafka,omitempty"`
	Webhook map[string]WebhookAuditConfig `json:"webhook,omitempty"`
}

type ExternalAudit struct {
	loggers []AuditLogger

	sync.RWMutex
}

func NewExternalAudit() *ExternalAudit {
	return &ExternalAudit{}
}

func (a *ExternalAudit) AddLoggers(al ...AuditLogger) {
	a.Lock()
	defer a.Unlock()

	a.loggers = append(a.loggers, al...)
}

func (a *ExternalAudit) Close() {
	a.Lock()
	loggers := a.loggers
	a.loggers = nil
	a.Unlock()

	for _, logger := range loggers {
		if logger != nil {
			logger.Close()
		}
	}
}

func (a *ExternalAudit) getLoggers() []AuditLogger {
	a.RLock()
	defer a.RUnlock()

	return a.loggers
}

func (a *ExternalAudit) Logger(w http.ResponseWriter, r *http.Request) {
	loggers := a.getLoggers()
	if len(loggers) <= 0 || w == nil || r == nil {
		return
	}

	var (
		statusCode int
		respBytes  int64
		timeCost   time.Duration
	)
	startTime := time.Now().UTC()
	if rs, ok := w.(*ResponseStater); ok {
		statusCode = rs.StatusCode
		respBytes = rs.Written
		startTime = rs.StartTime
		timeCost = time.Now().UTC().Sub(startTime)
	}
	duration := strconv.FormatInt(timeCost.Nanoseconds(), 10)
	param := ParseRequestParam(r)
	entry := AuditEntry{
		Version:       auditVersion,
		Time:          startTime,
		RequestID:     param.RequestID(),
		Requester:     param.Requester(),
		AccessKey:     param.AccessKey(),
		Owner:         param.Owner(),
		BytesRequest:  r.ContentLength,
		BytesResponse: respBytes,
		DurationNS:    duration,
		Duration:      duration + "ns",
	}

	entry.Request.API = param.API()
	entry.Request.Bucket = param.Bucket()
	entry.Request.Object = param.Object()
	entry.Request.Method = r.Method
	entry.Request.Proto = r.Proto
	entry.Request.Path = r.URL.Path
	entry.Request.Host = r.Host
	entry.Request.RemoteIP = getRequestIP(r)
	query := r.URL.Query()
	reqQuery := make(map[string]string, len(query))
	for k, v := range query {
		reqQuery[k] = strings.Join(v, ",")
	}
	entry.Request.Query = reqQuery
	header := make(map[string]string, len(r.Header))
	for k, v := range r.Header {
		header[k] = strings.Join(v, ",")
	}
	entry.Request.Header = header

	rh := w.Header()
	header = make(map[string]string, len(rh))
	for k, v := range rh {
		header[k] = strings.Join(v, ",")
	}
	entry.Response.Header = header
	entry.Response.StatusCode = statusCode
	entry.Response.Status = http.StatusText(statusCode)
	entry.Response.Error = getResponseErrorMessage(r)

	data, err := json.Marshal(entry)
	if err != nil {
		log.LogErrorf("audit entry json marshal failed: %v", err)
		return
	}

	for _, logger := range loggers {
		if logger != nil {
			go func(logger AuditLogger) {
				if err = logger.Send(data); err != nil {
					log.LogErrorf("send to external '%s' failed: %v", logger.Name(), err)
				}
			}(logger)
		}
	}
}
