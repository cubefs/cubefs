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
	"context"
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

	maxAuditWorkers = 50
	minAuditBufSize = 1000
)

type AuditLogger interface {
	Name() string
	Send(ctx context.Context, entry interface{}) error
	Close() error
}

type AuditEntry struct {
	Version   string    `json:"version"`
	Time      time.Time `json:"time"`
	RequestID string    `json:"request_id"`
	Requester string    `json:"requester"`
	AccessKey string    `json:"access_key"`
	Owner     string    `json:"owner,omitempty"`

	Request struct {
		API      string            `json:"api,omitempty"`
		Bucket   string            `json:"bucket,omitempty"`
		Object   string            `json:"object,omitempty"`
		Method   string            `json:"method,omitempty"`
		Proto    string            `json:"proto,omitempty"`
		Path     string            `json:"path,omitempty"`
		Query    map[string]string `json:"query,omitempty"`
		Header   map[string]string `json:"header,omitempty"`
		Host     string            `json:"host,omitempty"`
		RemoteIP string            `json:"remote_ip,omitempty"`
	} `json:"request"`
	Response struct {
		Status     string            `json:"status,omitempty"`
		StatusCode int               `json:"status_code,omitempty"`
		Header     map[string]string `json:"header,omitempty"`
		Error      string            `json:"error,omitempty"`
	} `json:"response"`

	BytesRequest  int64  `json:"bytes_req,omitempty"`
	BytesResponse int64  `json:"bytes_resp,omitempty"`
	Duration      string `json:"duration,omitempty"`
	DurationNS    string `json:"duration_ns,omitempty"`
}

type AuditsConfig struct {
	Local   *auditlog.Config              `json:"local,omitempty"`
	Kafka   map[string]AuditKafkaConfig   `json:"kafka,omitempty"`
	Webhook map[string]AuditWebhookConfig `json:"webhook,omitempty"`
}

type ExternalAudits struct {
	loggers []AuditLogger

	sync.RWMutex
}

func NewExternalAudits() *ExternalAudits {
	return &ExternalAudits{}
}

func (a *ExternalAudits) AddLoggers(al ...AuditLogger) {
	a.Lock()
	defer a.Unlock()

	a.loggers = append(a.loggers, al...)
}

func (a *ExternalAudits) Close() {
	var loggers []AuditLogger
	a.Lock()
	if a.loggers != nil {
		loggers = a.loggers[:]
		a.loggers = nil
	}
	a.Unlock()

	for _, logger := range loggers {
		logger.Close()
	}
}

func (a *ExternalAudits) getLoggers() []AuditLogger {
	a.RLock()
	defer a.RUnlock()

	return a.loggers[:]
}

func (a *ExternalAudits) Logger(w http.ResponseWriter, r *http.Request) {
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

	for _, logger := range loggers {
		if err := logger.Send(r.Context(), entry); err != nil {
			log.LogErrorf("send to external '%s' failed: %v", logger.Name(), err)
		}
	}
}
