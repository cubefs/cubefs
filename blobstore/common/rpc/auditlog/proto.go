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
	"context"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc2"
	"github.com/cubefs/cubefs/blobstore/util/largefile"
)

const (
	LogFormatText = "text"
	LogFormatJSON = "json"
)

type Config struct {
	// LogDir audit log whether to enable depend on whether config log dir
	LogDir string `json:"logdir"`
	// ChunkBits means one audit log file size
	// eg: chunkbits=20 means one log file will hold 1<<10 size
	ChunkBits uint `json:"chunkbits"`
	// BodyLimit negative means no body-cache, 0 means default buffer size.
	BodyLimit int `json:"bodylimit"`
	// No2xxBody means that the response body of 2xx will not be logged.
	No2xxBody bool `json:"no_2xx_body"`
	// rotate new audit log file every start time
	RotateNew     bool   `json:"rotate_new"`
	LogFileSuffix string `json:"log_file_suffix"`
	// 0 means no backup limit
	Backup       int              `json:"backup"`
	MetricConfig PrometheusConfig `json:"metric_config"`

	// Filters are or relations
	Filters       []FilterConfig `json:"filters"`
	MetricsFilter bool           `json:"metrics_filter"`

	// LogFormat valid value is "text" or "json", default is "text"
	LogFormat string `json:"log_format"`

	// LogAsyncQueueSize controls asynchronous writing.
	//  0 (default): disabled, wait for the disk, same as the historical behavior.
	//  < 0: enabled, queue size is the default 4096.
	//  > 0: enabled, this value is the queue size; values above 1<<20 are
	//       clamped to the max with a warning.
	LogAsyncQueueSize int `json:"log_async_queue_size"`
	// LogAsyncDrop changes the queue-full behavior from blocking
	// (backpressure, nothing is dropped) to dropping the line so the
	// business never waits for the audit disk.
	// Default false: do not drop, compatible with the historical writer.
	// Request goroutines may be dragged by a hung audit disk just like in
	// sync mode. Only takes effect when LogAsyncQueueSize != 0.
	LogAsyncDrop bool `json:"log_async_drop"`
}

// LogCloser a implemented audit logger should implements ProgressHandler
// and LogCloser interface to replace qn audit log
type LogCloser interface {
	Close() error
	Log([]byte) error
}

type noopLogCloser struct{}

var NoopLogCloser largefile.OwnedLogCloser = noopLogCloser{}

func (noopLogCloser) Close() error     { return nil }
func (noopLogCloser) Log([]byte) error { return nil }

// LogOwned implements the owned-buffer part of largefile.OwnedLogCloser:
// nothing is written, but Release is still required, otherwise the caller's
// pooled buffer would stay checked out forever.
func (noopLogCloser) LogOwned(line largefile.BufferOwned) error {
	line.Release()
	return nil
}

type MetricSender interface {
	Send(raw []byte) error
	SendEntry(LogEntry)
}

type Decoder interface {
	DecodeReq(req *http.Request) *DecodedReq
	DecodeReq2(req *rpc2.Request) *DecodedReq
}

type ResponseExtraHeader interface {
	ExtraHeader() http.Header
}

type Auditor interface {
	Audit(ctx context.Context, handler func(ctx context.Context, auditline *AuditLog) error) error
}

type AuditHandler interface {
	rpc.ProgressHandler
	rpc2.Interceptor
	Auditor
}

type noopAuditHandler struct{}

var NoopAuditHandler AuditHandler = noopAuditHandler{}

func (noopAuditHandler) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	f(w, req)
}

func (noopAuditHandler) Handle(w rpc2.ResponseWriter, req *rpc2.Request, f rpc2.Handle) error {
	return f(w, req)
}

func (noopAuditHandler) Audit(ctx context.Context, handler func(context.Context, *AuditLog) error) error {
	return handler(ctx, &AuditLog{})
}
