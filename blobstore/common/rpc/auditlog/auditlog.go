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
	"bytes"
	"encoding/json"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/largefile"
)

const (
	defaultReadBodyBuffLength = 512
	maxReadBodyBuffLength     = 2048
	defaultFileChunkBits      = 29
)

type jsonAuditlog struct {
	module       string
	decoder      Decoder
	metricSender MetricSender
	logFile      LogCloser

	logPool  sync.Pool
	bodyPool sync.Pool

	cfg *Config
}

// AuditLog Define a struct to represent the structured log data
type AuditLog struct {
	ReqType     string `json:"req_type"`
	Module      string `json:"module"`
	StartTime   int64  `json:"start_time"`
	Method      string `json:"method"`
	Path        string `json:"path"`
	ReqHeader   M      `json:"req_header"`
	ReqParams   M      `json:"req_params"`
	StatusCode  int    `json:"status_code"`
	RespHeader  M      `json:"resp_header"`
	RespBody    string `json:"resp_body"`
	BodyWritten int64  `json:"body_written"`
	Duration    int64  `json:"duration"`
}

func Open(module string, cfg *Config) (ph rpc.ProgressHandler, logFile LogCloser, err error) {
	if cfg.BodyLimit < 0 {
		cfg.BodyLimit = 0
	} else if cfg.BodyLimit == 0 {
		cfg.BodyLimit = defaultReadBodyBuffLength
	} else if cfg.BodyLimit > maxReadBodyBuffLength {
		cfg.BodyLimit = maxReadBodyBuffLength
	}

	if cfg.ChunkBits == 0 {
		cfg.ChunkBits = defaultFileChunkBits
	}

	largeLogConfig := largefile.Config{
		Path:              cfg.LogDir,
		FileChunkSizeBits: cfg.ChunkBits,
		Suffix:            cfg.LogFileSuffix,
		Backup:            cfg.Backup,
	}

	logFile = noopLogCloser{}
	if cfg.LogDir != "" {
		logFile, err = largefile.OpenLargeFileLog(largeLogConfig, cfg.RotateNew)
		if err != nil {
			return nil, nil, errors.Info(err, "auditlog.Open: large file log open failed").Detail(err)
		}
	}

	return &jsonAuditlog{
		module:       module,
		decoder:      &defaultDecoder{},
		metricSender: NewPrometheusSender(cfg.MetricConfig),
		logFile:      logFile,

		logPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
		bodyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, cfg.BodyLimit)
			},
		},
		cfg: cfg,
	}, logFile, nil
}

func (j *jsonAuditlog) Handler(w http.ResponseWriter, req *http.Request, f func(http.ResponseWriter, *http.Request)) {
	startTime := time.Now().UnixNano()

	span, ctx := trace.StartSpanFromHTTPHeaderSafe(req, j.module)
	defer span.Finish()
	_w := &responseWriter{
		module:         j.module,
		body:           j.bodyPool.Get().([]byte),
		bodyLimit:      j.cfg.BodyLimit,
		span:           span,
		startTime:      time.Now(),
		ResponseWriter: w,
	}
	req = req.WithContext(ctx)

	// parse request to decodeRep
	decodeReq := j.decoder.DecodeReq(req)

	// handle panic recover, return 597 status code
	defer func() {
		j.bodyPool.Put(_w.body) // nolint: staticcheck

		p := recover()
		if p != nil {
			span.Printf("WARN: panic fired in %v.panic - %v\n", f, p)
			span.Println(string(debug.Stack()))
			w.WriteHeader(597)
		}
	}()
	f(_w, req)

	endTime := time.Now().UnixNano() / 1000

	// Create a new AuditLog instance
	auditLog := &AuditLog{
		ReqType:   "REQ",
		Module:    j.module,
		StartTime: startTime / 100,
		Method:    req.Method,
		Path:      decodeReq.Path,
		ReqHeader: decodeReq.Header,
		ReqParams: decodeReq.Params,
	}

	// Update response fields in the AuditLog instance
	auditLog.StatusCode = _w.getStatusCode()
	auditLog.RespHeader = _w.getHeader()

	// record body in json or xml content type
	respContentType := _w.Header().Get("Content-Type")
	if (respContentType == rpc.MIMEJSON || respContentType == rpc.MIMEXML) &&
		_w.Header().Get("Content-Encoding") != rpc.GzipEncodingType {
		auditLog.RespBody = string(_w.getBody())
	}

	auditLog.BodyWritten = _w.getBodyWritten()
	auditLog.Duration = endTime - startTime/1000

	// Serialize the AuditLog instance as JSON
	auditLogJSON, err := json.Marshal(auditLog)
	if err != nil {
		span.Errorf("jsonlog.Handler JSON serialization failed, err: %s", err.Error())
		return
	}

	// Write the JSON data to the log file
	err = j.logFile.Log(auditLogJSON)
	if err != nil {
		span.Errorf("jsonlog.Handler Log failed, err: %s", err.Error())
		return
	}
}

// defaultLogFilter support uri and method filter based on keywords
func defaultLogFilter(r *http.Request, words []string) bool {
	method := strings.ToLower(r.Method)
	for _, word := range words {
		str := strings.ToLower(word)
		if method == str || strings.Contains(r.RequestURI, str) {
			return true
		}
	}
	return false
}
