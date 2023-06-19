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
	"strconv"
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
	ReqType    string `json:"req_type"`
	Module     string `json:"module"`
	StartTime  int64  `json:"start_time"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	ReqHeader  M      `json:"req_header"`
	ReqParams  string `json:"req_params"`
	StatusCode int    `json:"status_code"`
	RespHeader M      `json:"resp_header"`
	RespBody   string `json:"resp_body"`
	RespLength int64  `json:"resp_length"`
	Duration   int64  `json:"duration"`
}

func (a *AuditLog) ToBytesWithTab(buf *bytes.Buffer) (b []byte) {
	buf.WriteString(a.ReqType)
	buf.WriteByte('\t')
	buf.WriteString(a.Module)
	buf.WriteByte('\t')
	buf.WriteString(strconv.FormatInt(a.StartTime, 10))
	buf.WriteByte('\t')
	buf.WriteString(a.Method)
	buf.WriteByte('\t')
	buf.WriteString(a.Path)
	buf.WriteByte('\t')
	buf.Write(a.ReqHeader.Encode())
	buf.WriteByte('\t')
	buf.WriteString(a.ReqParams)
	buf.WriteByte('\t')
	buf.WriteString(strconv.Itoa(a.StatusCode))
	buf.WriteByte('\t')
	buf.Write(a.RespHeader.Encode())
	buf.WriteByte('\t')
	buf.WriteString(a.RespBody)
	buf.WriteByte('\t')
	buf.WriteString(strconv.FormatInt(a.RespLength, 10))
	buf.WriteByte('\t')
	buf.WriteString(strconv.FormatInt(a.Duration, 10))
	buf.WriteByte('\n')
	return buf.Bytes()
}

func (a *AuditLog) ToJson() (b []byte) {
	b, _ = json.Marshal(a)
	return
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
	var (
		logBytes []byte
		err      error
	)
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
	b := j.logPool.Get().(*bytes.Buffer)
	defer j.logPool.Put(b)
	b.Reset()

	auditLog := &AuditLog{
		ReqType:   "REQ",
		Module:    j.module,
		StartTime: startTime / 100,
		Method:    req.Method,
		Path:      decodeReq.Path,
		ReqHeader: decodeReq.Header,
	}

	if len(decodeReq.Params) <= maxSeekableBodyLength && len(decodeReq.Params) > 0 {
		auditLog.ReqParams = string(decodeReq.Params)
	}

	// record response info
	respContentType := _w.Header().Get("Content-Type")
	auditLog.StatusCode = _w.getStatusCode()

	// Check if track-log and tags changed or not,
	// if changed, we should set into response header again.
	// But the additional headers DO NOT write to client if
	// they set after response WriteHeader, just logging.
	wHeader := _w.Header()
	traceLogs := span.TrackLog()
	if len(wHeader[rpc.HeaderTraceLog]) < len(traceLogs) {
		wHeader[rpc.HeaderTraceLog] = traceLogs
	}
	tags := span.Tags().ToSlice()
	if len(wHeader[rpc.HeaderTraceTags]) < len(tags) {
		wHeader[rpc.HeaderTraceTags] = tags
	}
	auditLog.RespHeader = _w.getHeader()

	if (respContentType == rpc.MIMEJSON || respContentType == rpc.MIMEXML) &&
		_w.Header().Get("Content-Encoding") != rpc.GzipEncodingType {
		auditLog.RespBody = string(_w.getBody())
	}

	auditLog.RespLength = _w.getBodyWritten()
	auditLog.Duration = endTime - startTime/1000

	j.metricSender.Send(auditLog.ToBytesWithTab(b))

	if j.logFile == nil || (len(j.cfg.KeywordsFilter) > 0 && defaultLogFilter(req, j.cfg.KeywordsFilter)) {
		return
	}

	switch j.cfg.LogFormat {
	case LogFormatText:
		logBytes = auditLog.ToBytesWithTab(b)
	case LogFormatJSON:
		logBytes = auditLog.ToJson()
	default:
		logBytes = auditLog.ToBytesWithTab(b)
	}
	err = j.logFile.Log(logBytes)
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
