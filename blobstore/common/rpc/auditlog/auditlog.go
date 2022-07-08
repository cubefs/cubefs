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
	"net/http"
	"runtime/debug"
	"strconv"
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

func Open(module string, cfg *Config) (rpc.ProgressHandler, LogCloser, error) {
	if cfg.BodyLimit == 0 {
		cfg.BodyLimit = defaultReadBodyBuffLength
	}
	if cfg.BodyLimit > maxReadBodyBuffLength {
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
	logFile, err := largefile.OpenLargeFileLog(largeLogConfig, cfg.RotateNew)
	if err != nil {
		return nil, nil, errors.Info(err, "auditlog.Open: large file log open failed").Detail(err)
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

	b.WriteString("REQ\t")
	b.WriteString(j.module)
	b.WriteByte('\t')

	// record request info
	decodeReq := j.decoder.DecodeReq(req)
	b.WriteString(strconv.FormatInt(startTime/100, 10))
	b.WriteByte('\t')
	b.WriteString(req.Method)
	b.WriteByte('\t')
	b.WriteString(decodeReq.Path)
	b.WriteByte('\t')
	b.Write(decodeReq.Header.Encode())
	b.WriteByte('\t')
	if len(decodeReq.Params) < 1024 {
		b.Write(decodeReq.Params)
	}
	b.WriteByte('\t')

	// record response info
	respContentType := _w.Header().Get("Content-Type")
	b.Write(_w.getStatusCode())
	b.WriteByte('\t')
	// check if track log change or not,
	// if change, we should set into response header again
	traceLogs := span.TrackLog()
	wHeader := _w.Header()
	if len(wHeader[rpc.HeaderTraceLog]) < len(traceLogs) {
		wHeader[rpc.HeaderTraceLog] = traceLogs
	}
	b.Write(_w.getHeader())
	b.WriteByte('\t')
	// record body in json or xml content type
	if (respContentType == rpc.MIMEJSON || respContentType == rpc.MIMEXML) &&
		_w.Header().Get("Content-Encoding") != rpc.GzipEncodingType {
		b.Write(_w.getBody())
	}
	b.WriteByte('\t')
	b.WriteString(strconv.FormatInt(_w.getBodyWritten(), 10))
	b.WriteByte('\t')
	b.WriteString(strconv.FormatInt(endTime-startTime/1000, 10))
	b.WriteByte('\n')

	err := j.logFile.Log(b.Bytes())
	if err != nil {
		span.Errorf("jsonlog.Handler Log failed, err: %s", err.Error())
		return
	}
	// report request metric
	j.metricSender.Send(b.Bytes())
}
