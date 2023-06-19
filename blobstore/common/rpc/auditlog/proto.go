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

import "net/http"

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
	// rotate new audit log file every start time
	RotateNew     bool   `json:"rotate_new"`
	LogFileSuffix string `json:"log_file_suffix"`
	// 0 means no backup limit
	Backup       int              `json:"backup"`
	MetricConfig PrometheusConfig `json:"metric_config"`

	// KeywordsFilter log filter based on uri and request method
	KeywordsFilter []string `json:"keywords_filter"`

	// LogFormat valid value is "text" or "json", default is "text"
	LogFormat string `json:"log_format"`
}

// LogCloser a implemented audit logger should implements ProgressHandler
// and LogCloser interface to replace qn audit log
type LogCloser interface {
	Close() error
	Log([]byte) error
}

type noopLogCloser struct{}

var _ LogCloser = noopLogCloser{}

func (noopLogCloser) Close() error     { return nil }
func (noopLogCloser) Log([]byte) error { return nil }

type MetricSender interface {
	Send(raw []byte) error
}

type Decoder interface {
	DecodeReq(req *http.Request) *DecodedReq
}
