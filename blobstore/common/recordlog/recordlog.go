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

package recordlog

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/cubefs/cubefs/blobstore/util/largefile"
)

const DefaultBufferSizeKB = 16

type Encoder interface {
	Encode(v interface{}) error
	Close() error
}

//----------------------
//nop encoder
type NopEncoder struct{}

func (e *NopEncoder) Encode(v interface{}) error {
	return nil
}

func (e *NopEncoder) Close() error {
	return nil
}

//----------------------------------
// recode log encode
type Config struct {
	Dir       string `json:"dir"`
	ChunkBits uint   `json:"chunkbits"`
}

type RecordLog struct {
	*json.Encoder

	f        largefile.LargeFile
	roff     int64
	readLock sync.Mutex
}

func NewEncoder(conf *Config) (e Encoder, err error) {
	if conf == nil {
		return &NopEncoder{}, nil
	}
	var f largefile.LargeFile
	conf2 := largefile.Config{
		Path:              conf.Dir,
		FileChunkSizeBits: conf.ChunkBits,
		Suffix:            ".log",
	}

	f, err = largefile.OpenLargeFile(conf2)
	if err != nil {
		return
	}

	rlw := &RecordLog{f: f}
	rlw.Encoder = json.NewEncoder(rlw)
	return rlw, nil
}

func (rl *RecordLog) Write(p []byte) (n int, err error) {
	return rl.f.Write(p)
}

func (rl *RecordLog) Read(p []byte) (n int, err error) {
	rl.readLock.Lock()
	defer rl.readLock.Unlock()
	n, err = rl.f.ReadAt(p, rl.roff)
	rl.roff += int64(n)
	return
}

func (rl *RecordLog) Close() error {
	return rl.f.Close()
}

//-------------------------------
type Decoder interface {
	Decode(v interface{}) error
}

type RecordDecoder struct {
	*RecordLog
	r *bufio.Reader
}

func NewDecoder(conf *Config) (d Decoder, err error) {
	if conf == nil {
		return nil, errors.New("conf is nil")
	}

	conf2 := largefile.Config{
		Path:              conf.Dir,
		FileChunkSizeBits: conf.ChunkBits,
		Suffix:            ".log",
	}

	f, err := largefile.OpenLargeFile(conf2)
	if err != nil {
		return
	}

	rlw := &RecordLog{f: f}
	return &RecordDecoder{r: bufio.NewReaderSize(rlw, DefaultBufferSizeKB*1024), RecordLog: rlw}, nil
}

func (e *RecordDecoder) Decode(v interface{}) error {
	line, _, err := e.r.ReadLine()
	if err == nil || err == io.EOF && len(line) > 0 {
		err2 := json.Unmarshal(line, v)
		return err2
	}
	return err
}
