/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package utils

import (
	"bytes"
	"compress/zlib"
	"github.com/apache/rocketmq-client-go/v2/errors"
	"io/ioutil"
	"sync"
)

var zlibWriterPools []sync.Pool

var bufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func init() {
	zlibWriterPools = make([]sync.Pool, zlib.BestCompression)
	for i := 0; i < zlib.BestCompression; i++ {
		compressLevel := i
		zlibWriterPools[i] = sync.Pool{
			New: func() interface{} {
				z, _ := zlib.NewWriterLevel(nil, compressLevel+1)
				return z
			},
		}
	}
}

func Compress(raw []byte, compressLevel int) ([]byte, error) {
	if compressLevel < zlib.BestSpeed || compressLevel > zlib.BestCompression {
		return nil, errors.ErrCompressLevel
	}

	buf := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(buf)
	writerPool := &zlibWriterPools[compressLevel-1]
	writer := writerPool.Get().(*zlib.Writer)
	defer writerPool.Put(writer)
	buf.Reset()
	writer.Reset(buf)
	_, e := writer.Write(raw)
	if e != nil {
		return nil, e
	}

	e = writer.Close()
	if e != nil {
		return nil, e
	}
	result := make([]byte, buf.Len())
	buf.Read(result)
	return result, nil
}

func UnCompress(data []byte) []byte {
	rdata := bytes.NewReader(data)
	r, err := zlib.NewReader(rdata)
	if err != nil {
		return data
	}
	retData, err := ioutil.ReadAll(r)
	if err != nil {
		return data
	}
	return retData
}
