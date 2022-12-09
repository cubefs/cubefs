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

package primitive

import (
	"bytes"
	"sync"
)

var headerPool = sync.Pool{}
var bufferPool = sync.Pool{}

func init() {
	headerPool.New = func() interface{} {
		b := make([]byte, 4)
		return &b
	}
	bufferPool.New = func() interface{} {
		return new(bytes.Buffer)
	}
}

func GetHeader() *[]byte {
	d := headerPool.Get().(*[]byte)
	//d = (d)[:0]
	return d
}

func BackHeader(d *[]byte) {
	headerPool.Put(d)
}

func GetBuffer() *bytes.Buffer {
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func BackBuffer(b *bytes.Buffer) {
	b.Reset()
	bufferPool.Put(b)
}
