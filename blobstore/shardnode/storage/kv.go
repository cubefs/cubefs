// Copyright 2024 The CubeFS Authors.
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

package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	apierr "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

const (
	keyFieldSize   = 2
	valueFieldSize = 4
)

type kv struct {
	buff []byte
}

func newKV(buff []byte) *kv {
	return &kv{
		buff: buff,
	}
}

func initKV(key []byte, value *io.LimitedReader) (*kv, error) {
	if value.N > MaxValueSize {
		return nil, apierr.ErrValueSizeTooLarge
	}
	buf := bytespool.Alloc(keyFieldSize + valueFieldSize + int(len(key)) + int(value.N))
	n := 0
	// key
	binary.BigEndian.PutUint16(buf[n:], uint16(len(key)))
	n += keyFieldSize

	copy(buf[n:n+len(key)], key)
	n += len(key)

	// value
	valueSize := value.N
	binary.BigEndian.PutUint32(buf[n:], uint32(valueSize))
	n += valueFieldSize
	if value.R != nil {
		_n, err := io.ReadFull(value, buf[n:n+int(valueSize)])
		if err != nil {
			return nil, err
		}
		if _n != int(valueSize) {
			return nil, fmt.Errorf("incorrect number of bytes read (%d != %d)", _n, int(value.N))
		}
	}

	return &kv{
		buff: buf,
	}, nil
}

func (e *kv) Release() {
	bytespool.Free(e.buff)
}

func (e *kv) Marshal() []byte {
	return e.buff
}

func (e *kv) Key() []byte {
	return e.buff[keyFieldSize : keyFieldSize+e.keySize()]
}

func (e *kv) Value() []byte {
	idx := keyFieldSize + e.keySize() + valueFieldSize
	return e.buff[idx : idx+e.valueSize()]
}

func (e *kv) keySize() int {
	return int(binary.BigEndian.Uint16(e.buff[0:keyFieldSize]))
}

func (e *kv) valueSize() int {
	start := keyFieldSize + e.keySize()
	return int(binary.BigEndian.Uint32(e.buff[start : start+valueFieldSize]))
}
