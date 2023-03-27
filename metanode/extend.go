// Copyright 2018 The CubeFS Authors.
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

package metanode

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"

	"github.com/chubaofs/chubaofs/util/btree"
)

type Extend struct {
	inode   uint64
	dataMap map[string][]byte
	mu      sync.RWMutex
}
type ExtendBatch []*Extend

func NewExtend(inode uint64) *Extend {
	return &Extend{inode: inode, dataMap: make(map[string][]byte)}
}

func NewExtendFromBytes(raw []byte) (*Extend, error) {
	var err error
	var buffer = bytes.NewBuffer(raw)
	// decode inode
	var inode uint64
	if inode, err = binary.ReadUvarint(buffer); err != nil {
		return nil, err
	}
	var ext = NewExtend(inode)
	// decode number of key-value pairs
	var numKV uint64
	if numKV, err = binary.ReadUvarint(buffer); err != nil {
		return nil, err
	}
	var readBytes = func() ([]byte, error) {
		var length uint64
		if length, err = binary.ReadUvarint(buffer); err != nil {
			return nil, err
		}
		var data = make([]byte, length)
		if _, err = buffer.Read(data); err != nil {
			return nil, err
		}
		return data, nil
	}
	for i := 0; i < int(numKV); i++ {
		var k, v []byte
		if k, err = readBytes(); err != nil {
			return nil, err
		}
		if v, err = readBytes(); err != nil {
			return nil, err
		}
		ext.Put(k, v)
	}
	return ext, nil
}

func (e *Extend) Less(than btree.Item) bool {
	ext, is := than.(*Extend)
	return is && e.inode < ext.inode
}

func (e *Extend) Put(key, value []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dataMap[string(key)] = value
}

func (e *Extend) Get(key []byte) (value []byte, exist bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	value, exist = e.dataMap[string(key)]
	return
}

func (e *Extend) Remove(key []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.dataMap, string(key))
	return
}

func (e *Extend) Range(visitor func(key, value []byte) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	for k, v := range e.dataMap {
		if !visitor([]byte(k), v) {
			return
		}
	}
}

func (e *Extend) Merge(o *Extend, override bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	o.Range(func(key, value []byte) bool {
		strKey := string(key)
		if _, exist := e.dataMap[strKey]; override || !exist {
			copied := make([]byte, len(value))
			copy(copied, value)
			e.dataMap[strKey] = copied
		}
		return true
	})
}

func (e *Extend) Copy() btree.Item {
	newExt := NewExtend(e.inode)
	for k, v := range e.dataMap {
		newExt.dataMap[k] = v
	}
	return newExt
}

func (e *Extend) Bytes() ([]byte, error) {
	var err error
	e.mu.RLock()
	defer e.mu.RUnlock()
	var n int
	var tmp = make([]byte, binary.MaxVarintLen64)
	var buffer = bytes.NewBuffer(nil)
	// write inode with varint codec
	n = binary.PutUvarint(tmp, e.inode)
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	// write number of key-value pairs
	n = binary.PutUvarint(tmp, uint64(len(e.dataMap)))
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	// write key-value paris
	var writeBytes = func(val []byte) error {
		n = binary.PutUvarint(tmp, uint64(len(val)))
		if _, err = buffer.Write(tmp[:n]); err != nil {
			return err
		}
		if _, err = buffer.Write(val); err != nil {
			return err
		}
		return nil
	}
	sortedKeys := make([]string, 0)
	for k, _ := range e.dataMap{
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		// key
		if err = writeBytes([]byte(k)); err != nil {
			return nil, err
		}
		// value
		if err = writeBytes(e.dataMap[k]); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}
