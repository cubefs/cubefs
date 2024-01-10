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
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/util/btree"
)

type ExtentVal struct {
	dataMap map[string][]byte
	verSeq  uint64
}

type Extend struct {
	inode     uint64
	dataMap   map[string][]byte
	verSeq    uint64
	multiVers []*Extend
	versionMu sync.RWMutex
	mu        sync.RWMutex
}

func (e *Extend) checkSequence() (err error) {
	e.versionMu.RLock()
	defer e.versionMu.RUnlock()

	lastSeq := e.verSeq
	for id, extend := range e.multiVers {
		if lastSeq <= extend.verSeq {
			return fmt.Errorf("id[%v] seq [%v] not less than last seq [%v]", id, extend.verSeq, lastSeq)
		}
	}
	return
}

func (e *Extend) GetMinVer() uint64 {
	if len(e.multiVers) == 0 {
		return e.verSeq
	}
	return e.multiVers[len(e.multiVers)-1].verSeq
}

func (e *Extend) GetExtentByVersion(ver uint64) (extend *Extend) {
	if ver == 0 {
		return e
	}
	if isInitSnapVer(ver) {
		if e.GetMinVer() != 0 {
			return nil
		}
		return e.multiVers[len(e.multiVers)-1]
	}
	e.versionMu.RLock()
	defer e.versionMu.RUnlock()
	for i := 0; i < len(e.multiVers)-1; i++ {
		if e.multiVers[i].verSeq <= ver {
			return e.multiVers[i]
		}
	}
	return
}

func NewExtend(inode uint64) *Extend {
	return &Extend{inode: inode, dataMap: make(map[string][]byte)}
}

func NewExtendFromBytes(raw []byte) (*Extend, error) {
	var err error
	buffer := bytes.NewBuffer(raw)
	// decode inode
	var inode uint64
	if inode, err = binary.ReadUvarint(buffer); err != nil {
		return nil, err
	}
	ext := NewExtend(inode)
	// decode number of key-value pairs
	var numKV uint64
	if numKV, err = binary.ReadUvarint(buffer); err != nil {
		return nil, err
	}
	readBytes := func() ([]byte, error) {
		var length uint64
		if length, err = binary.ReadUvarint(buffer); err != nil {
			return nil, err
		}
		data := make([]byte, length)
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
		ext.Put(k, v, 0)
	}

	if buffer.Len() > 0 {
		// read verSeq
		verSeq, err := binary.ReadUvarint(buffer)
		if err != nil {
			return nil, err
		}
		ext.verSeq = verSeq

		// read number of multiVers
		numMultiVers, err := binary.ReadUvarint(buffer)
		if err != nil {
			return nil, err
		}
		if numMultiVers > 0 {
			// read each multiVers
			ext.multiVers = make([]*Extend, numMultiVers)
			for i := uint64(0); i < numMultiVers; i++ {
				// read multiVers length
				mvLen, err := binary.ReadUvarint(buffer)
				if err != nil {
					return nil, err
				}
				mvBytes := make([]byte, mvLen)
				if _, err = buffer.Read(mvBytes); err != nil {
					return nil, err
				}

				// recursively decode multiVers
				mv, err := NewExtendFromBytes(mvBytes)
				if err != nil {
					return nil, err
				}

				ext.multiVers[i] = mv
			}
		}
	}
	return ext, nil
}

func (e *Extend) Less(than btree.Item) bool {
	ext, is := than.(*Extend)
	return is && e.inode < ext.inode
}

func (e *Extend) Put(key, value []byte, verSeq uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dataMap[string(key)] = value
	e.verSeq = verSeq
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
	e.mu.RLock()
	defer e.mu.RUnlock()
	for k, v := range e.dataMap {
		newExt.dataMap[k] = v
	}
	newExt.verSeq = e.verSeq
	newExt.multiVers = e.multiVers
	return newExt
}

func (e *Extend) Bytes() ([]byte, error) {
	var err error
	e.mu.RLock()
	defer e.mu.RUnlock()
	var n int
	tmp := make([]byte, binary.MaxVarintLen64)
	buffer := bytes.NewBuffer(nil)
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
	writeBytes := func(val []byte) error {
		n = binary.PutUvarint(tmp, uint64(len(val)))
		if _, err = buffer.Write(tmp[:n]); err != nil {
			return err
		}
		if _, err = buffer.Write(val); err != nil {
			return err
		}
		return nil
	}
	for k, v := range e.dataMap {
		// key
		if err = writeBytes([]byte(k)); err != nil {
			return nil, err
		}
		// value
		if err = writeBytes(v); err != nil {
			return nil, err
		}
	}

	if e.verSeq > 0 {
		// write verSeq
		verSeqBytes := make([]byte, binary.MaxVarintLen64)
		verSeqLen := binary.PutUvarint(verSeqBytes, e.verSeq)
		if _, err = buffer.Write(verSeqBytes[:verSeqLen]); err != nil {
			return nil, err
		}

		// write number of multiVers
		n = binary.PutUvarint(tmp, uint64(len(e.multiVers)))
		if _, err = buffer.Write(tmp[:n]); err != nil {
			return nil, err
		}

		// write each multiVers
		for _, mv := range e.multiVers {
			// write multiVers bytes
			mvBytes, err := mv.Bytes()
			if err != nil {
				return nil, err
			}
			// write multiVers length
			n = binary.PutUvarint(tmp, uint64(len(mvBytes)))
			if _, err = buffer.Write(tmp[:n]); err != nil {
				return nil, err
			}
			// write multiVers bytes
			if _, err = buffer.Write(mvBytes); err != nil {
				return nil, err
			}
		}

		return buffer.Bytes(), nil
	}
	return buffer.Bytes(), nil
}

func (e *Extend) GetInode() (inode uint64) {
	return e.inode
}
