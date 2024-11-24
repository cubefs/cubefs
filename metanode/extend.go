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

	"github.com/cubefs/cubefs/proto"

	"github.com/cubefs/cubefs/util/btree"
)

type ExtentVal struct {
	dataMap map[string][]byte
	verSeq  uint64
}

type ExtendMultiSnap struct {
	verSeq    uint64
	multiVers []*Extend
	versionMu sync.RWMutex
}

type Extend struct {
	inode     uint64
	dataMap   map[string][]byte
	Quota     []byte
	multiSnap *ExtendMultiSnap
	mu        sync.RWMutex
}

func (e *Extend) getVersion() uint64 {
	if e.multiSnap == nil {
		return 0
	}
	return e.multiSnap.verSeq
}

func (e *Extend) genSnap() {
	if e.multiSnap == nil {
		e.multiSnap = &ExtendMultiSnap{}
	}
	e.multiSnap.multiVers = append([]*Extend{e.Copy().(*Extend)}, e.multiSnap.multiVers...)
}

func (e *Extend) setVersion(seq uint64) {
	if e.multiSnap == nil {
		e.multiSnap = &ExtendMultiSnap{}
	}
	e.multiSnap.verSeq = seq
}

func (e *Extend) checkSequence() (err error) {
	if e.multiSnap == nil {
		return
	}
	e.multiSnap.versionMu.RLock()
	defer e.multiSnap.versionMu.RUnlock()

	lastSeq := e.getVersion()
	for id, extend := range e.multiSnap.multiVers {
		if lastSeq <= extend.getVersion() {
			return fmt.Errorf("id[%v] seq [%v] not less than last seq [%v]", id, extend.getVersion(), lastSeq)
		}
	}
	return
}

func (e *Extend) GetMinVer() uint64 {
	if e.multiSnap == nil {
		return 0
	}
	if len(e.multiSnap.multiVers) == 0 {
		return e.multiSnap.verSeq
	}
	return e.multiSnap.multiVers[len(e.multiSnap.multiVers)-1].getVersion()
}

func (e *Extend) GetExtentByVersion(ver uint64) (extend *Extend) {
	if ver == 0 {
		return e
	}
	if e.multiSnap == nil {
		return
	}
	if isInitSnapVer(ver) {
		if e.GetMinVer() != 0 {
			return nil
		}
		return e.multiSnap.multiVers[len(e.multiSnap.multiVers)-1]
	}
	e.multiSnap.versionMu.RLock()
	defer e.multiSnap.versionMu.RUnlock()
	for i := 0; i < len(e.multiSnap.multiVers)-1; i++ {
		if e.multiSnap.multiVers[i].getVersion() <= ver {
			return e.multiSnap.multiVers[i]
		}
	}
	return
}

func NewExtend(inode uint64) *Extend {
	return &Extend{inode: inode, dataMap: make(map[string][]byte)}
}

func NewExtendWithQuota(inode uint64) *Extend {
	return &Extend{inode: inode}
}

func NewExtendFromBytes(raw []byte) (*Extend, error) {
	var err error
	buffer := bytes.NewBuffer(raw)
	// decode inode
	var inode uint64
	if inode, err = binary.ReadUvarint(buffer); err != nil {
		return nil, err
	}
	ext := NewExtendWithQuota(inode)
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
		if string(k) == proto.QuotaKey {
			ext.Quota = v
			continue
		}
		if ext.dataMap == nil {
			ext.dataMap = make(map[string][]byte)
		}
		ext.Put(k, v, 0)
	}

	if buffer.Len() > 0 {
		// read verSeq
		verSeq, err := binary.ReadUvarint(buffer)
		if err != nil {
			return nil, err
		}

		// read number of multiVers
		numMultiVers, err := binary.ReadUvarint(buffer)
		if err != nil {
			return nil, err
		}
		if verSeq > 0 || numMultiVers > 0 {
			ext.setVersion(verSeq)
		}
		if numMultiVers > 0 {
			// read each multiVers
			ext.multiSnap.multiVers = make([]*Extend, numMultiVers)
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

				ext.multiSnap.multiVers[i] = mv
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
	if e.dataMap == nil {
		e.dataMap = make(map[string][]byte)
	}
	e.dataMap[string(key)] = value
	if verSeq > 0 {
		e.setVersion(verSeq)
	}
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
	if e.dataMap == nil {
		return
	}
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
	if e.dataMap == nil {
		e.dataMap = make(map[string][]byte)
	}
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
	newExt := NewExtendWithQuota(e.inode)
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.dataMap) > 0 {
		newExt.dataMap = make(map[string][]byte, len(e.dataMap))
		for k, v := range e.dataMap {
			newExt.dataMap[k] = v
		}
	}
	if e.multiSnap == nil {
		return newExt
	}
	newExt.Quota = make([]byte, len(e.Quota))
	copy(newExt.Quota, e.Quota)
	newExt.multiSnap = &ExtendMultiSnap{}
	newExt.multiSnap.verSeq = e.multiSnap.verSeq
	newExt.multiSnap.multiVers = e.multiSnap.multiVers
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
	pairCnt := len(e.dataMap)
	if len(e.Quota) > 0 {
		pairCnt = pairCnt + 1
	}
	n = binary.PutUvarint(tmp, uint64(pairCnt))
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
	if len(e.Quota) > 0 {
		// key
		if err = writeBytes([]byte(proto.QuotaKey)); err != nil {
			return nil, err
		}
		// value
		if err = writeBytes(e.Quota); err != nil {
			return nil, err
		}
	}

	if e.getVersion() > 0 {
		// write verSeq
		verSeqBytes := make([]byte, binary.MaxVarintLen64)
		verSeqLen := binary.PutUvarint(verSeqBytes, e.getVersion())
		if _, err = buffer.Write(verSeqBytes[:verSeqLen]); err != nil {
			return nil, err
		}

		// write number of multiVers
		n = binary.PutUvarint(tmp, uint64(len(e.multiSnap.multiVers)))
		if _, err = buffer.Write(tmp[:n]); err != nil {
			return nil, err
		}

		// write each multiVers
		for _, mv := range e.multiSnap.multiVers {
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
