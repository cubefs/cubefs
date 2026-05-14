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

// ExtendMultiSnap represents a multi-version snapshot
type ExtendMultiSnap struct {
	verSeq    uint64
	multiVers []*Extend
	versionMu sync.RWMutex
}

// Extend represents an extended attribute with multi-version support
type Extend struct {
	inode     uint64
	dataMap   map[string][]byte
	Quota     []byte
	multiSnap *ExtendMultiSnap
	mu        sync.RWMutex
}

// getVersion returns the current version sequence
func (e *Extend) getVersion() uint64 {
	if e.multiSnap == nil {
		return 0
	}
	return e.multiSnap.verSeq
}

// genSnap generates a new snapshot
func (e *Extend) genSnap() {
	if e.multiSnap == nil {
		e.multiSnap = &ExtendMultiSnap{}
	}
	// Create a deep copy for snapshot
	snapshot := e.deepCopy()
	e.multiSnap.multiVers = append([]*Extend{snapshot}, e.multiSnap.multiVers...)
}

// setVersion sets the version sequence
func (e *Extend) setVersion(seq uint64) {
	if e.multiSnap == nil {
		e.multiSnap = &ExtendMultiSnap{}
	}
	e.multiSnap.verSeq = seq
}

// checkSequence validates the version sequence
func (e *Extend) checkSequence() error {
	if e.multiSnap == nil {
		return nil
	}

	e.multiSnap.versionMu.RLock()
	defer e.multiSnap.versionMu.RUnlock()

	lastSeq := e.getVersion()
	for id, extend := range e.multiSnap.multiVers {
		if lastSeq <= extend.getVersion() {
			return fmt.Errorf("version sequence validation failed: id[%d] seq[%d] not less than last seq[%d]",
				id, extend.getVersion(), lastSeq)
		}
	}
	return nil
}

// GetMinVer returns the minimum version
func (e *Extend) GetMinVer() uint64 {
	if e.multiSnap == nil {
		return 0
	}
	if len(e.multiSnap.multiVers) == 0 {
		return e.multiSnap.verSeq
	}
	return e.multiSnap.multiVers[len(e.multiSnap.multiVers)-1].getVersion()
}

// GetExtentByVersion returns the extend for a specific version
func (e *Extend) GetExtentByVersion(ver uint64) *Extend {
	if ver == 0 {
		return e
	}
	if e.multiSnap == nil {
		return nil
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
	return nil
}

// NewExtend creates a new Extend instance
func NewExtend(inode uint64) *Extend {
	return &Extend{
		inode:   inode,
		dataMap: make(map[string][]byte),
	}
}

// NewExtendWithQuota creates a new Extend instance with quota support
func NewExtendWithQuota(inode uint64) *Extend {
	return &Extend{
		inode: inode,
	}
}

// NewExtendFromBytes deserializes an Extend from byte array
func NewExtendFromBytes(raw []byte) (*Extend, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty data provided")
	}

	buffer := bytes.NewBuffer(raw)

	// Decode inode
	inode, err := binary.ReadUvarint(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read inode: %w", err)
	}

	ext := NewExtendWithQuota(inode)

	// Decode number of key-value pairs
	numKV, err := binary.ReadUvarint(buffer)
	if err != nil {
		return nil, fmt.Errorf("failed to read key-value count: %w", err)
	}

	// Read key-value pairs
	if err := ext.decodeKeyValuePairs(buffer, numKV); err != nil {
		return nil, fmt.Errorf("failed to decode key-value pairs: %w", err)
	}

	// Read version information if available
	if buffer.Len() > 0 {
		if err := ext.decodeVersionInfo(buffer); err != nil {
			return nil, fmt.Errorf("failed to decode version info: %w", err)
		}
	}

	return ext, nil
}

// decodeKeyValuePairs decodes key-value pairs from buffer
func (e *Extend) decodeKeyValuePairs(buffer *bytes.Buffer, numKV uint64) error {
	readBytes := func() ([]byte, error) {
		length, err := binary.ReadUvarint(buffer)
		if err != nil {
			return nil, fmt.Errorf("failed to read length: %w", err)
		}

		data := make([]byte, length)
		if _, err := buffer.Read(data); err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
		return data, nil
	}

	for i := uint64(0); i < numKV; i++ {
		key, err := readBytes()
		if err != nil {
			return fmt.Errorf("failed to read key %d: %w", i, err)
		}

		value, err := readBytes()
		if err != nil {
			return fmt.Errorf("failed to read value %d: %w", i, err)
		}

		if string(key) == proto.QuotaKey {
			e.Quota = value
			continue
		}

		if e.dataMap == nil {
			e.dataMap = make(map[string][]byte)
		}
		e.Put(key, value, 0)
	}

	return nil
}

// decodeVersionInfo decodes version information from buffer
func (e *Extend) decodeVersionInfo(buffer *bytes.Buffer) error {
	// Read version sequence
	verSeq, err := binary.ReadUvarint(buffer)
	if err != nil {
		return fmt.Errorf("failed to read version sequence: %w", err)
	}

	// Read number of multi-versions
	numMultiVers, err := binary.ReadUvarint(buffer)
	if err != nil {
		return fmt.Errorf("failed to read multi-version count: %w", err)
	}

	if verSeq > 0 || numMultiVers > 0 {
		e.setVersion(verSeq)
	}

	if numMultiVers > 0 {
		e.multiSnap.multiVers = make([]*Extend, numMultiVers)

		for i := uint64(0); i < numMultiVers; i++ {
			// Read multi-version length
			mvLen, err := binary.ReadUvarint(buffer)
			if err != nil {
				return fmt.Errorf("failed to read multi-version length %d: %w", i, err)
			}

			mvBytes := make([]byte, mvLen)
			if _, err := buffer.Read(mvBytes); err != nil {
				return fmt.Errorf("failed to read multi-version data %d: %w", i, err)
			}

			// Recursively decode multi-versions
			mv, err := NewExtendFromBytes(mvBytes)
			if err != nil {
				return fmt.Errorf("failed to decode multi-version %d: %w", i, err)
			}

			e.multiSnap.multiVers[i] = mv
		}
	}

	return nil
}

// Less implements btree.Item interface
func (e *Extend) Less(than btree.Item) bool {
	ext, ok := than.(*Extend)
	return ok && e.inode < ext.inode
}

// Put stores a key-value pair
func (e *Extend) Put(key, value []byte, verSeq uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.dataMap == nil {
		e.dataMap = make(map[string][]byte)
	}

	// Create a copy of the value to avoid external modifications
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	e.dataMap[string(key)] = valueCopy

	if verSeq > 0 {
		e.setVersion(verSeq)
	}
}

// Get retrieves a value by key
func (e *Extend) Get(key []byte) ([]byte, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	value, exists := e.dataMap[string(key)]
	return value, exists
}

// Remove removes a key-value pair
func (e *Extend) Remove(key []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.dataMap != nil {
		delete(e.dataMap, string(key))
	}
}

// Range iterates over all key-value pairs
func (e *Extend) Range(visitor func(key, value []byte) bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	for k, v := range e.dataMap {
		if !visitor([]byte(k), v) {
			return
		}
	}
}

// Merge merges another Extend into this one
func (e *Extend) Merge(other *Extend, override bool) {
	if other == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.dataMap == nil {
		e.dataMap = make(map[string][]byte)
	}

	other.Range(func(key, value []byte) bool {
		strKey := string(key)
		if _, exists := e.dataMap[strKey]; override || !exists {
			// Create a copy to avoid external modifications
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			e.dataMap[strKey] = valueCopy
		}
		return true
	})
}

// Copy creates a deep copy of the Extend
func (e *Extend) Copy() btree.Item {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.deepCopy()
}

// deepCopy creates a deep copy of the Extend
func (e *Extend) deepCopy() *Extend {
	newExt := NewExtendWithQuota(e.inode)

	// Copy dataMap
	if len(e.dataMap) > 0 {
		newExt.dataMap = make(map[string][]byte, len(e.dataMap))
		for k, v := range e.dataMap {
			valueCopy := make([]byte, len(v))
			copy(valueCopy, v)
			newExt.dataMap[k] = valueCopy
		}
	}

	// Copy Quota
	if len(e.Quota) > 0 {
		newExt.Quota = make([]byte, len(e.Quota))
		copy(newExt.Quota, e.Quota)
	}

	// Deep copy multiSnap
	if e.multiSnap != nil {
		newExt.multiSnap = &ExtendMultiSnap{
			verSeq: e.multiSnap.verSeq,
		}

		if len(e.multiSnap.multiVers) > 0 {
			newExt.multiSnap.multiVers = make([]*Extend, len(e.multiSnap.multiVers))
			for i, mv := range e.multiSnap.multiVers {
				newExt.multiSnap.multiVers[i] = mv.deepCopy()
			}
		}
	}

	return newExt
}

// Bytes serializes the Extend to byte array
func (e *Extend) Bytes() ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	buffer := bytes.NewBuffer(nil)

	// Write inode
	if err := e.writeUvarint(buffer, e.inode); err != nil {
		return nil, fmt.Errorf("failed to write inode: %w", err)
	}

	// Write key-value pairs
	if err := e.writeKeyValuePairs(buffer); err != nil {
		return nil, fmt.Errorf("failed to write key-value pairs: %w", err)
	}

	// Write version information if available
	if e.getVersion() > 0 {
		if err := e.writeVersionInfo(buffer); err != nil {
			return nil, fmt.Errorf("failed to write version info: %w", err)
		}
	}

	return buffer.Bytes(), nil
}

// writeUvarint writes a uint64 as varint
func (e *Extend) writeUvarint(buffer *bytes.Buffer, value uint64) error {
	tmp := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(tmp, value)
	_, err := buffer.Write(tmp[:n])
	return err
}

// writeBytes writes a byte array with length prefix
func (e *Extend) writeBytes(buffer *bytes.Buffer, data []byte) error {
	if err := e.writeUvarint(buffer, uint64(len(data))); err != nil {
		return err
	}
	_, err := buffer.Write(data)
	return err
}

// writeKeyValuePairs writes all key-value pairs
func (e *Extend) writeKeyValuePairs(buffer *bytes.Buffer) error {
	// Calculate total pairs including quota
	pairCnt := len(e.dataMap)
	if len(e.Quota) > 0 {
		pairCnt++
	}

	if err := e.writeUvarint(buffer, uint64(pairCnt)); err != nil {
		return err
	}

	// Write dataMap pairs
	for k, v := range e.dataMap {
		if err := e.writeBytes(buffer, []byte(k)); err != nil {
			return err
		}
		if err := e.writeBytes(buffer, v); err != nil {
			return err
		}
	}

	// Write quota if exists
	if len(e.Quota) > 0 {
		if err := e.writeBytes(buffer, []byte(proto.QuotaKey)); err != nil {
			return err
		}
		if err := e.writeBytes(buffer, e.Quota); err != nil {
			return err
		}
	}

	return nil
}

// writeVersionInfo writes version information
func (e *Extend) writeVersionInfo(buffer *bytes.Buffer) error {
	// Write version sequence
	if err := e.writeUvarint(buffer, e.getVersion()); err != nil {
		return err
	}

	// Write number of multi-versions
	if err := e.writeUvarint(buffer, uint64(len(e.multiSnap.multiVers))); err != nil {
		return err
	}

	// Write each multi-version
	for _, mv := range e.multiSnap.multiVers {
		mvBytes, err := mv.Bytes()
		if err != nil {
			return fmt.Errorf("failed to serialize multi-version: %w", err)
		}

		if err := e.writeBytes(buffer, mvBytes); err != nil {
			return err
		}
	}

	return nil
}

// GetInode returns the inode number
func (e *Extend) GetInode() uint64 {
	return e.inode
}
