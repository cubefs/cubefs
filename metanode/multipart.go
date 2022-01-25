// Copyright 2018 The Cubefs Authors.
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
	"time"

	"github.com/chubaofs/chubaofs/util/btree"
)

// Part defined necessary fields for multipart part management.
type Part struct {
	ID         uint16
	UploadTime time.Time
	MD5        string
	Size       uint64
	Inode      uint64
}

func (m *Part) Equal(o *Part) bool {
	return m.ID == o.ID &&
		m.Inode == o.Inode &&
		m.Size == o.Size &&
		m.MD5 == o.MD5
}

func (m Part) Bytes() ([]byte, error) {
	var err error
	var buffer = bytes.NewBuffer(nil)
	var tmp = make([]byte, binary.MaxVarintLen64)
	var n int
	// ID
	n = binary.PutUvarint(tmp, uint64(m.ID))
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	// upload time
	n = binary.PutVarint(tmp, m.UploadTime.UnixNano())
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	// MD5
	n = binary.PutUvarint(tmp, uint64(len(m.MD5)))
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	if _, err = buffer.WriteString(m.MD5); err != nil {
		return nil, err
	}
	// size
	n = binary.PutUvarint(tmp, m.Size)
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	// inode
	n = binary.PutUvarint(tmp, m.Inode)
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func PartFromBytes(raw []byte) *Part {
	var offset, n int
	// decode ID
	var u64ID uint64
	u64ID, n = binary.Uvarint(raw)
	offset += n
	// decode upload time
	var uploadTimeI64 int64
	uploadTimeI64, n = binary.Varint(raw[offset:])
	offset += n
	// decode MD5
	var md5Len uint64
	md5Len, n = binary.Uvarint(raw[offset:])
	offset += n
	var md5Content = string(raw[offset : offset+int(md5Len)])
	offset += int(md5Len)
	// decode size
	var sizeU64 uint64
	sizeU64, n = binary.Uvarint(raw[offset:])
	offset += n
	// decode inode
	var inode uint64
	inode, n = binary.Uvarint(raw[offset:])

	var muPart = &Part{
		ID:         uint16(u64ID),
		UploadTime: time.Unix(0, uploadTimeI64),
		MD5:        md5Content,
		Size:       sizeU64,
		Inode:      inode,
	}
	return muPart
}

type Parts []*Part

func (m Parts) Len() int {
	return len(m)
}

func (m Parts) sort() {
	sort.SliceStable(m, func(i, j int) bool {
		return m[i].ID < m[j].ID
	})
}

func (m *Parts) Hash(part *Part) (has bool) {
	i := sort.Search(len(*m), func(i int) bool {
		return (*m)[i].ID >= part.ID
	})
	has = i < len(*m) && (*m)[i].ID == part.ID
	return
}

func (m *Parts) LoadOrStore(part *Part) (actual *Part, stored bool) {
	i := sort.Search(len(*m), func(i int) bool {
		return (*m)[i].ID >= part.ID
	})
	if i >= 0 && i < len(*m) && (*m)[i].ID == part.ID {
		actual = (*m)[i]
		stored = false
		return
	}
	*m = append(*m, part)
	actual = part
	stored = true
	m.sort()
	return
}

// Deprecated
func (m *Parts) Insert(part *Part, replace bool) (success bool) {
	i := sort.Search(len(*m), func(i int) bool {
		return (*m)[i].ID >= part.ID
	})
	if i < len(*m) && (*m)[i].ID == part.ID {
		if replace {
			(*m)[i] = part
			return true
		}
		return false
	}
	*m = append(*m, part)
	m.sort()
	return true
}

func (m *Parts) Remove(id uint16) {
	i := sort.Search(len(*m), func(i int) bool {
		return (*m)[i].ID >= id
	})
	if i < len(*m) && (*m)[i].ID == id {
		if len(*m) > i+1 {
			*m = append((*m)[:i], (*m)[i+1:]...)
		} else {
			*m = (*m)[:i]
		}
	}
}

func (m Parts) Search(id uint16) (part *Part, found bool) {
	i := sort.Search(len(m), func(i int) bool {
		return m[i].ID >= id
	})
	if i < len(m) && m[i].ID == id {
		return m[i], true
	}
	return nil, false
}

func (m Parts) Bytes() ([]byte, error) {
	var err error
	var n int
	var buffer = bytes.NewBuffer(nil)
	var tmp = make([]byte, binary.MaxVarintLen64)
	n = binary.PutUvarint(tmp, uint64(len(m)))
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	var marshaled []byte
	for _, p := range m {
		marshaled, err = p.Bytes()
		if err != nil {
			return nil, err
		}
		// write part length
		n = binary.PutUvarint(tmp, uint64(len(marshaled)))
		if _, err = buffer.Write(tmp[:n]); err != nil {
			return nil, err
		}
		// write part bytes
		if _, err = buffer.Write(marshaled); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func PartsFromBytes(raw []byte) Parts {
	var offset, n int
	var numPartsU64 uint64
	numPartsU64, n = binary.Uvarint(raw)
	offset += n
	var muParts = make([]*Part, int(numPartsU64))
	for i := 0; i < int(numPartsU64); i++ {
		var partLengthU64 uint64
		partLengthU64, n = binary.Uvarint(raw[offset:])
		offset += n
		part := PartFromBytes(raw[offset : offset+int(partLengthU64)])
		muParts[i] = part
		offset += int(partLengthU64)
	}
	return muParts
}

type MultipartExtend map[string]string

func NewMultipartExtend() MultipartExtend {
	return make(map[string]string)
}

func (me MultipartExtend) Bytes() ([]byte, error) {
	var n int
	var err error
	var buffer = bytes.NewBuffer(nil)
	var tmp = make([]byte, binary.MaxVarintLen64)
	n = binary.PutUvarint(tmp, uint64(len(me)))
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	var marshalStr = func(src string) error {
		n = binary.PutUvarint(tmp, uint64(len(src)))
		if _, err = buffer.Write(tmp[:n]); err != nil {
			return err
		}
		if _, err = buffer.WriteString(src); err != nil {
			return err
		}
		return nil
	}
	for key, val := range me {
		if err = marshalStr(key); err != nil {
			return nil, err
		}
		if err = marshalStr(val); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func MultipartExtendFromBytes(raw []byte) MultipartExtend {
	var offset, n int
	var el uint64
	me := NewMultipartExtend()
	var unmarshalStr = func(data []byte) (string, int) {
		var n int
		var lengthU64 uint64
		lengthU64, n = binary.Uvarint(data)
		return string(data[n : n+int(lengthU64)]), n + int(lengthU64)
	}
	el, n = binary.Uvarint(raw)
	if el <= 0 {
		return nil
	}
	offset += n
	for i := 0; i < int(el); i++ {
		var key, val string
		key, n = unmarshalStr(raw[offset:])
		offset += n
		val, n = unmarshalStr(raw[offset:])
		offset += n
		me[key] = val
	}
	return me
}

// Multipart defined necessary fields for multipart session management.
type Multipart struct {
	// session fields
	id       string
	key      string
	initTime time.Time
	parts    Parts
	extend   MultipartExtend

	mu sync.RWMutex
}

func (m *Multipart) Less(than btree.Item) bool {
	tm, is := than.(*Multipart)
	return is && ((m.key < tm.key) || ((m.key == tm.key) && (m.id < tm.id)))
}

func (m *Multipart) Copy() btree.Item {
	return &Multipart{
		id:       m.id,
		key:      m.key,
		initTime: m.initTime,
		parts:    append(Parts{}, m.parts...),
		extend:   m.extend,
	}
}

func (m *Multipart) ID() string {
	return m.id
}

func (m *Multipart) LoadOrStorePart(part *Part) (actual *Part, stored bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.parts == nil {
		m.parts = PartsFromBytes(nil)
	}
	actual, stored = m.parts.LoadOrStore(part)
	return
}

// Deprecated
func (m *Multipart) InsertPart(part *Part, replace bool) (success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.parts == nil {
		m.parts = PartsFromBytes(nil)
	}
	success = m.parts.Insert(part, replace)
	return
}

func (m *Multipart) Parts() []*Part {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]*Part{}, m.parts...)
}

func (m *Multipart) Bytes() ([]byte, error) {
	var n int
	var buffer = bytes.NewBuffer(nil)
	var err error
	tmp := make([]byte, binary.MaxVarintLen64)
	// marshal id
	var marshalStr = func(src string) error {
		n = binary.PutUvarint(tmp, uint64(len(src)))
		if _, err = buffer.Write(tmp[:n]); err != nil {
			return err
		}
		if _, err = buffer.WriteString(src); err != nil {
			return err
		}
		return nil
	}
	// marshal id
	if err = marshalStr(m.id); err != nil {
		return nil, err
	}
	// marshal key
	if err = marshalStr(m.key); err != nil {
		return nil, err
	}
	// marshal init time
	n = binary.PutVarint(tmp, m.initTime.UnixNano())
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	// marshal parts
	var marshaledParts []byte
	if marshaledParts, err = m.parts.Bytes(); err != nil {
		return nil, err
	}
	n = binary.PutUvarint(tmp, uint64(len(marshaledParts)))
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	if _, err = buffer.Write(marshaledParts); err != nil {
		return nil, err
	}
	// marshall extend
	var extendBytes []byte
	if extendBytes, err = m.extend.Bytes(); err != nil {
		return nil, err
	}
	n = binary.PutUvarint(tmp, uint64(len(extendBytes)))
	if _, err = buffer.Write(tmp[:n]); err != nil {
		return nil, err
	}
	if _, err = buffer.Write(extendBytes); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func MultipartFromBytes(raw []byte) *Multipart {
	var unmarshalStr = func(data []byte) (string, int) {
		var n int
		var lengthU64 uint64
		lengthU64, n = binary.Uvarint(data)
		return string(data[n : n+int(lengthU64)]), n + int(lengthU64)
	}
	var offset, n int
	// decode id
	var id string
	id, n = unmarshalStr(raw)
	offset += n
	// decode key
	var key string
	key, n = unmarshalStr(raw[offset:])
	offset += n
	// decode init time
	var initTimeI64 int64
	initTimeI64, n = binary.Varint(raw[offset:])
	offset += n
	// decode parts
	var partsLengthU64 uint64
	partsLengthU64, n = binary.Uvarint(raw[offset:])
	offset += n
	var parts = PartsFromBytes(raw[offset : offset+int(partsLengthU64)])
	offset += int(partsLengthU64)
	// decode multipart extend
	var extendLengthU64 uint64
	extendLengthU64, n = binary.Uvarint(raw[offset:])
	offset += n
	var me = MultipartExtendFromBytes(raw[offset : offset+int(extendLengthU64)])

	var muSession = &Multipart{
		id:       id,
		key:      key,
		initTime: time.Unix(0, initTimeI64),
		parts:    parts,
		extend:   me,
	}
	return muSession
}
