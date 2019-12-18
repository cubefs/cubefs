// Copyright 2018 The Chubao Authors.
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
	"github.com/chubaofs/chubaofs/util/btree"
	"sort"
	"sync"
	"time"
)

// Part defined necessary fields for multipart part management.
type Part struct {
	ID         uint16
	UploadTime time.Time
	MD5        string
	Size       uint64
	Inode      uint64
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

func (m Parts) Less(i, j int) bool {
	return m[i].ID < m[j].ID
}

func (m Parts) Swap(i, j int) {
	m[i], m[j] = m[j], m[i]
}

func (m Parts) Sort() {
	sort.Sort(m)
}

func (m *Parts) Hash(part *Part) (has bool) {
	i := sort.Search(len(*m), func(i int) bool {
		return (*m)[i].ID >= part.ID
	})
	has = i < len(*m) && (*m)[i].ID == part.ID
	return
}

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
	m.Sort()
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

// Multipart defined necessary fields for multipart session management.
type Multipart struct {
	// session fields
	id       string
	key      string
	initTime time.Time
	parts    Parts

	mu sync.RWMutex
}

func (m *Multipart) Less(than btree.Item) bool {
	thanMultipart, is := than.(*Multipart)
	return is && m.id < thanMultipart.id
}

func (m *Multipart) Copy() btree.Item {
	return &Multipart{
		id:       m.id,
		key:      m.key,
		initTime: m.initTime,
		parts:    append(Parts{}, m.parts...),
	}
}

func (m *Multipart) ID() string {
	return m.id
}

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

	var muSession = &Multipart{
		id:       id,
		key:      key,
		initTime: time.Unix(0, initTimeI64),
		parts:    parts,
	}
	return muSession
}
