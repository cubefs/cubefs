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
	"fmt"
)

// Dentry wraps necessary properties of the `dentry` information in file system.
// Marshal exporterKey:
//  +-------+----------+------+
//  | item  | ParentId | Name |
//  +-------+----------+------+
//  | bytes |    8     | rest |
//  +-------+----------+------+
// Marshal value:
//  +-------+-------+------+
//  | item  | Inode | Type |
//  +-------+-------+------+
//  | bytes |   8   |   4  |
//  +-------+-------+------+
// Marshal entity:
//  +-------+-----------+--------------+-----------+--------------+
//  | item  | KeyLength | MarshaledKey | ValLength | MarshaledVal |
//  +-------+-----------+--------------+-----------+--------------+
//  | bytes |     4     |   KeyLength  |     4     |   ValLength  |
//  +-------+-----------+--------------+-----------+--------------+

const (
	BaseDentryKeyLen = 8
	DentryValueLen  = 12
	DentryKeyOffset = 4
	BaseDentryLen   = 24
)

type Dentry struct {
	ParentId uint64 `json:"pid"`  // FileID value of the parent inode.
	Name     string `json:"name"` // Name of the current dentry.
	Inode    uint64 `json:"ino"`  // FileID value of the current inode.
	Type     uint32 `json:"type"`
}

type DentryBatch []*Dentry

// Marshal marshals a dentry into a byte array.
func (d *Dentry) Marshal() (result []byte, err error) {
	keyBytes := d.MarshalKey()
	valBytes := d.MarshalValue()
	keyLen := uint32(len(keyBytes))
	valLen := uint32(len(valBytes))
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(64)
	if err = binary.Write(buff, binary.BigEndian, keyLen); err != nil {
		return
	}
	if _, err = buff.Write(keyBytes); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, valLen); err != nil {

	}
	if _, err = buff.Write(valBytes); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

// Unmarshal unmarshals the dentry from a byte array.
func (d *Dentry) Unmarshal(raw []byte) (err error) {
	var (
		keyLen uint32
		valLen uint32
	)
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &keyLen); err != nil {
		return
	}
	keyBytes := make([]byte, keyLen)
	if _, err = buff.Read(keyBytes); err != nil {
		return
	}
	if err = d.UnmarshalKey(keyBytes); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &valLen); err != nil {
		return
	}
	valBytes := make([]byte, valLen)
	if _, err = buff.Read(valBytes); err != nil {
		return
	}
	err = d.UnmarshalValue(valBytes)
	return
}

func (d *Dentry) MarshalV2() (result []byte, err error) {
	offset := 0
	nameLen := len(d.Name)
	result = make([]byte, 28+len(d.Name))
	binary.BigEndian.PutUint32(result[:4], uint32(8 + nameLen))
	offset += 4
	binary.BigEndian.PutUint64(result[offset:offset+8], d.ParentId)
	offset += 8
	copy(result[offset:offset+nameLen], []byte(d.Name))
	offset += nameLen
	binary.BigEndian.PutUint32(result[offset: offset+4], 12)
	offset += 4
	binary.BigEndian.PutUint64(result[offset:offset+8], d.Inode)
	offset += 8
	binary.BigEndian.PutUint32(result[offset:offset+4], d.Type)
	return
}

func (d *Dentry) UnmarshalV2(raw []byte) (err error) {
	if len(raw) < BaseDentryLen {
		return fmt.Errorf("dentry buff err, need at least %d, but buff len:%d", BaseDentryLen, len(raw))
	}
	offset := 0
	var keyLen = binary.BigEndian.Uint32(raw[:4])
	offset += 4
	d.ParentId = binary.BigEndian.Uint64(raw[offset:offset+8])
	offset += 8
	d.Name = string(raw[offset:offset+int(keyLen-8)])
	offset += int(keyLen-8)
	offset += 4
	d.Inode = binary.BigEndian.Uint64(raw[offset:offset+8])
	offset += 8
	d.Type = binary.BigEndian.Uint32(raw[offset:offset+4])
	return
}

func (d *Dentry) DentryKeyLen() int {
	return len(d.Name) + 8
}

func (d *Dentry) UnmarshalV2WithKeyAndValue(key, value []byte) (err error){
	if err = d.UnmarshalKeyV2(key); err != nil{
		return
	}
	if err = d.UnmarshalValueV2(value); err != nil{
		return
	}
	return nil
}

func (d *Dentry) UnmarshalKeyV2(key []byte) error{
	if len(key) < BaseDentryKeyLen {
		return fmt.Errorf("dentry key buff err, need at least %d, but buff len:%d", BaseDentryKeyLen, len(key))
	}
	d.ParentId = binary.BigEndian.Uint64(key[0:8])
	d.Name = string(key[8:])
	return nil
}

func (d *Dentry) UnmarshalValueV2(value []byte) error{
	if len(value) < DentryValueLen {
		return fmt.Errorf("dentry value buff err, need at least %d, but buff len:%d", BaseDentryKeyLen, len(value))
	}
	d.Inode = binary.BigEndian.Uint64(value[0:8])
	d.Type = binary.BigEndian.Uint32(value[8:12])
	return nil
}

// Marshal marshals the dentryBatch into a byte array.
func (d DentryBatch) Marshal() ([]byte, error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buff, binary.BigEndian, uint32(len(d))); err != nil {
		return nil, err
	}
	for _, dentry := range d {
		bs, err := dentry.Marshal()
		if err != nil {
			return nil, err
		}
		if err = binary.Write(buff, binary.BigEndian, uint32(len(bs))); err != nil {
			return nil, err
		}
		if _, err := buff.Write(bs); err != nil {
			return nil, err
		}
	}
	return buff.Bytes(), nil
}

// Unmarshal unmarshals the dentryBatch.
func DentryBatchUnmarshal(raw []byte) (DentryBatch, error) {
	buff := bytes.NewBuffer(raw)
	var batchLen uint32
	if err := binary.Read(buff, binary.BigEndian, &batchLen); err != nil {
		return nil, err
	}

	result := make(DentryBatch, 0, int(batchLen))

	var dataLen uint32
	for j := 0; j < int(batchLen); j++ {
		if err := binary.Read(buff, binary.BigEndian, &dataLen); err != nil {
			return nil, err
		}
		data := make([]byte, int(dataLen))
		if _, err := buff.Read(data); err != nil {
			return nil, err
		}
		den := &Dentry{}
		if err := den.Unmarshal(data); err != nil {
			return nil, err
		}
		result = append(result, den)
	}

	return result, nil
}

// Less tests whether the current dentry is less than the given one.
// This method is necessary fot B-Tree item implementation.
func (d *Dentry) Less(than BtreeItem) (less bool) {
	dentry, ok := than.(*Dentry)
	less = ok && ((d.ParentId < dentry.ParentId) || ((d.ParentId == dentry.ParentId) && (d.Name < dentry.Name)))
	return
}

func (d *Dentry) Copy() BtreeItem {
	newDentry := *d
	return &newDentry
}

// MarshalKey is the bytes version of the MarshalKey method which returns the byte slice result.
func (d *Dentry) MarshalKey() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(32)
	if err := binary.Write(buff, binary.BigEndian, &d.ParentId); err != nil {
		panic(err)
	}
	buff.Write([]byte(d.Name))
	k = buff.Bytes()
	return
}

// UnmarshalKey unmarshals the exporterKey from bytes.
func (d *Dentry) UnmarshalKey(k []byte) (err error) {
	buff := bytes.NewBuffer(k)
	if err = binary.Read(buff, binary.BigEndian, &d.ParentId); err != nil {
		return
	}
	d.Name = string(buff.Bytes())
	return
}

// MarshalValue marshals the exporterKey to bytes.
func (d *Dentry) MarshalValue() (k []byte) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(12)
	if err := binary.Write(buff, binary.BigEndian, &d.Inode); err != nil {
		panic(err)
	}
	if err := binary.Write(buff, binary.BigEndian, &d.Type); err != nil {
		panic(err)
	}
	k = buff.Bytes()
	return
}

// UnmarshalValue unmarshals the value from bytes.
func (d *Dentry) UnmarshalValue(val []byte) (err error) {
	buff := bytes.NewBuffer(val)
	if err = binary.Read(buff, binary.BigEndian, &d.Inode); err != nil {
		return
	}
	err = binary.Read(buff, binary.BigEndian, &d.Type)
	return
}

func (d *Dentry) String() string {
	return fmt.Sprintf(" ParentID: %v, InodeID: %v, Name: %v", d.ParentId, d.Inode, d.Name)
}
