// Copyright 2022 The ChubaoFS Authors.
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

package proto

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cubefs/cubefs/util/btree"
)

type Blob struct {
	MinBid uint64
	Count  uint64
	Vid    uint64
}

// ExtentKey defines the extent key struct.
type ObjExtentKey struct {
	Cid        uint64 // cluster id
	CodeMode   uint8  // EC encode and decode mode
	BlobSize   uint32 // block size
	BlobsLen   uint32 // blob array length
	Size       uint64 // objExtentKey size
	Blobs      []Blob
	FileOffset uint64 // obj offset in file
	Crc        uint32
}

// String returns the string format of the extentKey.
func (k ObjExtentKey) String() string {
	return fmt.Sprintf("ObjExtentKey{FileOffset(%v),Cid(%v),CodeMode(%v),BlobSize(%v),BlobsLen(%v),Blobs(%v),Size(%v),Crc(%v)}", k.FileOffset, k.Cid, k.CodeMode, k.BlobSize, k.BlobsLen, k.Blobs, k.Size, k.Crc)
}

// Less defines the less comparator.
func (k *ObjExtentKey) Less(than btree.Item) bool {
	that := than.(*ObjExtentKey)
	return k.FileOffset < that.FileOffset
}

// Marshal marshals the obj extent key.
func (k *ObjExtentKey) Copy() btree.Item {
	return k
}

func (k *ObjExtentKey) IsEquals(obj *ObjExtentKey) bool {
	if k.FileOffset != obj.FileOffset {
		return false
	}
	if k.Cid != obj.Cid {
		return false
	}
	if k.CodeMode != obj.CodeMode {
		return false
	}
	if k.BlobSize != obj.BlobSize {
		return false
	}
	if k.BlobsLen != obj.BlobsLen {
		return false
	}
	if k.Size != obj.Size {
		return false
	}
	if k.Crc != obj.Crc {
		return false
	}
	if len(k.Blobs) > 0 {
		for i := len(k.Blobs) - 1; i >= 0; i-- {
			if k.Blobs[i].Count != obj.Blobs[i].Count || k.Blobs[i].MinBid != obj.Blobs[i].MinBid || k.Blobs[i].Vid != obj.Blobs[i].Vid {
				return false
			}
		}
	}
	return true
}

// MarshalBinary marshals the binary format of the extent key.
func (k *ObjExtentKey) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	if err := binary.Write(buf, binary.BigEndian, uint32(len(k.Blobs))); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.FileOffset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Size); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Crc); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.CodeMode); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Cid); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.BlobSize); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, k.Blobs); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (k *ObjExtentKey) UnmarshalBinary(buf *bytes.Buffer) (err error) {
	if err = binary.Read(buf, binary.BigEndian, &k.BlobsLen); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.FileOffset); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Size); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Crc); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.CodeMode); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.Cid); err != nil {
		return
	}
	if err = binary.Read(buf, binary.BigEndian, &k.BlobSize); err != nil {
		return
	}
	blobs := make([]Blob, 0, int(k.BlobsLen))
	for i := 0; i < int(k.BlobsLen); i++ {
		tmpBlob := Blob{}
		if err = binary.Read(buf, binary.BigEndian, &tmpBlob); err != nil {
			return
		}
		blobs = append(blobs, tmpBlob)
	}
	k.Blobs = blobs

	return
}
