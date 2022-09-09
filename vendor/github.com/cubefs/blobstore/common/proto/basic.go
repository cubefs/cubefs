// Copyright 2022 The CubeFS Authors.
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
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/cubefs/blobstore/util/errors"
)

// basic type for all module
type (
	DiskID    uint32
	BlobID    uint64
	Vid       uint32
	ClusterID uint32
	BidCount  uint32
)

/*
	Encode or Decode function for basic type
*/

func EncodeDiskID(id DiskID) []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(id))
	return key
}

func DecodeDiskID(b []byte) DiskID {
	key := binary.BigEndian.Uint32(b)
	return DiskID(key)
}

func (diskID DiskID) ToString() string {
	return strconv.FormatUint(uint64(diskID), 10)
}

func (vid Vid) ToString() string {
	return strconv.FormatUint(uint64(vid), 10)
}

func (id ClusterID) ToString() string {
	return strconv.FormatUint(uint64(id), 10)
}

func EncodeToken(host string, vid Vid) (token string) {
	token = host + ";" + strconv.FormatUint(uint64(vid), 10)
	return
}

func DecodeToken(token string) (h string, vid Vid, err error) {
	parts := strings.Split(token, ";")
	if len(parts) != 2 {
		err = errors.New("decode tokens error")
		return
	}
	h = parts[0]
	vidU32, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return
	}
	vid = Vid(vidU32)

	return
}
