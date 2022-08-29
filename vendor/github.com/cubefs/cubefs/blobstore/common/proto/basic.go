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
	"fmt"
	"strconv"
	"strings"
)

// basic type for all module
type (
	DiskID    uint32
	BlobID    uint64
	Vid       uint32
	ClusterID uint32
)

func (id DiskID) Encode() []byte {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(id))
	return key
}

func (id *DiskID) Decode(b []byte) DiskID {
	key := binary.BigEndian.Uint32(b)
	*id = DiskID(key)
	return *id
}

func (id DiskID) ToString() string {
	return strconv.FormatUint(uint64(id), 10)
}

func (vid Vid) ToString() string {
	return strconv.FormatUint(uint64(vid), 10)
}

func (id ClusterID) ToString() string {
	return strconv.FormatUint(uint64(id), 10)
}

const seqToken = ";"

// EncodeToken encode host and vid to a string token.
func EncodeToken(host string, vid Vid) (token string) {
	return fmt.Sprintf("%s%s%s", host, seqToken, strconv.FormatUint(uint64(vid), 10))
}

// DecodeToken decode host and vid from the token.
func DecodeToken(token string) (host string, vid Vid, err error) {
	parts := strings.SplitN(token, seqToken, 2)
	if len(parts) != 2 {
		err = fmt.Errorf("invalid token %s", token)
		return
	}
	host = parts[0]
	vidU32, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return
	}
	vid = Vid(vidU32)
	return
}
