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

package uptoken

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/proto"
)

const (
	// TokenSize max size of token array
	// clusterID + vid + bid + count + time + size
	TokenSize = 4 + 4 + 10 + 5 + 5 + 5
)

// UploadToken token between alloc and putat
// [0:8]   hmac (8) first 8 bytes of sha1 summary
// [8:16]  minBid (8) in the SliceInfo
// [16:20] count (4) in the SliceInfo
// [20:24] time (4) expired unix utc time, 0 means not expired
type UploadToken struct {
	Data   [TokenSize]byte
	Offset uint8
}

// IsValidBid returns the bid is valid or not
func (t *UploadToken) IsValidBid(bid proto.BlobID) bool {
	minBid, n := binary.Uvarint(t.Data[8:])
	if n <= 0 {
		return false
	}
	count, n := binary.Uvarint(t.Data[8+n:])
	if n <= 0 {
		return false
	}
	return minBid <= uint64(bid) && uint64(bid) < minBid+count
}

// IsValid returns the token is valid or not
func (t *UploadToken) IsValid(clusterID proto.ClusterID, vid proto.Vid, bid proto.BlobID,
	size uint32, secretKey []byte) bool {
	var (
		minBid, count, expiredTime uint64
		ok                         bool
	)

	data := t.Data[:]
	offset := 8
	next := func() (uint64, bool) {
		val, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			return 0, false
		}
		offset += n
		return val, true
	}

	if minBid, ok = next(); !ok {
		return false
	}
	if count, ok = next(); !ok {
		return false
	}
	if !(minBid <= uint64(bid) && uint64(bid) < minBid+count) {
		return false
	}

	if expiredTime, ok = next(); !ok {
		return false
	}
	if expiredTime != 0 && time.Now().UTC().Unix() > int64(expiredTime) {
		return false
	}

	token := newUploadToken(clusterID, vid, proto.BlobID(minBid),
		uint32(count), size, uint32(expiredTime), secretKey)
	return bytes.Equal(token.Data[0:8], t.Data[0:8])
}

// NewUploadToken returns a token corresponding one SliceInfo in Location with expiration
// expiration = 0 means not expired forever
func NewUploadToken(clusterID proto.ClusterID, vid proto.Vid, bid proto.BlobID,
	count, size uint32, expiration time.Duration, secretKey []byte) UploadToken {
	expiredTime := uint32(0)
	if expiration != time.Duration(0) {
		expiredTime = uint32(time.Now().Add(expiration).UTC().Unix())
	}
	return newUploadToken(clusterID, vid, bid, count, size, expiredTime, secretKey)
}

func newUploadToken(clusterID proto.ClusterID, vid proto.Vid, bid proto.BlobID,
	count, size uint32, expiredTime uint32, secretKey []byte) UploadToken {
	var token UploadToken
	data := token.Data[:]
	binary.BigEndian.PutUint32(data[0:4], uint32(clusterID))
	binary.BigEndian.PutUint32(data[4:8], uint32(vid))

	offset := 8
	offset += binary.PutUvarint(data[offset:], uint64(bid))
	offset += binary.PutUvarint(data[offset:], uint64(count))
	offset += binary.PutUvarint(data[offset:], uint64(expiredTime))
	token.Offset = uint8(offset)
	offset += binary.PutUvarint(data[offset:], uint64(size))

	h := hmac.New(sha1.New, secretKey)
	h.Write(data[:offset])
	sum := h.Sum(nil)

	// replace clusterID and vid
	copy(data[0:8], sum[0:8])
	return token
}

// EncodeToken encode token to string
func EncodeToken(token UploadToken) string {
	return hex.EncodeToString(token.Data[:token.Offset])
}

// DecodeToken decode token from string
func DecodeToken(s string) UploadToken {
	var token UploadToken
	src, _ := hex.DecodeString(s)
	token.Offset = uint8(copy(token.Data[:], src))
	return token
}
