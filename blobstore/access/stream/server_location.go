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

package stream

import (
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/uptoken"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
)

const (
	// DO NOT CHANGE IT.
	_crcPoly         = uint32(0x59c8943c)
	_tokenExpiration = time.Hour * 12
)

var (
	// DO NOT CHANGE IT.
	_crcTable    = crc32.MakeTable(_crcPoly)
	_crcMagicKey = [20]byte{
		0x52, 0xe, 0x53, 0x53, 0x81,
		0x1f, 0x51, 0xb7, 0xa4, 0x72,
		0x10, 0x33, 0x64, 0xa7, 0x3a,
		0x10, 0x19, 0xbc, 0x60, 0x7,
	}
	_initLocationSecret sync.Once

	// tokenSecretKeys alloc token with the first secret key always,
	// so that you can change the secret key.
	//
	// parse-1: insert a new key at the first index,
	// parse-2: delete the old key at the last index after _tokenExpiration duration.
	tokenSecretKeys = [...][20]byte{
		{0x5f, 0x00, 0x88, 0x96, 0x00, 0xa1, 0xfe, 0x1b},
		{0xff, 0x1f, 0x2f, 0x4f, 0x7f, 0xaf, 0xef, 0xff},
	}
	InitTokenSecret sync.Once

	LocationCrcCalculate = calcCrc
	LocationCrcFill      = fillCrc
	LocationCrcVerify    = verifyCrc
	LocationCrcSign      = signCrc
	LocationInitSecret   = initLocationSecret

	StreamTokenSecretKeys = tokenSecretKeys
	StreamGenTokens       = genTokens
)

func initLocationSecret(b []byte) {
	_initLocationSecret.Do(func() {
		copy(_crcMagicKey[7:], b)
	})
}

func calcCrc(loc *access.Location) (uint32, error) {
	crcWriter := crc32.New(_crcTable)

	buf := bytespool.Alloc(1024)
	defer bytespool.Free(buf)

	n := loc.Encode2(buf)
	if n < 4 {
		return 0, fmt.Errorf("no enough bytes(%d) fill into buf", n)
	}

	if _, err := crcWriter.Write(_crcMagicKey[:]); err != nil {
		return 0, fmt.Errorf("fill crc %s", err.Error())
	}
	if _, err := crcWriter.Write(buf[4:n]); err != nil {
		return 0, fmt.Errorf("fill crc %s", err.Error())
	}

	return crcWriter.Sum32(), nil
}

func fillCrc(loc *access.Location) error {
	crc, err := calcCrc(loc)
	if err != nil {
		return err
	}
	loc.Crc = crc
	return nil
}

func verifyCrc(loc *access.Location) bool {
	crc, err := calcCrc(loc)
	if err != nil {
		return false
	}
	return loc.Crc == crc
}

func signCrc(loc *access.Location, locs []access.Location) error {
	first := locs[0]
	bids := make(map[proto.BlobID]struct{}, 64)

	if loc.ClusterID != first.ClusterID ||
		loc.CodeMode != first.CodeMode ||
		loc.BlobSize != first.BlobSize {
		return fmt.Errorf("not equal in constant field")
	}

	for _, l := range locs {
		if !verifyCrc(&l) {
			return fmt.Errorf("not equal in crc %d", l.Crc)
		}

		// assert
		if l.ClusterID != first.ClusterID ||
			l.CodeMode != first.CodeMode ||
			l.BlobSize != first.BlobSize {
			return fmt.Errorf("not equal in constant field")
		}

		for _, blob := range l.Blobs {
			for c := 0; c < int(blob.Count); c++ {
				bids[blob.MinBid+proto.BlobID(c)] = struct{}{}
			}
		}
	}

	for _, blob := range loc.Blobs {
		for c := 0; c < int(blob.Count); c++ {
			bid := blob.MinBid + proto.BlobID(c)
			if _, ok := bids[bid]; !ok {
				return fmt.Errorf("not equal in blob_id(%d)", bid)
			}
		}
	}

	return fillCrc(loc)
}

// genTokens generate tokens
//  1. Returns 0 token if has no blobs.
//  2. Returns 1 token if file size less than blobsize.
//  3. Returns len(blobs) tokens if size divided by blobsize.
//  4. Otherwise returns len(blobs)+1 tokens, the last token
//     will be used by the last blob, even if the last slice blobs' size
//     less than blobsize.
//  5. Each segment blob has its specified token include the last blob.
func genTokens(location *access.Location) []string {
	tokens := make([]string, 0, len(location.Blobs)+1)

	hasMultiBlobs := location.Size >= uint64(location.BlobSize)
	lastSize := uint32(location.Size % uint64(location.BlobSize))
	for idx, blob := range location.Blobs {
		// returns one token if size < blobsize
		if hasMultiBlobs {
			count := blob.Count
			if idx == len(location.Blobs)-1 && lastSize > 0 {
				count--
			}
			tokens = append(tokens, uptoken.EncodeToken(uptoken.NewUploadToken(location.ClusterID,
				blob.Vid, blob.MinBid, count,
				location.BlobSize, _tokenExpiration, tokenSecretKeys[0][:])))
		}

		// token of the last blob
		if idx == len(location.Blobs)-1 && lastSize > 0 {
			tokens = append(tokens, uptoken.EncodeToken(uptoken.NewUploadToken(location.ClusterID,
				blob.Vid, blob.MinBid+proto.BlobID(blob.Count)-1, 1,
				lastSize, _tokenExpiration, tokenSecretKeys[0][:])))
		}
	}

	return tokens
}
