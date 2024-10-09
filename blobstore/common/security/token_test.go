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

package security_test

import (
	"crypto/rand"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/security"
)

func TestAccessServerTokenBase(t *testing.T) {
	token := security.NewUploadToken(1, 1, 1, 1, 1, 0, []byte{})
	require.Equal(t, "d29f19731941e44a010100", security.EncodeToken(token))
}

func TestAccessServerTokenValid(t *testing.T) {
	for ii := 0; ii < 1000; ii++ {
		cid := proto.ClusterID(mrand.Uint32())
		vid := proto.Vid(mrand.Uint32())
		bid := proto.BlobID(mrand.Uint64())
		count := mrand.Uint32()
		size := mrand.Uint32()
		secretKey := make([]byte, mrand.Intn(40)+1)
		rand.Read(secretKey)
		secretKey[len(secretKey)-1] = 0xff

		if cid == 0 || vid == 0 || bid == 0 || count == 0 {
			continue
		}

		token := security.NewUploadToken(cid, vid, bid, count, size, time.Minute, secretKey)
		require.True(t, token.IsValidBid(bid))
		require.True(t, token.IsValid(cid, vid, bid, size, secretKey))

		{ // invalid bid
			for i := 0; i < 100; i++ {
				bidx := proto.BlobID(mrand.Int63n(int64(bid >> 1)))
				require.False(t, token.IsValidBid(bidx))
				require.False(t, token.IsValid(cid, vid, bidx, size, secretKey))
			}
		}
		{ // valid bid
			for i := 0; i < 100; i++ {
				bidx := bid + proto.BlobID(mrand.Int63n(int64(count)))
				require.True(t, token.IsValidBid(bidx))
				require.True(t, token.IsValid(cid, vid, bidx, size, secretKey))
			}
		}
		{ // invalid bid
			for i := uint32(1); i < 100; i++ {
				bidx := bid + proto.BlobID(count+i)
				require.False(t, token.IsValidBid(bidx))
				require.False(t, token.IsValid(cid, vid, bidx, size, secretKey))
			}
		}
		{ // invalid clusterID
			for i := 0; i < 100; i++ {
				cidx := proto.ClusterID(mrand.Uint32())
				if cid != cidx {
					require.False(t, token.IsValid(cidx, vid, bid, size, secretKey))
				}
			}
		}
		{ // invalid vid
			for i := 0; i < 100; i++ {
				vidx := proto.Vid(mrand.Uint32())
				if vid != vidx {
					require.False(t, token.IsValid(cid, vidx, bid, size, secretKey))
				}
			}
		}
		{ // valid size
			for i := 0; i < 100; i++ {
				sizex := mrand.Uint32()
				if size != sizex {
					require.False(t, token.IsValid(cid, vid, bid, sizex, secretKey))
				}
			}
		}
		{ // invalid secretKey
			for i := 0; i < 100; i++ {
				secretKeyx := secretKey[:len(secretKey)/2]
				require.False(t, token.IsValid(cid, vid, bid, size, secretKeyx))
			}
		}
		{ // invalid token
			for i := 0; i < 9; i++ {
				var tokenx security.UploadToken
				tokenx.Offset = uint8(copy(tokenx.Data[:], token.Data[:token.Offset]))
				char := mrand.Int31n(0xff)
				if char != int32(token.Data[i]) {
					tokenx.Data[i] = byte(char)
					require.False(t, tokenx.IsValid(cid, vid, bid, size, secretKey))
				}
			}
		}
		{ // encode decode
			str := security.EncodeToken(token)
			tokenx := security.DecodeToken(str)
			require.Equal(t, token.Offset, tokenx.Offset)
			require.Equal(t, token.Data[:token.Offset], tokenx.Data[:tokenx.Offset])
		}
	}
}

func TestAccessServerTokenExpired(t *testing.T) {
	secretKey := []byte{0x1f, 0xff}
	for ii := 0; ii < 1000; ii++ {
		expired := mrand.Intn(40) - 20
		token := security.NewUploadToken(1, 1, 1, 1, 1, time.Duration(expired)*time.Second, secretKey)
		if expired >= 0 {
			require.True(t, token.IsValid(1, 1, 1, 1, secretKey))
		} else {
			require.False(t, token.IsValid(1, 1, 1, 1, secretKey))
		}
	}
}

func BenchmarkAccessServerTokenNew(b *testing.B) {
	secretKey := []byte{}
	for ii := 0; ii <= b.N; ii++ {
		security.NewUploadToken(1, 1, 1, 1, 1, 1, secretKey)
	}
}

func BenchmarkAccessServerTokenValid(b *testing.B) {
	secretKey := []byte{}
	token := security.NewUploadToken(1, 1, 1, 1, 1, 1, secretKey)
	b.ResetTimer()
	for ii := 0; ii <= b.N; ii++ {
		token.IsValid(1, 1, 1, 1, secretKey)
	}
}

func BenchmarkAccessServerTokenEncode(b *testing.B) {
	secretKey := []byte{}
	token := security.NewUploadToken(1, 1, 1, 1, 1, 1, secretKey)
	b.ResetTimer()
	for ii := 0; ii <= b.N; ii++ {
		security.EncodeToken(token)
	}
}

func BenchmarkAccessServerTokenDecode(b *testing.B) {
	for ii := 0; ii <= b.N; ii++ {
		security.DecodeToken("d29f19731941e44a010100")
	}
}
