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

package blobstore

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cubefs/blobstore/api/access"
	"github.com/cubefs/blobstore/common/crc32block"
	"github.com/cubefs/blobstore/common/proto"
	"github.com/cubefs/blobstore/util/bytespool"
	cproto "github.com/cubefs/cubefs/proto"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"
)

const (
	blobSize = 1 << 20
)

var (
	client     access.API
	services   []*api.ServiceEntry
	hostsApply []*api.ServiceEntry
	dataCache  []byte
	blobCache  = make([]byte, blobSize)
	tokenAlloc = []byte("token")
	tokenPutat = []byte("token")

	partRandBroken = false
)

type MockEbsService struct {
	service *httptest.Server
}

func NewMockEbsService() *MockEbsService {
	dataCache = make([]byte, 1<<25)
	mockServer := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Path == "/v1/health/service/access" {
				w.WriteHeader(http.StatusOK)
				b, _ := json.Marshal(hostsApply)
				w.Write(b)

			} else if req.URL.Path == "/put" {
				putSize := req.URL.Query().Get("size")
				// just for testing timeout
				if strings.HasPrefix(putSize, "-") {
					time.Sleep(30 * time.Second)
					w.WriteHeader(http.StatusForbidden)
					return
				}

				dataSize, _ := strconv.Atoi(putSize)
				size := req.Header.Get("Content-Length")
				l, _ := strconv.Atoi(size)

				w.Header().Set("X-Ack-Crc-Encoded", "1")
				w.WriteHeader(http.StatusOK)

				body := crc32block.NewDecoderReader(req.Body)
				dataCache = dataCache[:cap(dataCache)]
				dataCache = dataCache[:crc32block.DecodeSizeWithDefualtBlock(int64(l))]
				io.ReadFull(body, dataCache)

				hashesStr := req.URL.Query().Get("hashes")
				algsInt, _ := strconv.Atoi(hashesStr)
				algs := access.HashAlgorithm(algsInt)

				hashSumMap := algs.ToHashSumMap()
				for alg := range hashSumMap {
					hasher := alg.ToHasher()
					hasher.Write(dataCache)
					hashSumMap[alg] = hasher.Sum(nil)
				}

				loc := access.Location{Size: uint64(dataSize)}
				fillCrc(&loc)
				resp := access.PutResp{
					Location:   loc,
					HashSumMap: hashSumMap,
				}
				b, _ := json.Marshal(resp)
				w.Write(b)

			} else if req.URL.Path == "/get" {
				var args access.GetArgs
				requestBody(req, &args)
				if !verifyCrc(&args.Location) {
					w.WriteHeader(http.StatusForbidden)
					return
				}

				data := make([]byte, args.ReadSize)
				w.Header().Set("Content-Length", strconv.Itoa(len(data)))
				w.WriteHeader(http.StatusOK)
				w.Write(data)

			} else if req.URL.Path == "/delete" {
				args := access.DeleteArgs{}
				requestBody(req, &args)
				if !args.IsValid() {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				for _, loc := range args.Locations {
					if !verifyCrc(&loc) {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
				}

				if len(args.Locations) > 0 && len(args.Locations)%2 == 0 {
					locs := args.Locations[:]
					b, _ := json.Marshal(access.DeleteResp{FailedLocations: locs})
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Length", strconv.Itoa(len(b)))
					w.WriteHeader(http.StatusIMUsed)
					w.Write(b)
					return
				}

				b, _ := json.Marshal(access.DeleteResp{})
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Content-Length", strconv.Itoa(len(b)))
				w.WriteHeader(http.StatusOK)
				w.Write(b)

			} else if req.URL.Path == "/sign" {
				args := access.SignArgs{}
				requestBody(req, &args)
				if err := signCrc(&args.Location, args.Locations); err != nil {
					w.WriteHeader(http.StatusForbidden)
					return
				}

				b, _ := json.Marshal(access.SignResp{Location: args.Location})
				w.WriteHeader(http.StatusOK)
				w.Write(b)

			} else {
				w.WriteHeader(http.StatusOK)
			}
		}))

	return &MockEbsService{
		service: mockServer,
	}
}

func requestBody(req *http.Request, val interface{}) {
	l := req.Header.Get("Content-Length")
	size, _ := strconv.Atoi(l)
	data := make([]byte, size)
	io.ReadFull(req.Body, data)
	json.Unmarshal(data, val)
}

func calcCrc(loc *access.Location) (uint32, error) {
	crcWriter := crc32.New(crc32.IEEETable)

	buf := bytespool.Alloc(1024)
	defer bytespool.Free(buf)

	n := loc.Encode2(buf)
	if n < 4 {
		return 0, fmt.Errorf("no enough bytes(%d) fill into buf", n)
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

func TestEbsClient_Write_Read(t *testing.T) {
	cfg := access.Config{}
	mockServer := NewMockEbsService()
	cfg.Consul.Address = mockServer.service.URL[7:] // strip http://
	cfg.PriorityAddrs = []string{mockServer.service.URL}
	cfg.ConnMode = access.QuickConnMode
	cfg.MaxSizePutOnce = 1 << 20

	blobStoreClient, err := NewEbsClient(cfg)
	if err != nil {
		panic(err)
	}
	var testCases = []struct {
		size int
	}{
		{1},
		{1023},
		{1 << 10},
		{1 << 20},
	}
	for _, tc := range testCases {
		data := make([]byte, tc.size)
		ctx := context.Background()
		location, err := blobStoreClient.Write(ctx, "testVol", data, uint32(tc.size))
		require.Exactly(t, nil, err)

		//read prepare
		blobs := make([]cproto.Blob, 0)
		for _, info := range location.Blobs {
			blob := cproto.Blob{
				MinBid: uint64(info.MinBid),
				Count:  uint64(info.Count),
				Vid:    uint64(info.Vid),
			}
			blobs = append(blobs, blob)
		}
		oek := cproto.ObjExtentKey{
			Cid:      uint64(location.ClusterID),
			CodeMode: uint8(location.CodeMode),
			Size:     location.Size,
			BlobSize: location.BlobSize,
			Blobs:    blobs,
			BlobsLen: uint32(len(blobs)),
			Crc:      location.Crc,
		}
		buf := make([]byte, oek.Size)
		read, err := blobStoreClient.Read(ctx, "", buf, 0, oek.Size, oek)
		require.NoError(t, err)
		require.Exactly(t, tc.size, read)
	}

}
