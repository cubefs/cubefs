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

package access_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"io"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"

	"github.com/cubefs/cubefs/blobstore/api/access"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/common/uptoken"
	_ "github.com/cubefs/cubefs/blobstore/testing/nolog"
	"github.com/cubefs/cubefs/blobstore/util/bytespool"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	blobSize = 1 << 20
)

type dataCacheT struct {
	mu   sync.Mutex
	data map[proto.BlobID][]byte
}

func (c *dataCacheT) clean() {
	c.mu.Lock()
	c.data = make(map[proto.BlobID][]byte, len(c.data))
	c.mu.Unlock()
}

func (c *dataCacheT) put(bid proto.BlobID, b []byte) {
	c.mu.Lock()
	c.data[bid] = b
	c.mu.Unlock()
}

func (c *dataCacheT) get(bid proto.BlobID) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[bid]
}

var (
	mockServer *httptest.Server
	client     access.API
	dataCache  *dataCacheT
	services   []*api.ServiceEntry
	hostsApply []*api.ServiceEntry
	tokenAlloc = []byte("token")
	tokenPutat = []byte("token")

	partRandBroken = false
)

func init() {
	t := time.Now()
	services = make([]*api.ServiceEntry, 0, 2)
	services = append(services, &api.ServiceEntry{
		Node: &api.Node{
			ID: "unreachable",
		},
		Service: &api.AgentService{
			Service: "access",
			Address: "127.0.0.1",
			Port:    9997,
		},
	})

	dataCache = &dataCacheT{}
	dataCache.clean()

	rpc.RegisterArgsParser(&access.PutArgs{}, "json")
	rpc.RegisterArgsParser(&access.PutAtArgs{}, "json")

	handler := rpc.New()
	handler.Handle(http.MethodGet, "/v1/health/service/access", handleService)
	handler.Handle(http.MethodPost, "/alloc", handleAlloc, rpc.OptArgsBody())
	handler.Handle(http.MethodPut, "/put", handlePut, rpc.OptArgsQuery())
	handler.Handle(http.MethodPut, "/putat", handlePutAt, rpc.OptArgsQuery())
	handler.Handle(http.MethodPost, "/get", handleGet, rpc.OptArgsBody())
	handler.Handle(http.MethodPost, "/delete", handleDelete, rpc.OptArgsBody())
	handler.Handle(http.MethodPost, "/sign", handleSign, rpc.OptArgsBody())
	handler.Router.NotFound = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mockServer = httptest.NewServer(handler)

	u := strings.Split(mockServer.URL[7:], ":")
	port, _ := strconv.Atoi(u[1])
	services = append(services, &api.ServiceEntry{
		Node: &api.Node{
			ID: "mockServer",
		},
		Service: &api.AgentService{
			Service: "access",
			Address: u[0],
			Port:    port,
		},
	})
	hostsApply = services

	cfg := access.Config{}
	cfg.PriorityAddrs = []string{mockServer.URL}
	cfg.ConnMode = access.QuickConnMode
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	cfg.LogLevel = log.Lfatal
	cli, err := access.New(cfg)
	if err != nil {
		panic(err)
	}
	client = cli

	mrand.Seed(int64(time.Since(t)))
}

func handleService(c *rpc.Context) {
	c.RespondJSON(hostsApply)
}

func handleAlloc(c *rpc.Context) {
	args := new(access.AllocArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	loc := access.Location{
		ClusterID: 1,
		Size:      args.Size,
		BlobSize:  blobSize,
		Blobs: []access.SliceInfo{
			{
				MinBid: proto.BlobID(mrand.Int()),
				Vid:    proto.Vid(mrand.Int()),
				Count:  uint32((args.Size + blobSize - 1) / blobSize),
			},
		},
	}
	// split to two blobs if large enough
	if loc.Blobs[0].Count > 2 {
		loc.Blobs[0].Count = 2
		loc.Blobs = append(loc.Blobs, []access.SliceInfo{
			{
				MinBid: proto.BlobID(mrand.Int()),
				Vid:    proto.Vid(mrand.Int()),
				Count:  uint32((args.Size - 2*blobSize + blobSize - 1) / blobSize),
			},
		}...)
	}
	// alloc the rest parts
	if args.AssignClusterID > 0 {
		loc.Blobs = []access.SliceInfo{
			{
				MinBid: proto.BlobID(mrand.Int()),
				Vid:    proto.Vid(mrand.Int()),
				Count:  uint32((args.Size + blobSize - 1) / blobSize),
			},
		}
	}

	tokens := make([]string, 0, len(loc.Blobs)+1)

	hasMultiBlobs := loc.Size >= uint64(loc.BlobSize)
	lastSize := uint32(loc.Size % uint64(loc.BlobSize))
	for idx, blob := range loc.Blobs {
		// returns one token if size < blobsize
		if hasMultiBlobs {
			count := blob.Count
			if idx == len(loc.Blobs)-1 && lastSize > 0 {
				count--
			}
			tokens = append(tokens, uptoken.EncodeToken(uptoken.NewUploadToken(loc.ClusterID,
				blob.Vid, blob.MinBid, count,
				loc.BlobSize, 0, tokenAlloc[:])))
		}

		// token of the last blob
		if idx == len(loc.Blobs)-1 && lastSize > 0 {
			tokens = append(tokens, uptoken.EncodeToken(uptoken.NewUploadToken(loc.ClusterID,
				blob.Vid, blob.MinBid+proto.BlobID(blob.Count)-1, 1,
				lastSize, 0, tokenAlloc[:])))
		}
	}

	fillCrc(&loc)
	c.RespondJSON(access.AllocResp{
		Location: loc,
		Tokens:   tokens,
	})
}

func handlePut(c *rpc.Context) {
	args := new(access.PutArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	// just for testing timeout
	if args.Size < 0 {
		time.Sleep(30 * time.Second)
		c.RespondStatus(http.StatusForbidden)
		return
	}

	buf := make([]byte, args.Size)
	_, err := io.ReadFull(c.Request.Body, buf)
	if err != nil {
		c.RespondError(errcode.ErrAccessReadRequestBody)
		return
	}
	dataCache.put(0, buf)

	hashSumMap := args.Hashes.ToHashSumMap()
	for alg := range hashSumMap {
		hasher := alg.ToHasher()
		hasher.Write(buf)
		hashSumMap[alg] = hasher.Sum(nil)
	}

	loc := access.Location{Size: uint64(args.Size)}
	fillCrc(&loc)
	c.RespondJSON(access.PutResp{
		Location:   loc,
		HashSumMap: hashSumMap,
	})
}

func handlePutAt(c *rpc.Context) {
	args := new(access.PutAtArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if partRandBroken && args.BlobID%3 == 0 { // random broken
		c.RespondStatus(http.StatusForbidden)
		return
	}

	token := uptoken.DecodeToken(args.Token)
	if !token.IsValid(args.ClusterID, args.Vid, args.BlobID, uint32(args.Size), tokenPutat[:]) {
		c.RespondStatus(http.StatusForbidden)
		return
	}

	blobBuf := make([]byte, args.Size)
	io.ReadFull(c.Request.Body, blobBuf)

	hashSumMap := args.Hashes.ToHashSumMap()
	for alg := range hashSumMap {
		hasher := alg.ToHasher()
		hasher.Write(blobBuf)
		hashSumMap[alg] = hasher.Sum(nil)
	}

	dataCache.put(args.BlobID, blobBuf)
	c.RespondJSON(access.PutAtResp{HashSumMap: hashSumMap})
}

func handleGet(c *rpc.Context) {
	args := new(access.GetArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if args.Location.Size == 100 {
		c.RespondStatus(http.StatusBadRequest)
		return
	}

	if !verifyCrc(&args.Location) {
		c.RespondStatus(http.StatusForbidden)
		return
	}

	c.Writer.Header().Set(rpc.HeaderContentLength, strconv.Itoa(int(args.ReadSize)))
	c.RespondStatus(http.StatusOK)
	if buf := dataCache.get(0); len(buf) > 0 {
		c.Writer.Write(buf)
	} else {
		for _, blob := range args.Location.Spread() {
			c.Writer.Write(dataCache.get(blob.Bid))
		}
	}
}

func handleDelete(c *rpc.Context) {
	args := new(access.DeleteArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if !args.IsValid() {
		c.RespondStatus(http.StatusBadRequest)
		return
	}
	for _, loc := range args.Locations {
		if !verifyCrc(&loc) {
			c.RespondStatus(http.StatusBadRequest)
			return
		}
	}

	if len(args.Locations) > 0 && len(args.Locations)%2 == 0 {
		locs := args.Locations[:]
		c.RespondStatusData(http.StatusIMUsed, access.DeleteResp{FailedLocations: locs})
		return
	}
	c.RespondJSON(access.DeleteResp{})
}

func handleSign(c *rpc.Context) {
	args := new(access.SignArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	if err := signCrc(&args.Location, args.Locations); err != nil {
		c.RespondStatus(http.StatusForbidden)
		return
	}
	c.RespondJSON(access.SignResp{Location: args.Location})
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

type stringid struct{ id string }

func (s stringid) String() string { return s.id }

type traceid struct{ id string }

func (t traceid) TraceID() string { return t.id }

type requestid struct{ id string }

func (r requestid) RequestID() string { return r.id }

func randCtx() context.Context {
	ctx := context.Background()

	switch mrand.Int31() % 7 {
	case 0:
		return ctx
	case 1:
		return access.WithRequestID(ctx, nil)
	case 2:
		return access.WithRequestID(ctx, "TestAccessClient-string")
	case 3:
		return access.WithRequestID(ctx, stringid{"TestAccessClient-String"})
	case 4:
		return access.WithRequestID(ctx, traceid{"TestAccessClient-TraceID"})
	case 5:
		return access.WithRequestID(ctx, requestid{"TestAccessClient-RequestID"})
	case 6:
		return access.WithRequestID(ctx, struct{}{})
	default:
	}

	_, ctx = trace.StartSpanFromContext(ctx, "TestAccessClient")
	return ctx
}

func TestAccessClientConnectionMode(t *testing.T) {
	cases := []struct {
		mode access.RPCConnectMode
		size int64
	}{
		{0, 0},
		{0, -1},
		{access.QuickConnMode, 1 << 10},
		{access.GeneralConnMode, 1 << 10},
		{access.SlowConnMode, 1 << 10},
		{access.NoLimitConnMode, 1 << 10},
		{access.DefaultConnMode, 1 << 10},
		{100, 1 << 10},
		{0xff, 100},
	}

	for _, cs := range cases {
		cfg := access.Config{}
		cfg.PriorityAddrs = []string{mockServer.URL}
		cfg.MaxSizePutOnce = cs.size
		cfg.ConnMode = cs.mode
		cfg.LogLevel = log.Lfatal
		cli, err := access.New(cfg)
		require.NoError(t, err)

		if cs.size <= 0 {
			continue
		}
		loc := access.Location{Size: uint64(mrand.Int63n(cs.size))}
		fillCrc(&loc)
		_, err = cli.Delete(randCtx(), &access.DeleteArgs{
			Locations: []access.Location{loc},
		})
		require.NoError(t, err)
	}
}

func TestAccessClientPutGet(t *testing.T) {
	cases := []struct {
		size int
	}{
		{0},
		{1},
		{1 << 6},
		{1 << 10},
		{1 << 19},
		{(1 << 19) + 1},
		{1 << 20},
	}
	for _, cs := range cases {
		buff := make([]byte, cs.size)
		rand.Read(buff)
		crcExpected := crc32.ChecksumIEEE(buff)
		args := access.PutArgs{
			Size:   int64(cs.size),
			Hashes: access.HashAlgCRC32,
			Body:   bytes.NewBuffer(buff),
		}

		loc, hashSumMap, err := client.Put(randCtx(), &args)
		crc, _ := hashSumMap.GetSum(access.HashAlgCRC32)
		require.NoError(t, err)
		require.Equal(t, crcExpected, crc)

		body, err := client.Get(randCtx(), &access.GetArgs{Location: loc, ReadSize: uint64(cs.size)})
		require.NoError(t, err)
		defer body.Close()

		io.ReadFull(body, buff)
		require.Equal(t, crcExpected, crc32.ChecksumIEEE(buff))
	}

	// test code 400
	_, err := client.Get(randCtx(), &access.GetArgs{Location: access.Location{Size: 100}, ReadSize: uint64(100)})
	require.Error(t, err)
}

func TestAccessClientPutAtBase(t *testing.T) {
	cfg := access.Config{}
	cfg.Consul.Address = mockServer.URL[7:]
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	cfg.LogLevel = log.Lfatal
	client, err := access.New(cfg)
	require.NoError(t, err)

	cases := []struct {
		size int
	}{
		{0},
		{1},
		{1 << 10},
		{(1 << 19) + 1},
		{1 << 20},
		{1<<20 + 1023},
		{1<<23 + 1023},
		{1 << 24},
	}
	for _, cs := range cases {
		dataCache.clean()

		buff := make([]byte, cs.size)
		rand.Read(buff)
		crcExpected := crc32.ChecksumIEEE(buff)
		args := access.PutArgs{
			Size:   int64(cs.size),
			Hashes: access.HashAlgCRC32,
			Body:   bytes.NewBuffer(buff),
		}

		loc, hashSumMap, err := client.Put(randCtx(), &args)
		crc, _ := hashSumMap.GetSum(access.HashAlgCRC32)
		require.NoError(t, err)
		require.Equal(t, crcExpected, crc)

		body, err := client.Get(randCtx(), &access.GetArgs{Location: loc, ReadSize: uint64(cs.size)})
		require.NoError(t, err)
		defer body.Close()

		io.ReadFull(body, buff)
		require.Equal(t, crcExpected, crc32.ChecksumIEEE(buff))
	}
}

func TestAccessServiceUnavailable(t *testing.T) {
	cfg := access.Config{}
	cfg.Consul.Address = ""
	cfg.PriorityAddrs = []string{}
	_, err := access.New(cfg)
	require.Equal(t, errcode.ErrAccessServiceDiscovery, err)
}

func TestAccessClientPutAtMerge(t *testing.T) {
	cfg := access.Config{}
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	cfg.LogLevel = log.Lfatal
	cfg.PriorityAddrs = []string{mockServer.URL}
	client, err := access.New(cfg)
	require.NoError(t, err)

	partRandBroken = true
	defer func() {
		partRandBroken = false
	}()
	dataCache.clean()

	cases := []struct {
		size int
	}{
		{1<<21 + 77777},
		{1<<21 + 77777},
		{1<<21 + 77777},
		{1 << 22},
		{1 << 22},
		{1 << 22},
	}
	for _, cs := range cases {
		dataCache.clean()

		size := cs.size
		buff := make([]byte, size)
		rand.Read(buff)
		crcExpected := crc32.ChecksumIEEE(buff)
		args := access.PutArgs{
			Size:   int64(size),
			Hashes: access.HashAlgCRC32,
			Body:   bytes.NewBuffer(buff),
		}

		loc, hashSumMap, err := client.Put(randCtx(), &args)
		crc, _ := hashSumMap.GetSum(access.HashAlgCRC32)
		require.NoError(t, err)
		require.Equal(t, crcExpected, crc)

		body, err := client.Get(randCtx(), &access.GetArgs{Location: loc, ReadSize: uint64(size)})
		require.NoError(t, err)
		defer body.Close()

		io.ReadFull(body, buff)
		require.Equal(t, crcExpected, crc32.ChecksumIEEE(buff))
	}
}

func TestAccessClientPutMaxBlobsLength(t *testing.T) {
	cfg := access.Config{}
	cfg.PriorityAddrs = []string{mockServer.URL}
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	cfg.LogLevel = log.Lfatal
	client, err := access.New(cfg)
	require.NoError(t, err)

	partRandBroken = true
	defer func() {
		partRandBroken = false
	}()

	cases := []struct {
		size int
		err  error
	}{
		{1 << 20, nil},
		{(1 << 21) + 1023, nil},
		{1 << 25, errcode.ErrUnexpected},
	}
	for _, cs := range cases {
		dataCache.clean()

		buff := make([]byte, cs.size)
		rand.Read(buff)
		args := access.PutArgs{
			Size:   int64(cs.size),
			Hashes: access.HashAlgDummy,
			Body:   bytes.NewBuffer(buff),
		}

		_, _, err := client.Put(randCtx(), &args)
		require.ErrorIs(t, cs.err, err)
	}
}

func linearTimeoutMs(baseSec, size, speedMBps float64) int64 {
	const alignMs = 500
	spentSec := size / 1024.0 / 1024.0 / speedMBps
	timeoutMs := int64((baseSec + spentSec) * 1000)
	if timeoutMs < 0 {
		timeoutMs = 0
	}
	if ms := timeoutMs % alignMs; ms > 0 {
		timeoutMs += alignMs - ms
	}
	return timeoutMs
}

func TestAccessClientPutTimeout(t *testing.T) {
	cfg := access.Config{}
	cfg.PriorityAddrs = []string{mockServer.URL}
	cfg.LogLevel = log.Lfatal
	cfg.MaxHostRetry = 1

	mb := int(1 << 20)
	ms := time.Millisecond

	type caseT struct {
		mode  access.RPCConnectMode
		size  int
		minMs time.Duration
		maxMs time.Duration
	}
	cases := []caseT{
		// QuickConnMode 3s + size / 40, dial and response 2s
		{access.QuickConnMode, mb * -119, ms * 0, ms * 600},
		{access.QuickConnMode, mb * -100, ms * 500, ms * 1000},
		{access.QuickConnMode, mb * -80, ms * 1000, ms * 1500},
		{access.QuickConnMode, mb * -1, ms * 2000, ms * 2500},

		// DefaultConnMode 30s + size / 10, dial and response 5s
		{access.DefaultConnMode, mb * -299, ms * 0, ms * 600},
		{access.DefaultConnMode, mb * -280, ms * 2000, ms * 2500},
		{access.DefaultConnMode, mb * -270, ms * 3000, ms * 3500},
		{access.DefaultConnMode, mb * -1, ms * 5000, ms * 5500},
	}

	var wg sync.WaitGroup
	wg.Add(len(cases))

	run := func(cfg access.Config, cs caseT) {
		defer wg.Done()
		cfg.ConnMode = cs.mode
		switch cs.mode {
		case access.QuickConnMode:
			cfg.ClientTimeoutMs = linearTimeoutMs(3, float64(cs.size), 40)
		case access.DefaultConnMode:
			cfg.ClientTimeoutMs = linearTimeoutMs(30, float64(cs.size), 10)
		}
		client, err := access.New(cfg)
		require.NoError(t, err)

		buff := make([]byte, 0)
		args := access.PutArgs{
			Size: int64(cs.size),
			Body: bytes.NewBuffer(buff),
		}

		startTime := time.Now()
		_, _, err = client.Put(randCtx(), &args)
		require.Error(t, err)
		duration := time.Since(startTime)
		require.GreaterOrEqual(t, cs.maxMs, duration, "greater duration: ", duration)
		require.LessOrEqual(t, cs.minMs, duration, "less duration: ", duration)
	}

	for _, cs := range cases {
		go run(cfg, cs)
	}
	wg.Wait()
}

func TestAccessClientDelete(t *testing.T) {
	{
		locs, err := client.Delete(randCtx(), nil)
		require.Nil(t, locs)
		require.ErrorIs(t, errcode.ErrIllegalArguments, err)
	}
	{
		locs, err := client.Delete(randCtx(), &access.DeleteArgs{})
		require.Nil(t, locs)
		require.ErrorIs(t, errcode.ErrIllegalArguments, err)
	}
	{
		locs, err := client.Delete(randCtx(), &access.DeleteArgs{
			Locations: make([]access.Location, 1),
		})
		require.Nil(t, locs)
		require.NoError(t, err)
	}
	{
		args := &access.DeleteArgs{
			Locations: make([]access.Location, 1000),
		}
		_, err := client.Delete(randCtx(), args)
		require.NoError(t, err)
	}
	{
		args := &access.DeleteArgs{
			Locations: make([]access.Location, access.MaxDeleteLocations+1),
		}
		locs, err := client.Delete(randCtx(), args)
		require.Equal(t, args.Locations, locs)
		require.ErrorIs(t, errcode.ErrIllegalArguments, err)
	}
	{
		loc := access.Location{Size: 100, Blobs: make([]access.SliceInfo, 0)}
		fillCrc(&loc)
		args := &access.DeleteArgs{
			Locations: make([]access.Location, 0, access.MaxDeleteLocations),
		}
		for i := 1; i < access.MaxDeleteLocations/10; i++ {
			args.Locations = append(args.Locations, loc)
			locs, err := client.Delete(randCtx(), args)
			if i%2 == 0 {
				require.Equal(t, args.Locations, locs)
				require.Equal(t, errcode.ErrUnexpected, err)
			} else {
				require.NoError(t, err)
			}
		}
	}
}

func TestAccessClientRequestBody(t *testing.T) {
	cfg := access.Config{}
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 2
	cfg.LogLevel = log.Lfatal
	cfg.PriorityAddrs = []string{mockServer.URL}
	client, err := access.New(cfg)
	require.NoError(t, err)

	cases := []struct {
		size int
	}{
		{0},
		{1},
		{1 << 10},
		{(1 << 19) + 1},
		{1 << 20},
		{1<<20 + 1023},
		{1<<23 + 1023},
		{1 << 24},
	}
	for _, cs := range cases {
		dataCache.clean()

		buff := make([]byte, cs.size)
		rand.Read(buff)
		args := access.PutArgs{
			Size: int64(cs.size) + 1,
			Body: bytes.NewBuffer(buff),
		}

		_, _, err := client.Put(randCtx(), &args)
		require.Equal(t, errcode.CodeAccessReadRequestBody, rpc.DetectStatusCode(err))
	}
}

func TestAccessClientPutAtToken(t *testing.T) {
	tokenKey := tokenPutat
	defer func() {
		tokenPutat = tokenKey
	}()

	cfg := access.Config{}
	cfg.MaxSizePutOnce = 1 << 20
	cfg.PartConcurrence = 1
	cfg.MaxPartRetry = -1
	cfg.LogLevel = log.Lfatal
	cfg.PriorityAddrs = []string{mockServer.URL}
	client, err := access.New(cfg)
	require.NoError(t, err)
	cases := []struct {
		keyLen int
	}{
		{0},
		{1},
		{20},
		{100},
	}
	buff := make([]byte, 1<<21)
	rand.Read(buff)
	for _, cs := range cases {
		tokenPutat = make([]byte, cs.keyLen)
		rand.Read(tokenPutat)
		args := access.PutArgs{
			Size: int64(1 << 21),
			Body: bytes.NewBuffer(buff),
		}
		_, _, err := client.Put(randCtx(), &args)
		require.ErrorIs(t, errcode.ErrUnexpected, err)
	}
}

func TestAccessClientRPCConfig(t *testing.T) {
	cfg := access.Config{}
	cfg.RPCConfig = &rpc.Config{}
	cfg.LogLevel = log.Lfatal
	cfg.PriorityAddrs = []string{mockServer.URL}
	client, err := access.New(cfg)
	require.NoError(t, err)
	cases := []struct {
		size int
	}{
		{0},
		{1},
		{1 << 6},
		{1 << 10},
	}
	for _, cs := range cases {
		buff := make([]byte, cs.size)
		rand.Read(buff)
		args := access.PutArgs{
			Size: int64(cs.size),
			Body: bytes.NewBuffer(buff),
		}
		loc, _, err := client.Put(randCtx(), &args)
		require.NoError(t, err)
		_, err = client.Get(randCtx(), &access.GetArgs{Location: loc, ReadSize: uint64(cs.size)})
		require.NoError(t, err)
	}
}

func TestAccessClientLogger(t *testing.T) {
	file, err := os.CreateTemp(os.TempDir(), "TestAccessClientLogger")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	defer func() {
		os.Remove(file.Name())
	}()

	cfg := access.Config{
		LogLevel: log.Lfatal,
		Logger: &access.Logger{
			Filename: file.Name(),
		},
		PriorityAddrs: []string{"127.0.0.1:9500"},
	}
	client, err := access.New(cfg)
	require.NoError(t, err)
	defer func() {
		cfg.Logger.Close()
	}()

	size := 1024
	args := access.PutArgs{
		Size: int64(size),
		Body: bytes.NewBuffer(make([]byte, size-1)),
	}
	_, _, err = client.Put(randCtx(), &args)
	require.Error(t, err)
}
