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

package access

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
	"gopkg.in/natefinch/lumberjack.v2"

	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

const (
	defaultMaxSizePutOnce    int64 = 1 << 28 // 256MB
	defaultMaxPartRetry      int   = 3
	defaultMaxHostRetry      int   = 3
	defaultPartConcurrence   int   = 4
	defaultServiceIntervalMs int64 = 5000
	defaultServiceName             = "access"
)

// RPCConnectMode self-defined rpc client connection config setting
type RPCConnectMode uint8

// timeout: [short - - - - - - - - -> long]
//       quick --> general --> default --> slow --> nolimit
// speed: 40MB -->  20MB   -->  10MB   --> 4MB  --> nolimit
const (
	DefaultConnMode RPCConnectMode = iota
	QuickConnMode
	GeneralConnMode
	SlowConnMode
	NoLimitConnMode
)

func (mode RPCConnectMode) getConfig(speed float64, timeout, baseTimeout int64) rpc.Config {
	getSpeed := func(defaultVal float64) float64 {
		if speed > 0 {
			return speed
		}
		return defaultVal
	}
	getBaseTimeout := func(defaultVal int64) int64 {
		if baseTimeout > 0 {
			return baseTimeout
		}
		return defaultVal
	}
	getTimeout := func(speed float64) int64 {
		if timeout > 0 {
			return timeout
		}
		return 5 * (1 << 30) * 1e3 / int64(speed*(1<<20))
	}

	config := rpc.Config{
		// the whole request and response timeout
		ClientTimeoutMs:   getTimeout(getSpeed(10)),
		BodyBandwidthMBPs: getSpeed(10),
		BodyBaseTimeoutMs: getBaseTimeout(30 * 1000),
		Tc: rpc.TransportConfig{
			// dial timeout
			DialTimeoutMs: 5 * 1000,
			// response header timeout after send the request
			ResponseHeaderTimeoutMs: 5 * 1000,
			// IdleConnTimeout is the maximum amount of time an idle
			// (keep-alive) connection will remain idle before closing
			// itself.Zero means no limit.
			IdleConnTimeoutMs: 30 * 1000,

			MaxIdleConns:        0,
			MaxConnsPerHost:     2048,
			MaxIdleConnsPerHost: 1024,
			DisableCompression:  true,
		},
	}

	switch mode {
	case QuickConnMode:
		config.ClientTimeoutMs = getTimeout(getSpeed(40))
		config.BodyBandwidthMBPs = getSpeed(40)
		config.BodyBaseTimeoutMs = getBaseTimeout(3 * 1000)
		config.Tc.DialTimeoutMs = 2 * 1000
		config.Tc.ResponseHeaderTimeoutMs = 2 * 1000
		config.Tc.IdleConnTimeoutMs = 10 * 1000
	case GeneralConnMode:
		config.ClientTimeoutMs = getTimeout(getSpeed(20))
		config.BodyBandwidthMBPs = getSpeed(20)
		config.BodyBaseTimeoutMs = getBaseTimeout(10 * 1000)
		config.Tc.DialTimeoutMs = 3 * 1000
		config.Tc.ResponseHeaderTimeoutMs = 3 * 1000
		config.Tc.IdleConnTimeoutMs = 30 * 1000
	case SlowConnMode:
		config.ClientTimeoutMs = getTimeout(getSpeed(4))
		config.BodyBandwidthMBPs = getSpeed(4)
		config.BodyBaseTimeoutMs = getBaseTimeout(120 * 1000)
		config.Tc.DialTimeoutMs = 10 * 1000
		config.Tc.ResponseHeaderTimeoutMs = 10 * 1000
		config.Tc.IdleConnTimeoutMs = 60 * 1000
	case NoLimitConnMode:
		config.ClientTimeoutMs = 0
		config.BodyBandwidthMBPs = getSpeed(0)
		config.BodyBaseTimeoutMs = getBaseTimeout(0)
		config.Tc.DialTimeoutMs = 0
		config.Tc.ResponseHeaderTimeoutMs = 0
		config.Tc.IdleConnTimeoutMs = 600 * 1000
	default:
	}

	return config
}

// Config access client config
type Config struct {
	// ConnMode rpc connection timeout setting
	ConnMode RPCConnectMode
	// ClientTimeoutMs the whole request and response timeout
	ClientTimeoutMs int64
	// BodyBandwidthMBPs reading body timeout, request or response
	//   timeout = ContentLength/BodyBandwidthMBPs + BodyBaseTimeoutMs
	BodyBandwidthMBPs float64
	// BodyBaseTimeoutMs base timeout for read body
	BodyBaseTimeoutMs int64

	// Consul is consul config for discovering service
	Consul ConsulConfig
	// ServiceIntervalMs is interval ms for discovering service
	ServiceIntervalMs int64
	// PriorityAddrs priority addrs of access service when retry
	PriorityAddrs []string
	// MaxSizePutOnce max size using once-put object interface
	MaxSizePutOnce int64
	// MaxPartRetry max retry times when putting one part, 0 means forever
	MaxPartRetry int
	// MaxHostRetry max retry hosts of access service
	MaxHostRetry int
	// PartConcurrence concurrence of put parts
	PartConcurrence int

	// rpc selector config
	// Failure retry interval, default value is -1, if FailRetryIntervalS < 0,
	// remove failed hosts will not work.
	FailRetryIntervalS int
	// Within MaxFailsPeriodS, if the number of failures is greater than or equal to MaxFails,
	// the host is considered disconnected.
	MaxFailsPeriodS int
	// HostTryTimes Number of host failure retries
	HostTryTimes int

	// RPCConfig user-defined rpc config
	// All connections will use the config if it's not nil
	// ConnMode will be ignored if rpc config is setting
	RPCConfig *rpc.Config

	// LogLevel client output logging level.
	LogLevel log.Level

	// Logger trace all logging to the logger if setting.
	// It is an io.WriteCloser that writes to the specified filename.
	// YOU should CLOSE it after you do not use the client anymore.
	Logger *Logger
}

// ConsulConfig alias of consul api.Config
// Fixup: client and sdk using the same config type
type ConsulConfig = api.Config

// Logger alias of lumberjack Logger
// See more at: https://github.com/natefinch/lumberjack
type Logger = lumberjack.Logger

// client access rpc client
type client struct {
	config    Config
	rpcClient atomic.Value
	stop      chan struct{}
}

// API access api for s3
// To trace request id, the ctx is better WithRequestID(ctx, rid).
type API interface {
	// Put object once if size is not greater than MaxSizePutOnce, otherwise put blobs one by one.
	// return a location and map of hash summary bytes you excepted.
	//
	// If PutArgs' body is of type *bytes.Buffer, *bytes.Reader, or *strings.Reader,
	// GetBody is populated, then the Put once request has retry ability.
	Put(ctx context.Context, args *PutArgs) (location Location, hashSumMap HashSumMap, err error)
	// Get object, range is supported.
	Get(ctx context.Context, args *GetArgs) (body io.ReadCloser, err error)
	// Delete all blobs in these locations.
	// return failed locations which have yet been deleted if error is not nil.
	Delete(ctx context.Context, args *DeleteArgs) (failedLocations []Location, err error)
}

var _ API = (*client)(nil)

type noopBody struct{}

var _ io.ReadCloser = (*noopBody)(nil)

func (rc noopBody) Read(p []byte) (n int, err error) { return 0, io.EOF }
func (rc noopBody) Close() error                     { return nil }

var memPool *resourcepool.MemPool

func init() {
	memPool = resourcepool.NewMemPool(map[int]int{
		1 << 12: -1,
		1 << 14: -1,
		1 << 18: -1,
		1 << 20: -1,
		1 << 22: -1,
		1 << 23: -1,
		1 << 24: -1,
	})
}

// New returns an access API
func New(cfg Config) (API, error) {
	defaulter.LessOrEqual(&cfg.MaxSizePutOnce, defaultMaxSizePutOnce)
	defaulter.Less(&cfg.MaxPartRetry, defaultMaxPartRetry)
	defaulter.LessOrEqual(&cfg.MaxHostRetry, defaultMaxHostRetry)
	defaulter.LessOrEqual(&cfg.PartConcurrence, defaultPartConcurrence)
	if cfg.ServiceIntervalMs < 500 {
		cfg.ServiceIntervalMs = defaultServiceIntervalMs
	}

	log.SetOutputLevel(cfg.LogLevel)
	if cfg.Logger != nil {
		log.SetOutput(cfg.Logger)
	}

	c := &client{
		config: cfg,
		stop:   make(chan struct{}),
	}

	runtime.SetFinalizer(c, func(c *client) {
		rpcClient, ok := c.rpcClient.Load().(rpc.Client)
		if ok {
			rpcClient.Close()
		}
		close(c.stop)
	})

	if cfg.Consul.Address == "" {
		if len(cfg.PriorityAddrs) < 1 {
			return nil, errcode.ErrAccessServiceDiscovery
		}
		c.rpcClient.Store(getClient(&cfg, cfg.PriorityAddrs))
		return c, nil
	}

	consulConfig := cfg.Consul
	consulClient, err := api.NewClient(&consulConfig)
	if err != nil {
		return nil, errcode.ErrAccessServiceDiscovery
	}

	first := true
	serviceName := defaultServiceName
	hostGetter := func() ([]string, error) {
		if first && len(cfg.PriorityAddrs) > 0 {
			hosts := make([]string, len(cfg.PriorityAddrs))
			copy(hosts, cfg.PriorityAddrs[:])
			first = false
			return hosts, nil
		}
		services, _, err := consulClient.Health().Service(serviceName, "", true, nil)
		if err != nil {
			return nil, err
		}
		hosts := make([]string, 0, len(services))
		for _, s := range services {
			address := s.Service.Address
			if address == "" {
				address = s.Node.Address
			}
			hosts = append(hosts, fmt.Sprintf("http://%s:%d", address, s.Service.Port))
		}
		if len(hosts) == 0 {
			return nil, fmt.Errorf("unavailable service")
		}
		return hosts, nil
	}

	hosts, err := hostGetter()
	if err != nil {
		log.Errorf("get hosts from consul failed: %v", err)
		return nil, errcode.ErrAccessServiceDiscovery
	}
	c.rpcClient.Store(getClient(&cfg, hosts))

	ticker := time.NewTicker(time.Duration(cfg.ServiceIntervalMs) * time.Millisecond)
	go func() {
		for {
			old := hosts
			select {
			case <-ticker.C:
				hosts, err = hostGetter()
				if err != nil {
					log.Warnf("update hosts from consul failed: %v", err)
					continue
				}
				if isUpdated(old, hosts) {
					oldClient, ok := c.rpcClient.Load().(rpc.Client)
					if ok && oldClient != nil {
						oldClient.Close()
					}
					c.rpcClient.Store(getClient(&cfg, hosts))
				}
			case <-c.stop:
				ticker.Stop()
				return
			}
		}
	}()

	return c, nil
}

func isUpdated(a, b []string) bool {
	if len(a) != len(b) {
		return true
	}

	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return true
		}
	}
	return false
}

func getClient(cfg *Config, hosts []string) rpc.Client {
	lbConfig := &rpc.LbConfig{
		Hosts:              hosts,
		FailRetryIntervalS: cfg.FailRetryIntervalS,
		MaxFailsPeriodS:    cfg.MaxFailsPeriodS,
		HostTryTimes:       cfg.HostTryTimes,
		RequestTryTimes:    cfg.MaxHostRetry,
		ShouldRetry:        shouldRetry,
	}

	if cfg.RPCConfig == nil {
		rpcConfig := cfg.ConnMode.getConfig(cfg.BodyBandwidthMBPs,
			cfg.ClientTimeoutMs, cfg.BodyBaseTimeoutMs)
		lbConfig.Config = rpcConfig
		return rpc.NewLbClient(lbConfig, nil)
	}
	lbConfig.Config = *cfg.RPCConfig

	return rpc.NewLbClient(lbConfig, nil)
}

func (c *client) Put(ctx context.Context, args *PutArgs) (location Location, hashSumMap HashSumMap, err error) {
	if args.Size == 0 {
		hashSumMap := args.Hashes.ToHashSumMap()
		for alg := range hashSumMap {
			hashSumMap[alg] = alg.ToHasher().Sum(nil)
		}
		return Location{Blobs: make([]SliceInfo, 0)}, hashSumMap, nil
	}

	ctx = withReqidContext(ctx)
	if args.Size <= c.config.MaxSizePutOnce {
		return c.putObject(ctx, args)
	}
	return c.putParts(ctx, args)
}

func (c *client) putObject(ctx context.Context, args *PutArgs) (location Location, hashSumMap HashSumMap, err error) {
	rpcClient := c.rpcClient.Load().(rpc.Client)

	urlStr := fmt.Sprintf("/put?size=%d&hashes=%d", args.Size, args.Hashes)
	req, err := http.NewRequest(http.MethodPut, urlStr, args.Body)
	if err != nil {
		return
	}

	resp := &PutResp{}
	if err = rpcClient.DoWith(ctx, req, resp, rpc.WithCrcEncode()); err == nil {
		location = resp.Location
		hashSumMap = resp.HashSumMap
	}
	return
}

type blobPart struct {
	cid   proto.ClusterID
	vid   proto.Vid
	bid   proto.BlobID
	size  int
	token string
	buf   []byte
}

func (c *client) putPartsBatch(ctx context.Context, parts []blobPart) error {
	rpcClient := c.rpcClient.Load().(rpc.Client)

	tasks := make([]func() error, 0, len(parts))
	for _, part := range parts {
		part := part
		tasks = append(tasks, func() error {
			urlStr := fmt.Sprintf("/putat?clusterid=%d&volumeid=%d&blobid=%d&size=%d&hashes=%d&token=%s",
				part.cid, part.vid, part.bid, part.size, 0, part.token)
			req, err := http.NewRequest(http.MethodPut, urlStr, bytes.NewReader(part.buf))
			if err != nil {
				return err
			}
			resp := &PutAtResp{}
			return rpcClient.DoWith(ctx, req, resp, rpc.WithCrcEncode())
		})
	}

	if err := task.Run(context.Background(), tasks...); err != nil {
		for _, part := range parts {
			part := part
			// asynchronously delete blob
			go func() {
				urlStr := fmt.Sprintf("/deleteblob?clusterid=%d&volumeid=%d&blobid=%d&size=%d&token=%s",
					part.cid, part.vid, part.bid, part.size, part.token)
				req, err := http.NewRequest(http.MethodDelete, urlStr, nil)
				if err != nil {
					return
				}
				rpcClient.DoWith(ctx, req, nil)
			}()
		}
		return err
	}
	return nil
}

func (c *client) readerPipeline(span trace.Span, reqBody io.Reader,
	closeCh <-chan struct{}, size, blobSize int) <-chan []byte {
	ch := make(chan []byte, c.config.PartConcurrence-1)
	go func() {
		for size > 0 {
			toread := blobSize
			if toread > size {
				toread = size
			}

			buf, _ := memPool.Alloc(toread)
			buf = buf[:toread]
			_, err := io.ReadFull(reqBody, buf)
			if err != nil {
				span.Error("read buffer from request", err)
				memPool.Put(buf)
				close(ch)
				return
			}

			select {
			case <-closeCh:
				memPool.Put(buf)
				close(ch)
				return
			case ch <- buf:
			}

			size -= toread
		}
		close(ch)
	}()
	return ch
}

func (c *client) putParts(ctx context.Context, args *PutArgs) (Location, HashSumMap, error) {
	span := trace.SpanFromContextSafe(ctx)
	rpcClient := c.rpcClient.Load().(rpc.Client)

	hashSumMap := args.Hashes.ToHashSumMap()
	hasherMap := make(HasherMap, len(hashSumMap))
	for alg := range hashSumMap {
		hasherMap[alg] = alg.ToHasher()
	}

	reqBody := args.Body
	if len(hasherMap) > 0 {
		reqBody = io.TeeReader(args.Body, hasherMap.ToWriter())
	}

	var (
		loc    Location
		tokens []string
	)

	signArgs := SignArgs{}
	success := false
	defer func() {
		if success {
			return
		}

		locations := signArgs.Locations[:]
		if len(locations) > 1 {
			signArgs.Location = loc.Copy()
			signResp := &SignResp{}
			if err := rpcClient.PostWith(ctx, "/sign", signResp, signArgs); err == nil {
				locations = []Location{signResp.Location.Copy()}
			}
		}
		if len(locations) > 0 {
			if _, err := c.Delete(ctx, &DeleteArgs{Locations: locations}); err != nil {
				span.Warnf("clean location '%+v' failed %s", locations, err.Error())
			}
		}
	}()

	// alloc
	allocResp := &AllocResp{}
	if err := rpcClient.PostWith(ctx, "/alloc", allocResp, AllocArgs{Size: uint64(args.Size)}); err != nil {
		return allocResp.Location, nil, err
	}
	loc = allocResp.Location
	tokens = allocResp.Tokens
	signArgs.Locations = append(signArgs.Locations, loc.Copy())

	// buffer pipeline
	closeCh := make(chan struct{})
	bufferPipe := c.readerPipeline(span, reqBody, closeCh, int(loc.Size), int(loc.BlobSize))
	defer func() {
		close(closeCh)
		// waiting pipeline close if has error
		for buf := range bufferPipe {
			if len(buf) > 0 {
				memPool.Put(buf)
			}
		}
	}()

	releaseBuffer := func(parts []blobPart) {
		for _, part := range parts {
			memPool.Put(part.buf)
		}
	}

	currBlobIdx := 0
	currBlobCount := uint32(0)
	remainSize := loc.Size
	restPartsLoc := loc

	readSize := 0
	for readSize < int(loc.Size) {
		parts := make([]blobPart, 0, c.config.PartConcurrence)

		// waiting at least one blob
		buf, ok := <-bufferPipe
		if !ok && readSize < int(loc.Size) {
			return Location{}, nil, errcode.ErrAccessReadRequestBody
		}
		readSize += len(buf)
		parts = append(parts, blobPart{size: len(buf), buf: buf})

		more := true
		for more && len(parts) < c.config.PartConcurrence {
			select {
			case buf, ok := <-bufferPipe:
				if !ok {
					if readSize < int(loc.Size) {
						releaseBuffer(parts)
						return Location{}, nil, errcode.ErrAccessReadRequestBody
					}
					more = false
				} else {
					readSize += len(buf)
					parts = append(parts, blobPart{size: len(buf), buf: buf})
				}
			default:
				more = false
			}
		}

		tryTimes := c.config.MaxPartRetry
		for {
			if len(loc.Blobs) > MaxLocationBlobs {
				releaseBuffer(parts)
				return Location{}, nil, errcode.ErrUnexpected
			}

			// feed new params
			currIdx := currBlobIdx
			currCount := currBlobCount
			for i := range parts {
				token := tokens[currIdx]
				if restPartsLoc.Size > uint64(loc.BlobSize) && parts[i].size < int(loc.BlobSize) {
					token = tokens[currIdx+1]
				}
				parts[i].token = token
				parts[i].cid = loc.ClusterID
				parts[i].vid = loc.Blobs[currIdx].Vid
				parts[i].bid = loc.Blobs[currIdx].MinBid + proto.BlobID(currCount)

				currCount++
				if loc.Blobs[currIdx].Count == currCount {
					currIdx++
					currCount = 0
				}
			}

			err := c.putPartsBatch(ctx, parts)
			if err == nil {
				for _, part := range parts {
					remainSize -= uint64(part.size)
					currBlobCount++
					// next blobs
					if loc.Blobs[currBlobIdx].Count == currBlobCount {
						currBlobIdx++
						currBlobCount = 0
					}
				}

				break
			}
			span.Warn("putat parts", err)

			if tryTimes > 0 { // has retry setting
				if tryTimes == 1 {
					releaseBuffer(parts)
					span.Error("exceed the max retry limit", c.config.MaxPartRetry)
					return Location{}, nil, errcode.ErrUnexpected
				}
				tryTimes--
			}

			var restPartsResp *AllocResp
			// alloc the rest parts
			err = retry.Timed(3, 10).RuptOn(func() (bool, error) {
				resp := &AllocResp{}
				if err := rpcClient.PostWith(ctx, "/alloc", resp, AllocArgs{
					Size:            remainSize,
					BlobSize:        loc.BlobSize,
					CodeMode:        loc.CodeMode,
					AssignClusterID: loc.ClusterID,
				}); err != nil {
					return true, err
				}
				if len(resp.Location.Blobs) > 0 {
					if newVid := resp.Location.Blobs[0].Vid; newVid == loc.Blobs[currBlobIdx].Vid {
						return false, fmt.Errorf("alloc the same vid %d", newVid)
					}
				}
				restPartsResp = resp
				return true, nil
			})
			if err != nil {
				releaseBuffer(parts)
				span.Error("alloc another parts to put", err)
				return Location{}, nil, errcode.ErrUnexpected
			}

			restPartsLoc = restPartsResp.Location
			signArgs.Locations = append(signArgs.Locations, restPartsLoc.Copy())

			if currBlobCount > 0 {
				loc.Blobs[currBlobIdx].Count = currBlobCount
				currBlobIdx++
			}
			loc.Blobs = append(loc.Blobs[:currBlobIdx], restPartsLoc.Blobs...)
			tokens = append(tokens[:currBlobIdx], restPartsResp.Tokens...)

			currBlobCount = 0
		}

		releaseBuffer(parts)
	}

	if len(signArgs.Locations) > 1 {
		signArgs.Location = loc.Copy()
		// sign
		signResp := &SignResp{}
		if err := rpcClient.PostWith(ctx, "/sign", signResp, signArgs); err != nil {
			span.Error("sign location with crc", err)
			return Location{}, nil, errcode.ErrUnexpected
		}
		loc = signResp.Location
	}

	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}
	success = true
	return loc, hashSumMap, nil
}

func (c *client) Get(ctx context.Context, args *GetArgs) (body io.ReadCloser, err error) {
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}
	rpcClient := c.rpcClient.Load().(rpc.Client)

	ctx = withReqidContext(ctx)
	if args.Location.Size == 0 || args.ReadSize == 0 {
		return noopBody{}, nil
	}

	resp, err := rpcClient.Post(ctx, "/get", args)
	if err != nil {
		return nil, err
	}

	return resp.Body, nil
}

func (c *client) Delete(ctx context.Context, args *DeleteArgs) ([]Location, error) {
	if !args.IsValid() {
		if args == nil {
			return nil, errcode.ErrIllegalArguments
		}
		return args.Locations, errcode.ErrIllegalArguments
	}
	rpcClient := c.rpcClient.Load().(rpc.Client)

	ctx = withReqidContext(ctx)
	locations := make([]Location, 0, len(args.Locations))
	for _, loc := range args.Locations {
		if loc.Size > 0 {
			locations = append(locations, loc.Copy())
		}
	}
	if len(locations) == 0 {
		return nil, nil
	}

	if err := retry.Timed(3, 10).On(func() error {
		// access response 2xx even if there has failed locations
		deleteResp := &DeleteResp{}
		if err := rpcClient.PostWith(ctx, "/delete", deleteResp,
			DeleteArgs{Locations: locations}); err != nil && rpc.DetectStatusCode(err) != http.StatusIMUsed {
			return err
		}
		if len(deleteResp.FailedLocations) > 0 {
			locations = deleteResp.FailedLocations[:]
			return errcode.ErrUnexpected
		}
		return nil
	}); err != nil {
		return locations, err
	}
	return nil, nil
}

func shouldRetry(code int, err error) bool {
	if err != nil {
		if httpErr, ok := err.(rpc.HTTPError); ok {
			// 500 need to retry next host
			return httpErr.StatusCode() == http.StatusInternalServerError
		}
		return true
	}
	if code/100 != 4 && code/100 != 2 {
		return true
	}
	return false
}
