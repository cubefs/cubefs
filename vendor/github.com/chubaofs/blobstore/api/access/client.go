package access

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/hashicorp/consul/api"
	"gopkg.in/natefinch/lumberjack.v2"

	errcode "github.com/chubaofs/blobstore/common/errors"
	"github.com/chubaofs/blobstore/common/proto"
	"github.com/chubaofs/blobstore/common/resourcepool"
	"github.com/chubaofs/blobstore/common/rpc"
	"github.com/chubaofs/blobstore/common/trace"
	"github.com/chubaofs/blobstore/util/log"
	"github.com/chubaofs/blobstore/util/selector"
	"github.com/chubaofs/blobstore/util/task"
)

const (
	defaultMaxSizePutOnce    = 1 << 28 // 256MB
	defaultMaxPartRetry      = 3
	defaultMaxHostRetry      = 3
	defaultPartConcurrence   = 4
	defaultServiceIntervalMs = 5000
	defaultServiceName       = "access"

	_cacheBufferPutOnce = 1 << 23 // 8M
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
	Consul api.Config
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

	// RPCConfig user-defined rpc config
	// All connections will use the config if it's not nil
	// ConnMode will be ignored if rpc config is setting
	RPCConfig *rpc.Config

	// Logger trace all logging to the logger if setting.
	// It is an io.WriteCloser that writes to the specified filename.
	// YOU should CLOSE it after you do not use the client anymore.
	Logger *Logger
}

// Logger alias of lumberjack Logger
// See more at: https://github.com/natefinch/lumberjack
type Logger = lumberjack.Logger

// client access rpc client
type client struct {
	config       Config
	consulClient *api.Client
	selector     selector.Selector
	rpcClient    rpc.Client
}

// API access api for s3
// To trace request id, the ctx is better WithRequestID(ctx, rid).
type API interface {
	// Put object once if size is not greater than MaxSizePutOnce, otherwise put blobs one by one.
	// return a location and map of hash summary bytes you excepted.
	Put(ctx context.Context, args *PutArgs) (location Location, hashSumMap HashSumMap, err error)
	// Get object, range is supported.
	Get(ctx context.Context, args *GetArgs) (body io.ReadCloser, err error)
	// Delete all blobs in these locations.
	// return failed locations which have yet been deleted if error is not nil.
	Delete(ctx context.Context, args *DeleteArgs) (failedLocations []Location, err error)
}

var _ API = (*client)(nil)

type hadReader struct {
	isRead bool
	reader io.Reader
}

// Read the io.Reader had been read
func (r *hadReader) Read(p []byte) (n int, err error) {
	r.isRead = true
	return r.reader.Read(p)
}

type noopBody struct{}

var _ io.ReadCloser = (*noopBody)(nil)

func (rc noopBody) Read(p []byte) (n int, err error) { return 0, io.EOF }
func (rc noopBody) Close() error                     { return nil }

var (
	memPool *resourcepool.MemPool
)

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
	if cfg.MaxSizePutOnce <= 0 {
		cfg.MaxSizePutOnce = defaultMaxSizePutOnce
	}
	if cfg.MaxPartRetry < 0 {
		cfg.MaxPartRetry = defaultMaxPartRetry
	}
	if cfg.MaxHostRetry <= 0 {
		cfg.MaxHostRetry = defaultMaxHostRetry
	}
	if cfg.PartConcurrence <= 0 {
		cfg.PartConcurrence = defaultPartConcurrence
	}
	if cfg.ServiceIntervalMs < 500 {
		cfg.ServiceIntervalMs = defaultServiceIntervalMs
	}

	var rpcClient rpc.Client
	if cfg.RPCConfig != nil {
		rpcClient = rpc.NewClient(cfg.RPCConfig)
	} else {
		rpcConfig := cfg.ConnMode.getConfig(cfg.BodyBandwidthMBPs,
			cfg.ClientTimeoutMs, cfg.BodyBaseTimeoutMs)
		rpcClient = rpc.NewClient(&rpcConfig)
	}

	if cfg.Logger != nil {
		log.SetOutput(cfg.Logger)
	}

	consulClient, err := api.NewClient(&cfg.Consul)
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

	hostSelector, err := selector.NewSelector(cfg.ServiceIntervalMs, hostGetter)
	if err != nil {
		return nil, errcode.ErrAccessServiceDiscovery
	}

	return &client{
		config:       cfg,
		consulClient: consulClient,
		selector:     hostSelector,
		rpcClient:    rpcClient,
	}, nil
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
	span := trace.SpanFromContextSafe(ctx)

	var (
		cached bool
		buffer []byte
		reader *hadReader
	)
	if args.Size > 0 && args.Size <= _cacheBufferPutOnce {
		cached = true
		buffer, _ = memPool.Alloc(int(args.Size))
		buffer = buffer[:args.Size]
		defer memPool.Put(buffer)

		_, err := io.ReadFull(args.Body, buffer)
		if err != nil {
			span.Error("read buffer from request", err)
			return Location{}, nil, errcode.ErrAccessReadRequestBody
		}
	} else {
		reader = &hadReader{
			isRead: false,
			reader: args.Body,
		}
	}

	err = c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
		var body io.Reader
		if cached {
			body = bytes.NewReader(buffer)
		} else {
			if reader.isRead {
				span.Info("retry on other access host which had been read body")
				return errcode.ErrAccessReadConflictBody
			}
			body = reader
		}

		urlStr := fmt.Sprintf("%s/put?size=%d&hashes=%d", host, args.Size, args.Hashes)
		req, e := http.NewRequest(http.MethodPut, urlStr, body)
		if e != nil {
			return e
		}

		resp := &PutResp{}
		e = c.rpcClient.DoWith(ctx, req, resp, rpc.WithCrcEncode())
		if e == nil {
			location = resp.Location
			hashSumMap = resp.HashSumMap
		}
		return e
	})
	return
}

type blobPart struct {
	cid   proto.ClusterID
	vid   proto.Vid
	bid   proto.BlobID
	size  int
	index int
	token string
	buf   []byte
}

func (c *client) putPartsBatch(ctx context.Context, parts []blobPart) error {
	tasks := make([]func() error, 0, len(parts))
	for _, part := range parts {
		part := part
		tasks = append(tasks, func() error {
			return c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
				urlStr := fmt.Sprintf("%s/putat?clusterid=%d&volumeid=%d&blobid=%d&size=%d&hashes=%d&token=%s",
					host, part.cid, part.vid, part.bid, part.size, 0, part.token)
				req, err := http.NewRequest(http.MethodPut, urlStr, bytes.NewReader(part.buf))
				if err != nil {
					return err
				}
				resp := &PutAtResp{}
				return c.rpcClient.DoWith(ctx, req, resp, rpc.WithCrcEncode())
			})
		})
	}

	if err := task.Run(context.Background(), tasks...); err != nil {
		for _, part := range parts {
			part := part
			// asynchronously delete blob
			go func() {
				c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
					urlStr := fmt.Sprintf("%s/deleteblob?clusterid=%d&volumeid=%d&blobid=%d&size=%d&token=%s",
						host, part.cid, part.vid, part.bid, part.size, part.token)
					req, err := http.NewRequest(http.MethodDelete, urlStr, nil)
					if err != nil {
						return err
					}
					return c.rpcClient.DoWith(ctx, req, nil)
				})
			}()
		}
		return err
	}
	return nil
}

func (c *client) putParts(ctx context.Context, args *PutArgs) (Location, HashSumMap, error) {
	span := trace.SpanFromContextSafe(ctx)

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
			if err := c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
				return c.rpcClient.PostWith(ctx, fmt.Sprintf("%s/sign", host), signResp, signArgs)
			}); err == nil {
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
	err := c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
		allocResp := &AllocResp{}
		if err := c.rpcClient.PostWith(ctx, fmt.Sprintf("%s/alloc", host), allocResp, AllocArgs{
			Size: uint64(args.Size),
		}); err != nil {
			return err
		}
		loc = allocResp.Location
		tokens = allocResp.Tokens
		return nil
	})
	if err != nil {
		return loc, nil, err
	}
	signArgs.Locations = append(signArgs.Locations, loc.Copy())

	// buffer pipeline
	closeCh := make(chan struct{})
	bufferPipe := func(size, blobSize int) <-chan []byte {
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
					close(ch)
					return
				}

				select {
				case <-closeCh:
					close(ch)
					return
				case ch <- buf:
				}

				size -= toread
			}
			close(ch)
		}()
		return ch
	}(int(loc.Size), int(loc.BlobSize))

	defer func() {
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

	index := -1
	readSize := 0
	for readSize < int(loc.Size) {
		parts := make([]blobPart, 0, c.config.PartConcurrence)

		// waiting at least one blob
		buf, ok := <-bufferPipe
		if !ok && readSize < int(loc.Size) {
			return Location{}, nil, errcode.ErrAccessReadRequestBody
		}
		index++
		readSize += len(buf)
		parts = append(parts, blobPart{size: len(buf), index: index, buf: buf})

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
					index++
					readSize += len(buf)
					parts = append(parts, blobPart{size: len(buf), index: index, buf: buf})
				}
			default:
				more = false
			}
		}

		tryTimes := c.config.MaxPartRetry
		for {
			if uint32(len(loc.Blobs)) > MaxLocationBlobs {
				releaseBuffer(parts)
				close(closeCh)
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
					close(closeCh)
					span.Error("exceed the max retry limit", c.config.MaxPartRetry)
					return Location{}, nil, errcode.ErrUnexpected
				}
				tryTimes--
			}

			var restPartsResp *AllocResp
			// alloc the rest parts
			err = c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
				resp := &AllocResp{}
				if err := c.rpcClient.PostWith(ctx, fmt.Sprintf("%s/alloc", host), resp, AllocArgs{
					Size:            remainSize,
					BlobSize:        loc.BlobSize,
					CodeMode:        loc.CodeMode,
					AssignClusterID: loc.ClusterID,
				}); err != nil {
					return err
				}
				if len(resp.Location.Blobs) > 0 {
					if newVid := resp.Location.Blobs[0].Vid; newVid == loc.Blobs[currBlobIdx].Vid {
						return fmt.Errorf("alloc the same vid %d", newVid)
					}
				}
				restPartsResp = resp
				return nil
			})
			if err != nil {
				releaseBuffer(parts)
				close(closeCh)
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
		err = c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
			signResp := &SignResp{}
			if err := c.rpcClient.PostWith(ctx, fmt.Sprintf("%s/sign", host), signResp, signArgs); err != nil {
				return err
			}
			loc = signResp.Location
			return nil
		})
		if err != nil {
			span.Error("sign location with crc", err)
			return Location{}, nil, errcode.ErrUnexpected
		}
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

	ctx = withReqidContext(ctx)
	if args.Location.Size == 0 || args.ReadSize == 0 {
		return noopBody{}, nil
	}

	err = c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
		resp, e := c.rpcClient.Post(ctx, fmt.Sprintf("%s/get", host), args)
		if e != nil {
			return e
		}
		if resp.StatusCode >= 400 {
			return rpc.NewError(resp.StatusCode, "StatusCode", fmt.Errorf("code: %d", resp.StatusCode))
		}
		body = resp.Body
		return nil
	})
	return
}

func (c *client) Delete(ctx context.Context, args *DeleteArgs) ([]Location, error) {
	if !args.IsValid() {
		if args == nil {
			return nil, errcode.ErrIllegalArguments
		}
		return args.Locations, errcode.ErrIllegalArguments
	}

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

	err := c.tryN(ctx, c.config.MaxHostRetry, func(host string) error {
		// access response 2xx even if there has failed locations
		deleteResp := &DeleteResp{}
		if err := c.rpcClient.PostWith(ctx, fmt.Sprintf("%s/delete", host), deleteResp,
			DeleteArgs{Locations: locations}); err != nil && rpc.DetectStatusCode(err) != http.StatusIMUsed {
			return err
		}
		if len(deleteResp.FailedLocations) > 0 {
			locations = deleteResp.FailedLocations[:]
			return errcode.ErrUnexpected
		}
		return nil
	})
	if err == nil {
		return nil, nil
	}
	return locations, err
}

func (c *client) tryN(ctx context.Context, n int, connector func(string) error) error {
	span := trace.SpanFromContextSafe(ctx)

	hs := c.selector.GetRandomN(n)
	hosts := make([]string, 0, len(c.config.PriorityAddrs)+len(hs))
	hosts = append(c.config.PriorityAddrs[:], hs...)
	if len(hosts) == 0 {
		return errcode.ErrAccessServiceDiscovery
	}

	for _, host := range hosts {
		err := connector(host)
		if err == nil {
			return nil
		}

		// has connected access node
		if httpErr, ok := err.(rpc.HTTPError); ok {
			// 500 need to retry next host
			if httpErr.StatusCode() == http.StatusInternalServerError {
				span.Warn("httpcode 500 need to try next, failed on", host, err)
				continue
			}
			return err
		}

		// cannot connected most probably
		span.Warn("failed on", host, err)
	}

	span.Error("try all hosts failed")
	return errcode.ErrUnexpected
}
