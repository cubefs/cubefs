package sdk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cubefs/cubefs/blobstore/access/stream"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/common/uptoken"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

const (
	defaultClientTimeoutMs int64 = 5000    // 5s
	defaultMaxSizePutOnce  int64 = 1 << 28 // 256MB
	defaultMaxPartRetry    int   = 3
	defaultPartConcurrence int   = 4
)

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

type Config struct {
	stream.StreamConfig
	Limit           stream.LimitConfig `json:"limit"`
	ClientTimeoutMs int64              `json:"client_timeout_ms"`
	MaxSizePutOnce  int64              `json:"max_size_put_once"`
	MaxPartRetry    int                `json:"max_part_retry"`
	PartConcurrence int                `json:"part_concurrence"`
	LogLevel        log.Level          `json:"log_level"`
	Logger          *lumberjack.Logger `json:"logger"`
}

type sdkHandler struct {
	conf    Config
	handler stream.StreamHandler
	limiter stream.Limiter
	closer  closer.Closer
}

func New(conf *Config) (acapi.API, error) {
	fixConfig(conf)

	cl := closer.New()
	h, err := stream.NewStreamHandler(&conf.StreamConfig, cl.Done())
	if err != nil {
		return nil, err
	}

	return &sdkHandler{
		conf:    *conf,
		handler: h,
		limiter: stream.NewLimiter(conf.Limit),
		closer:  cl,
	}, nil
}

func (s *sdkHandler) Get(ctx context.Context, args *acapi.GetArgs) (body io.ReadCloser, err error) {
	if !args.IsValid() || args.Body == nil {
		return nil, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	if args.Location.Size == 0 || args.ReadSize == 0 {
		return noopBody{}, nil
	}

	return s.doGet(ctx, args)
}

func (s *sdkHandler) Delete(ctx context.Context, args *acapi.DeleteArgs) (failedLocations []acapi.Location, err error) {
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	locations := make([]acapi.Location, 0, len(args.Locations)) // check location size
	for _, loc := range args.Locations {
		if loc.Size > 0 {
			locations = append(locations, loc)
		}
	}
	if len(locations) == 0 {
		return nil, nil
	}

	if err = retry.Timed(3, 10).On(func() error {
		// access response 2xx even if there has failed locations
		deleteResp, err1 := s.doDelete(ctx, &acapi.DeleteArgs{Locations: locations})
		if err1 != nil && rpc.DetectStatusCode(err1) != http.StatusIMUsed {
			return err1
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

func (s *sdkHandler) Put(ctx context.Context, args *acapi.PutArgs) (acapi.Location, acapi.HashSumMap, error) {
	if args == nil {
		return acapi.Location{}, nil, errcode.ErrIllegalArguments
	}

	if args.Size == 0 {
		hashSumMap := args.Hashes.ToHashSumMap()
		for alg := range hashSumMap {
			hashSumMap[alg] = alg.ToHasher().Sum(nil)
		}
		return acapi.Location{Blobs: make([]acapi.SliceInfo, 0)}, hashSumMap, nil
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	if args.Size <= s.conf.MaxSizePutOnce {
		return s.doPutObject(ctx, args)
	}
	return s.putParts(ctx, args)
}

func (s *sdkHandler) Alloc(ctx context.Context, args *acapi.AllocArgs) (acapi.AllocResp, error) {
	if !args.IsValid() {
		return acapi.AllocResp{}, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	return s.doAlloc(ctx, args)
}

// sign generate crc with locations
func (s *sdkHandler) sign(ctx context.Context, args *acapi.SignArgs) (acapi.SignResp, error) {
	if !args.IsValid() {
		return acapi.SignResp{}, errcode.ErrIllegalArguments
	}
	ctx = acapi.ClientWithReqidContext(ctx)
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept /sign request args: %+v", args)

	loc := args.Location
	crcOld := loc.Crc
	if err := stream.LocationCrcSign(&loc, args.Locations); err != nil {
		span.Error("stream sign failed", errors.Detail(err))
		return acapi.SignResp{}, errcode.ErrIllegalArguments
	}

	span.Infof("done /sign request crc %d -> %d, resp:%+v", crcOld, loc.Crc, loc)
	return acapi.SignResp{Location: loc}, nil
}

// deleteBlob delete one blob
func (s *sdkHandler) deleteBlob(ctx context.Context, args *acapi.DeleteBlobArgs) error {
	if !args.IsValid() {
		return errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept /deleteblob request args:%+v", args)

	valid := false
	for _, secretKey := range stream.StreamTokenSecretKeys {
		token := uptoken.DecodeToken(args.Token)
		if token.IsValid(args.ClusterID, args.Vid, args.BlobID, uint32(args.Size), secretKey[:]) {
			valid = true
			break
		}
	}
	if !valid {
		span.Warnf("invalid token:%s", args.Token)
		return errcode.ErrIllegalArguments
	}

	if err := s.handler.Delete(ctx, &acapi.Location{
		ClusterID: args.ClusterID,
		BlobSize:  1,
		Blobs: []acapi.SliceInfo{{
			MinBid: args.BlobID,
			Vid:    args.Vid,
			Count:  1,
		}},
	}); err != nil {
		span.Error("stream delete blob failed", errors.Detail(err))
		return err
	}

	span.Info("done /deleteblob request")
	return nil
}

func (s *sdkHandler) doGet(ctx context.Context, args *acapi.GetArgs) (resp io.ReadCloser, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept sdk request args:%+v", args)
	if !stream.LocationCrcVerify(&args.Location) {
		err = errcode.ErrIllegalArguments
		span.Error("stream get args is invalid ", errors.Detail(err))
		return
	}

	writer := s.limiter.Writer(ctx, args.Body)
	transfer, err := s.handler.Get(ctx, writer, args.Location, args.ReadSize, args.Offset)
	if err != nil {
		span.Error("stream get prepare failed", errors.Detail(err))
		return
	}

	err = transfer()
	if err != nil {
		span.Error("stream get transfer failed", errors.Detail(err))
		return
	}
	span.Info("done sdk get request")

	return io.NopCloser(args.Body), nil
}

func (s *sdkHandler) doDelete(ctx context.Context, args *acapi.DeleteArgs) (resp acapi.DeleteResp, err error) {
	span := trace.SpanFromContextSafe(ctx)

	defer func() {
		if err != nil {
			err = httpError(err)
			return
		}

		if len(resp.FailedLocations) > 0 {
			span.Errorf("failed locations N %d of %d", len(resp.FailedLocations), len(args.Locations))
			// must return 2xx even if has failed locations,
			// cos rpc read body only on 2xx.
			// TODO: return other http status code
			err = rpc.NewError(http.StatusIMUsed, "", errors.New("error StatusIMUsed"))
			return
		}
	}()
	span.Debugf("accept sdk delete request args: locations %d", len(args.Locations))
	defer span.Info("done sdk delete request")

	clusterBlobsN := make(map[proto.ClusterID]int, 4)
	for _, loc := range args.Locations {
		if !stream.LocationCrcVerify(&loc) {
			span.Infof("invalid crc %+v", loc)
			err = errcode.ErrIllegalArguments
			return
		}
		clusterBlobsN[loc.ClusterID] += len(loc.Blobs)
	}

	if len(args.Locations) == 1 {
		loc := args.Locations[0]
		if err := s.handler.Delete(ctx, &loc); err != nil {
			span.Error("stream delete failed", errors.Detail(err))
			resp.FailedLocations = []acapi.Location{loc}
		}
		return
	}

	// merge the same cluster locations to one delete message,
	// anyone of this cluster failed, all locations mark failure,
	//
	// a min delete message about 10-20 bytes,
	// max delete locations is 1024, one location is max to 5G,
	// merged message max size about 40MB.
	merged := make(map[proto.ClusterID][]acapi.SliceInfo, len(clusterBlobsN))
	for id, n := range clusterBlobsN {
		merged[id] = make([]acapi.SliceInfo, 0, n)
	}
	for _, loc := range args.Locations {
		merged[loc.ClusterID] = append(merged[loc.ClusterID], loc.Blobs...)
	}

	for cid := range merged {
		if err := s.handler.Delete(ctx, &acapi.Location{
			ClusterID: cid,
			BlobSize:  1,
			Blobs:     merged[cid],
		}); err != nil {
			span.Error("stream delete failed", cid, errors.Detail(err))
			for i := range args.Locations {
				if args.Locations[i].ClusterID == cid {
					resp.FailedLocations = append(resp.FailedLocations, args.Locations[i])
				}
			}
		}
	}

	return
}

func (s *sdkHandler) doPutObject(ctx context.Context, args *acapi.PutArgs) (acapi.Location, acapi.HashSumMap, error) {
	span := trace.SpanFromContextSafe(ctx)
	var err error

	span.Debugf("accept sdk put request args:%+v", args)
	if !args.IsValid() {
		err = errcode.ErrIllegalArguments
		span.Error("stream get args is invalid ", errors.Detail(err))
		return acapi.Location{}, nil, err
	}

	hashSumMap := args.Hashes.ToHashSumMap()
	hasherMap := make(acapi.HasherMap, len(hashSumMap))
	// make hashser
	for alg := range hashSumMap {
		hasherMap[alg] = alg.ToHasher()
	}

	rc := s.limiter.Reader(ctx, args.Body)
	loc, err := s.handler.Put(ctx, rc, args.Size, hasherMap)
	if err != nil {
		span.Error("stream put failed", errors.Detail(err))
		err = httpError(err)
		return acapi.Location{}, nil, err
	}

	// hasher sum
	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}

	if err = stream.LocationCrcFill(loc); err != nil {
		span.Error("stream put fill location crc", err)
		err = httpError(err)
		return acapi.Location{}, nil, err
	}

	span.Infof("done /put request location:%+v hash:%+v", loc, hashSumMap.All())
	return *loc, hashSumMap, nil
}

func (s *sdkHandler) doPutAt(ctx context.Context, args *acapi.PutAtArgs) (hashSumMap acapi.HashSumMap, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk putat request args:%+v", args)

	valid := false
	for _, secretKey := range stream.StreamTokenSecretKeys {
		token := uptoken.DecodeToken(args.Token)
		if token.IsValid(args.ClusterID, args.Vid, args.BlobID, uint32(args.Size), secretKey[:]) {
			valid = true
			break
		}
	}
	if !valid {
		span.Warnf("invalid token:%s", args.Token)
		return nil, errcode.ErrIllegalArguments
	}

	hashSumMap = args.Hashes.ToHashSumMap()
	hasherMap := make(acapi.HasherMap, len(hashSumMap))
	for alg := range hashSumMap {
		hasherMap[alg] = alg.ToHasher()
	}

	rc := s.limiter.Reader(ctx, args.Body)
	err = s.handler.PutAt(ctx, rc, args.ClusterID, args.Vid, args.BlobID, args.Size, hasherMap)
	if err != nil {
		span.Error("stream putat failed ", errors.Detail(err))
		return nil, err
	}

	// hasher sum
	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}
	span.Infof("done /putat request hash:%+v", hashSumMap.All())

	return hashSumMap, nil
}

func (s *sdkHandler) doAlloc(ctx context.Context, args *acapi.AllocArgs) (resp acapi.AllocResp, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk alloc request args:%+v", args)

	location, err := s.handler.Alloc(ctx, args.Size, args.BlobSize, args.AssignClusterID, args.CodeMode)
	if err != nil {
		span.Error("stream alloc failed ", errors.Detail(err))
		return resp, err
	}

	if err = stream.LocationCrcFill(location); err != nil {
		span.Error("stream alloc fill location crc", err)
		return resp, err
	}

	resp = acapi.AllocResp{
		Location: *location,
		Tokens:   stream.StreamGenTokens(location),
	}
	span.Infof("done /alloc request resp:%+v", resp)

	return resp, nil
}

type blobPart struct {
	cid   proto.ClusterID
	vid   proto.Vid
	bid   proto.BlobID
	size  int
	token string
	buf   []byte
}

func (s *sdkHandler) putPartsBatch(ctx context.Context, parts []blobPart) error {
	tasks := make([]func() error, 0, len(parts))
	for _, pt := range parts {
		part := pt
		tasks = append(tasks, func() error {
			_, err := s.doPutAt(ctx, &acapi.PutAtArgs{
				ClusterID: part.cid,
				Vid:       part.vid,
				BlobID:    part.bid,
				Size:      int64(part.size),
				Hashes:    0,
				Token:     part.token,
				Body:      bytes.NewReader(part.buf),
			})
			return err
		})
	}

	if err := task.Run(context.Background(), tasks...); err != nil {
		for _, pt := range parts {
			part := pt
			// asynchronously delete blob
			go func() {
				s.deleteBlob(ctx, &acapi.DeleteBlobArgs{
					ClusterID: part.cid,
					Vid:       part.vid,
					BlobID:    part.bid,
					Size:      int64(part.size),
					Token:     part.token,
				})
			}()
		}
		return err
	}
	return nil
}

func (s *sdkHandler) readerPipeline(span trace.Span, reqBody io.Reader,
	closeCh <-chan struct{}, size, blobSize int) <-chan []byte {
	ch := make(chan []byte, s.conf.PartConcurrence-1)
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

func (s *sdkHandler) putParts(ctx context.Context, args *acapi.PutArgs) (acapi.Location, acapi.HashSumMap, error) {
	span := trace.SpanFromContextSafe(ctx)

	hashSumMap := args.Hashes.ToHashSumMap()
	hasherMap := make(acapi.HasherMap, len(hashSumMap))
	for alg := range hashSumMap {
		hasherMap[alg] = alg.ToHasher()
	}

	reqBody := args.Body
	if len(hasherMap) > 0 {
		reqBody = io.TeeReader(args.Body, hasherMap.ToWriter())
	}

	var (
		loc    acapi.Location
		tokens []string
	)

	signArgs := acapi.SignArgs{}
	success := false
	defer func() {
		if success {
			return
		}

		locations := signArgs.Locations[:]
		if len(locations) > 1 {
			signArgs.Location = loc.Copy()
			signResp, err := s.sign(ctx, &signArgs)
			if err == nil {
				locations = []acapi.Location{signResp.Location.Copy()}
			}
		}
		if len(locations) > 0 {
			if _, err := s.Delete(ctx, &acapi.DeleteArgs{Locations: locations}); err != nil {
				span.Warnf("clean location '%+v' failed %s", locations, err.Error())
			}
		}
	}()

	// alloc
	allocResp, err := s.Alloc(ctx, &acapi.AllocArgs{Size: uint64(args.Size)})
	if err != nil {
		return acapi.Location{}, nil, err
	}
	loc = allocResp.Location
	tokens = allocResp.Tokens
	signArgs.Locations = append(signArgs.Locations, loc.Copy())

	// buffer pipeline
	closeCh := make(chan struct{})
	bufferPipe := s.readerPipeline(span, reqBody, closeCh, int(loc.Size), int(loc.BlobSize))
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
		parts := make([]blobPart, 0, s.conf.PartConcurrence)

		// waiting at least one blob
		buf, ok := <-bufferPipe
		if !ok && readSize < int(loc.Size) {
			return acapi.Location{}, nil, errcode.ErrAccessReadRequestBody
		}
		readSize += len(buf)
		parts = append(parts, blobPart{size: len(buf), buf: buf})

		more := true
		for more && len(parts) < s.conf.PartConcurrence {
			select {
			case buf, ok := <-bufferPipe:
				if !ok {
					if readSize < int(loc.Size) {
						releaseBuffer(parts)
						return acapi.Location{}, nil, errcode.ErrAccessReadRequestBody
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

		tryTimes := s.conf.MaxPartRetry
		for {
			if len(loc.Blobs) > acapi.MaxLocationBlobs {
				releaseBuffer(parts)
				return acapi.Location{}, nil, errcode.ErrUnexpected
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

			err := s.putPartsBatch(ctx, parts)
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
					span.Error("exceed the max retry limit", s.conf.MaxPartRetry)
					return acapi.Location{}, nil, errcode.ErrUnexpected
				}
				tryTimes--
			}

			var restPartsResp *acapi.AllocResp
			// alloc the rest parts
			err = retry.Timed(3, 10).RuptOn(func() (bool, error) {
				resp, err1 := s.Alloc(ctx, &acapi.AllocArgs{
					Size:            remainSize,
					BlobSize:        loc.BlobSize,
					AssignClusterID: loc.ClusterID,
					CodeMode:        loc.CodeMode,
				})
				if err1 != nil {
					return true, err1
				}
				if len(resp.Location.Blobs) > 0 {
					if newVid := resp.Location.Blobs[0].Vid; newVid == loc.Blobs[currBlobIdx].Vid {
						return false, fmt.Errorf("alloc the same vid %d", newVid)
					}
				}
				restPartsResp = &resp
				return true, nil
			})
			if err != nil {
				releaseBuffer(parts)
				span.Error("alloc another parts to put", err)
				return acapi.Location{}, nil, errcode.ErrUnexpected
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
		signResp, err1 := s.sign(ctx, &signArgs)
		if err1 != nil {
			span.Error("sign location with crc", err1)
			return acapi.Location{}, nil, errcode.ErrUnexpected
		}
		loc = signResp.Location
	}

	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}
	success = true
	return loc, hashSumMap, nil
}

func httpError(err error) error {
	if e, ok := err.(rpc.HTTPError); ok {
		return e
	}
	if e, ok := err.(*errors.Error); ok {
		return rpc.NewError(http.StatusInternalServerError, "ServerError", e.Cause())
	}
	return errcode.ErrUnexpected
}

func fixConfig(cfg *Config) {
	defaulter.Less(&cfg.ClientTimeoutMs, defaultClientTimeoutMs)
	defaulter.LessOrEqual(&cfg.MaxSizePutOnce, defaultMaxSizePutOnce)
	defaulter.Less(&cfg.MaxPartRetry, defaultMaxPartRetry)
	defaulter.LessOrEqual(&cfg.PartConcurrence, defaultPartConcurrence)
	log.SetOutputLevel(cfg.LogLevel)
	if cfg.Logger != nil {
		log.SetOutput(cfg.Logger)
	}
}
