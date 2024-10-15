// Copyright 2024 The CubeFS Authors.
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

package sdk

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/cubefs/cubefs/blobstore/access/stream"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/shardnode"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/security"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
	"github.com/cubefs/cubefs/blobstore/util/task"
)

const (
	defaultMaxSizePutOnce  int64  = 1 << 28 // 256MB
	defaultMaxRetry        int    = 3
	defaultRetryDelayMs    uint32 = 10
	defaultPartConcurrence int    = 4
	defaultListCount       uint64 = 100

	limitNameGet    = "get"
	limitNamePut    = "put"
	limitNameDelete = "delete"
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

// ResetMemoryPool is thread unsafe, call it on init.
func ResetMemoryPool(sizeClasses map[int]int) {
	memPool = resourcepool.NewMemPool(sizeClasses)
}

type Config struct {
	stream.StreamConfig

	Limit           stream.LimitConfig `json:"limit"`
	MaxSizePutOnce  int64              `json:"max_size_put_once"`
	MaxRetry        int                `json:"max_retry"`
	RetryDelayMs    uint32             `json:"retry_delay_ms"`
	PartConcurrence int                `json:"part_concurrence"`

	LogLevel log.Level `json:"log_level"`
	Logger   io.Writer `json:"-"`
}

type sdkHandler struct {
	conf    Config
	handler stream.StreamHandler
	limiter stream.Limiter
	closer  closer.Closer
}

func New(conf *Config) (acapi.Client, error) {
	fixConfig(conf)
	// add region magic checksum to the secret keys
	security.InitWithRegionMagic(conf.StreamConfig.ClusterConfig.RegionMagic)

	cl := closer.New()
	h, err := stream.NewStreamHandler(&conf.StreamConfig, cl.Done())
	if err != nil {
		log.Errorf("new stream handler failed, err: %+v", err)
		return nil, err
	}

	return &sdkHandler{
		conf:    *conf,
		handler: h,
		limiter: stream.NewLimiter(conf.Limit),
		closer:  cl,
	}, nil
}

func (s *sdkHandler) Get(ctx context.Context, args *acapi.GetArgs) (io.ReadCloser, error) {
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	if args.Location.Size_ == 0 || args.ReadSize == 0 {
		return noopBody{}, nil
	}

	name := limitNameGet
	if err := s.limiter.Acquire(name); err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Debugf("access concurrent limited %s, err:%+v", name, err)
		return nil, errcode.ErrAccessLimited
	}
	defer s.limiter.Release(name)

	return s.doGet(ctx, args)
}

func (s *sdkHandler) Delete(ctx context.Context, args *acapi.DeleteArgs) (failedLocations []proto.Location, err error) {
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	locations := make([]proto.Location, 0, len(args.Locations)) // check location size
	for _, loc := range args.Locations {
		if loc.Size_ > 0 {
			locations = append(locations, loc)
		}
	}
	if len(locations) == 0 {
		return nil, nil
	}

	name := limitNameDelete
	if err := s.limiter.Acquire(name); err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Debugf("access concurrent limited %s, err:%+v", name, err)
		return nil, errcode.ErrAccessLimited
	}
	defer s.limiter.Release(name)

	if err = retry.Timed(s.conf.MaxRetry, s.conf.RetryDelayMs).On(func() error {
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

func (s *sdkHandler) Put(ctx context.Context, args *acapi.PutArgs) (lc proto.Location, hm acapi.HashSumMap, err error) {
	if args == nil {
		return proto.Location{}, nil, errcode.ErrIllegalArguments
	}

	if args.Size == 0 {
		hashSumMap := args.Hashes.ToHashSumMap()
		for alg := range hashSumMap {
			hashSumMap[alg] = alg.ToHasher().Sum(nil)
		}
		return proto.Location{Slices: make([]proto.Slice, 0)}, hashSumMap, nil
	}

	ctx = acapi.ClientWithReqidContext(ctx)

	name := limitNamePut
	if err := s.limiter.Acquire(name); err != nil {
		span := trace.SpanFromContextSafe(ctx)
		span.Debugf("access concurrent limited %s, err:%+v", name, err)
		return proto.Location{}, nil, errcode.ErrAccessLimited
	}
	defer s.limiter.Release(name)

	if args.Size <= s.conf.MaxSizePutOnce {
		if args.GetBody == nil {
			return s.doPutObject(ctx, args)
		}

		i := 0
		err = retry.Timed(s.conf.MaxRetry, s.conf.RetryDelayMs).On(func() error {
			if i >= 1 {
				args.Body, err = args.GetBody()
				if err != nil {
					return err
				}
			}

			i++
			lc, hm, err = s.doPutObject(ctx, args)
			return err
		})
		return lc, hm, err
	}
	return s.putParts(ctx, args)
}

func (s *sdkHandler) ListBlob(ctx context.Context, args *acapi.ListBlobArgs) (shardnode.ListBlobRet, error) {
	if !args.IsValid() {
		return shardnode.ListBlobRet{}, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk list blob request args: %v", *args)
	if args.Count == 0 {
		args.Count = defaultListCount
	}

	return s.handler.ListBlob(ctx, args)
}

func (s *sdkHandler) CreateBlob(ctx context.Context, args *acapi.CreateBlobArgs) (acapi.CreateBlobRet, error) {
	if !args.IsValid() {
		return acapi.CreateBlobRet{}, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk create blob request args: %v", *args)

	loc, err := s.handler.CreateBlob(ctx, args)
	if err != nil {
		return acapi.CreateBlobRet{}, err
	}
	return acapi.CreateBlobRet{Location: *loc}, nil
}

func (s *sdkHandler) DeleteBlob(ctx context.Context, args *acapi.DelBlobArgs) error {
	if !args.IsValid() {
		return errcode.ErrIllegalArguments
	}

	// delete meta at shardnode, then delete data at blobnode
	ctx = acapi.ClientWithReqidContext(ctx)
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk delete blob request args: %v", *args)
	return s.handler.DeleteBlob(ctx, args)
}

func (s *sdkHandler) SealBlob(ctx context.Context, args *acapi.SealBlobArgs) error {
	if !args.IsValid() {
		return errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk seal blob request args: %v", *args)
	return s.handler.SealBlob(ctx, args)
}

func (s *sdkHandler) GetBlob(ctx context.Context, args *acapi.GetBlobArgs) (io.ReadCloser, error) {
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk get blob request args: %v", *args)
	loc, err := s.handler.GetBlob(ctx, args)
	if err != nil {
		return nil, err
	}

	arg := &acapi.GetArgs{
		Location: *loc,
		Offset:   args.Offset,
		ReadSize: args.ReadSize,
		Writer:   args.Writer,
	}
	if arg.Location.Size_ == 0 { // means blob not seal, we will fix size
		// At the last of the Slices, maybe some of the slices is an empty slice that hasn't been written yet
		for _, slice := range arg.Location.Slices {
			arg.Location.Size_ += slice.ValidSize
		}
	}

	if err = security.LocationCrcFill(&arg.Location); err != nil {
		return nil, err
	}

	return s.Get(ctx, arg)
}

func (s *sdkHandler) PutBlob(ctx context.Context, args *acapi.PutBlobArgs) (cid proto.ClusterID, err error) {
	if !args.IsValid() {
		return 0, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk put blob request args: %v", *args)

	defer func() {
		// cid != 0, means create ok, but put fail, or seal fail. need delete
		if err != nil && cid != 0 {
			delArgs := &acapi.DelBlobArgs{
				BlobName:  args.BlobName,
				ClusterID: cid,
				ShardKeys: args.ShardKeys,
			}
			if err1 := s.DeleteBlob(ctx, delArgs); err1 != nil {
				span.Warnf("put fail, clean args=%v, cluster=%d, err=%+v", delArgs, cid, err1)
			}
		}
	}()

	loc, err := s.putBlobs(ctx, args)
	if err != nil {
		return loc.ClusterID, err
	}

	if args.NeedSeal {
		sealArgs := &acapi.SealBlobArgs{
			BlobName:  args.BlobName,
			ShardKeys: args.ShardKeys,
			ClusterID: loc.ClusterID,
			Slices:    loc.Slices,
		}
		if err = s.SealBlob(ctx, sealArgs); err != nil {
			span.Warnf("seal fail, seal args=%v", sealArgs)
			return loc.ClusterID, err
		}
	}
	return loc.ClusterID, nil
}

func (s *sdkHandler) alloc(ctx context.Context, args *acapi.AllocArgs) (acapi.AllocResp, error) {
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
	if err := security.LocationCrcSign(&loc, args.Locations); err != nil {
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

	if err := s.handler.Delete(ctx, &proto.Location{
		ClusterID: args.ClusterID,
		SliceSize: 1,
		Slices: []proto.Slice{{
			MinSliceID: args.BlobID,
			Vid:        args.Vid,
			Count:      1,
		}},
	}); err != nil {
		span.Error("stream delete blob failed", errors.Detail(err))
		return err
	}

	span.Info("done /deleteblob request")
	return nil
}

func (s *sdkHandler) doGet(ctx context.Context, args *acapi.GetArgs) (io.ReadCloser, error) {
	span := trace.SpanFromContextSafe(ctx)
	var err error

	span.Debugf("accept sdk request args:%+v", args)
	if !security.LocationCrcVerify(&args.Location) {
		err = errcode.ErrIllegalArguments
		span.Error("stream get args is invalid ", errors.Detail(err))
		return noopBody{}, err
	}

	if args.Writer != nil {
		err = s.zeroCopyGet(ctx, args)
		return noopBody{}, err
	}

	r, w := io.Pipe()
	writer := s.limiter.Writer(ctx, w)
	transfer, err := s.handler.Get(ctx, writer, args.Location, args.ReadSize, args.Offset)
	if err != nil {
		span.Error("stream get prepare failed", errors.Detail(err))
		w.Close()
		return noopBody{}, err
	}

	go func() {
		err = transfer()
		if err != nil {
			span.Error("stream get transfer failed", errors.Detail(err))
			w.CloseWithError(err) // Read will return this error
			return
		}
		span.Info("done sdk get request")
		w.Close() // Read will return io.EOF
	}()

	return r, nil
}

// will get blobs by zero copy, this function has different response, no Close, no go routine
func (s *sdkHandler) zeroCopyGet(ctx context.Context, args *acapi.GetArgs) error {
	span := trace.SpanFromContextSafe(ctx)

	writer := s.limiter.Writer(ctx, args.Writer)
	transfer, err := s.handler.Get(ctx, writer, args.Location, args.ReadSize, args.Offset)
	if err != nil {
		span.Error("stream get prepare failed", errors.Detail(err))
		return err
	}

	err = transfer()
	if err != nil {
		span.Error("stream get transfer failed", errors.Detail(err))
		return err
	}
	span.Info("done sdk get request")

	return nil
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
		if !security.LocationCrcVerify(&loc) {
			span.Infof("invalid crc %+v", loc)
			err = errcode.ErrIllegalArguments
			return
		}
		clusterBlobsN[loc.ClusterID] += len(loc.Slices)
	}

	if len(args.Locations) == 1 {
		loc := args.Locations[0]
		if err := s.handler.Delete(ctx, &loc); err != nil {
			span.Error("stream delete failed", errors.Detail(err))
			resp.FailedLocations = []proto.Location{loc}
		}
		return
	}

	// merge the same cluster locations to one delete message,
	// anyone of this cluster failed, all locations mark failure,
	//
	// a min delete message about 10-20 bytes,
	// max delete locations is 1024, one location is max to 5G,
	// merged message max size about 40MB.
	merged := make(map[proto.ClusterID][]proto.Slice, len(clusterBlobsN))
	for id, n := range clusterBlobsN {
		merged[id] = make([]proto.Slice, 0, n)
	}
	for _, loc := range args.Locations {
		merged[loc.ClusterID] = append(merged[loc.ClusterID], loc.Slices...)
	}

	for cid := range merged {
		if err := s.handler.Delete(ctx, &proto.Location{
			ClusterID: cid,
			SliceSize: 1,
			Slices:    merged[cid],
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

func (s *sdkHandler) doPutObject(ctx context.Context, args *acapi.PutArgs) (proto.Location, acapi.HashSumMap, error) {
	span := trace.SpanFromContextSafe(ctx)
	var err error

	span.Debugf("accept sdk put request args:%+v", args)
	if !args.IsValid() {
		err = errcode.ErrIllegalArguments
		span.Error("stream get args is invalid ", errors.Detail(err))
		return proto.Location{}, nil, err
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
		return proto.Location{}, nil, err
	}

	// hasher sum
	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}

	if err = security.LocationCrcFill(loc); err != nil {
		span.Error("stream put fill location crc", err)
		err = httpError(err)
		return proto.Location{}, nil, err
	}

	span.Infof("done /put request location:%+v hash:%+v", loc, hashSumMap.All())
	return *loc, hashSumMap, nil
}

func (s *sdkHandler) doPutAt(ctx context.Context, args *acapi.PutAtArgs) (hashSumMap acapi.HashSumMap, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk putat request args:%+v", args)

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

	resp = acapi.AllocResp{
		Location: *location,
		Tokens:   security.StreamGenTokens(location),
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
				Body:      bytes.NewReader(part.buf),
			})
			return err
		})
	}

	newCtx := trace.NewContextFromContext(ctx)
	if err := task.Run(ctx, tasks...); err != nil {
		for _, pt := range parts {
			part := pt
			// asynchronously delete blob
			go func() {
				s.deleteBlob(newCtx, &acapi.DeleteBlobArgs{
					ClusterID: part.cid,
					Vid:       part.vid,
					BlobID:    part.bid,
					Size:      int64(part.size),
				})
			}()
		}
		return err
	}
	return nil
}

func (s *sdkHandler) readerPipeline(span trace.Span, reqBody io.Reader,
	closeCh <-chan struct{}, size, blobSize int,
) <-chan []byte {
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

func (s *sdkHandler) putParts(ctx context.Context, args *acapi.PutArgs) (proto.Location, acapi.HashSumMap, error) {
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
		loc    proto.Location
		tokens []string
	)

	signArgs := acapi.SignArgs{}
	success := false
	defer func() {
		if success {
			return
		}

		// force to clean up, even canceled context
		newCtx := trace.NewContextFromSpan(span)
		locations := signArgs.Locations[:]
		if len(locations) > 1 {
			signArgs.Location = loc.Copy()
			signResp, err := s.sign(newCtx, &signArgs)
			if err == nil {
				locations = []proto.Location{signResp.Location.Copy()}
			}
		}
		if len(locations) > 0 {
			if _, err := s.Delete(newCtx, &acapi.DeleteArgs{Locations: locations}); err != nil {
				span.Warnf("clean location '%+v' failed %s", locations, err.Error())
			}
		}
	}()

	// alloc
	allocResp, err := s.alloc(ctx, &acapi.AllocArgs{Size: uint64(args.Size)})
	if err != nil {
		return proto.Location{}, nil, err
	}
	loc = allocResp.Location
	tokens = allocResp.Tokens
	signArgs.Locations = append(signArgs.Locations, loc.Copy())

	// buffer pipeline
	closeCh := make(chan struct{})
	bufferPipe := s.readerPipeline(span, reqBody, closeCh, int(loc.Size_), int(loc.SliceSize))
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
	remainSize := loc.Size_
	restPartsLoc := loc

	readSize := 0
	for readSize < int(loc.Size_) {
		parts := make([]blobPart, 0, s.conf.PartConcurrence)

		// waiting at least one blob
		buf, ok := <-bufferPipe
		if !ok && readSize < int(loc.Size_) {
			return proto.Location{}, nil, errcode.ErrAccessReadRequestBody
		}
		readSize += len(buf)
		parts = append(parts, blobPart{size: len(buf), buf: buf})

		more := true
		for more && len(parts) < s.conf.PartConcurrence {
			select {
			case buf, ok := <-bufferPipe:
				if !ok {
					if readSize < int(loc.Size_) {
						releaseBuffer(parts)
						return proto.Location{}, nil, errcode.ErrAccessReadRequestBody
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

		tryTimes := s.conf.MaxRetry
		for {
			if len(loc.Slices) > acapi.MaxLocationBlobs {
				releaseBuffer(parts)
				return proto.Location{}, nil, errcode.ErrUnexpected
			}

			// feed new params
			currIdx := currBlobIdx
			currCount := currBlobCount
			for i := range parts {
				token := tokens[currIdx]
				if restPartsLoc.Size_ > uint64(loc.SliceSize) && parts[i].size < int(loc.SliceSize) {
					token = tokens[currIdx+1]
				}
				parts[i].token = token
				parts[i].cid = loc.ClusterID
				parts[i].vid = loc.Slices[currIdx].Vid
				parts[i].bid = loc.Slices[currIdx].MinSliceID + proto.BlobID(currCount)

				currCount++
				if loc.Slices[currIdx].Count == currCount {
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
					if loc.Slices[currBlobIdx].Count == currBlobCount {
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
					span.Error("exceed the max retry limit", s.conf.MaxRetry)
					return proto.Location{}, nil, errcode.ErrUnexpected
				}
				tryTimes--
			}

			var restPartsResp *acapi.AllocResp
			// alloc the rest parts
			err = retry.Timed(s.conf.MaxRetry, s.conf.RetryDelayMs).RuptOn(func() (bool, error) {
				resp, err1 := s.alloc(ctx, &acapi.AllocArgs{
					Size:            remainSize,
					BlobSize:        loc.SliceSize,
					AssignClusterID: loc.ClusterID,
					CodeMode:        loc.CodeMode,
				})
				if err1 != nil {
					return true, err1
				}
				if len(resp.Location.Slices) > 0 {
					if newVid := resp.Location.Slices[0].Vid; newVid == loc.Slices[currBlobIdx].Vid {
						return false, fmt.Errorf("alloc the same vid %d", newVid)
					}
				}
				restPartsResp = &resp
				return true, nil
			})
			if err != nil {
				releaseBuffer(parts)
				span.Error("alloc another parts to put", err)
				return proto.Location{}, nil, errcode.ErrUnexpected
			}

			restPartsLoc = restPartsResp.Location
			signArgs.Locations = append(signArgs.Locations, restPartsLoc.Copy())

			if currBlobCount > 0 {
				loc.Slices[currBlobIdx].Count = currBlobCount
				currBlobIdx++
			}
			loc.Slices = append(loc.Slices[:currBlobIdx], restPartsLoc.Slices...)
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
			return proto.Location{}, nil, errcode.ErrUnexpected
		}
		loc = signResp.Location
	}

	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}
	success = true
	return loc, hashSumMap, nil
}

func (s *sdkHandler) putBlobs(ctx context.Context, args *acapi.PutBlobArgs) (proto.Location, error) {
	// create
	created, err := s.CreateBlob(ctx, &acapi.CreateBlobArgs{
		BlobName:  args.BlobName,
		ShardKeys: args.ShardKeys,
		CodeMode:  args.CodeMode,
		Size:      args.Size,
	})
	if err != nil {
		return proto.Location{}, err
	}

	// buf
	loc := created.Location
	failLoc := proto.Location{ClusterID: loc.ClusterID}
	buf, err := memPool.Alloc(int(loc.SliceSize))
	if err != nil {
		return failLoc, err
	}
	defer memPool.Put(buf[:loc.SliceSize]) // prevent buf get smaller

	// put every slice
	needRead := true
	for blobIdx, retryCnt := 0, 0; blobIdx < len(loc.Slices); {
		buf1, sliceIdx, remainSize, err := s.putOneSlice(ctx, args, loc, blobIdx, needRead, buf)
		if err == nil { // reset put next slice
			blobIdx++
			retryCnt = 0
			needRead = true
			buf = buf[:loc.SliceSize]
			continue
		}

		if remainSize == 0 { // means: read error
			return failLoc, err
		}
		if err = s.delFailSlice(ctx, loc, blobIdx, sliceIdx, remainSize); err != nil {
			return failLoc, err
		}
		retryCnt++ // prevent one slice from failing all the time, and cant put ok after alloc
		if retryCnt > s.conf.MaxRetry {
			return failLoc, err
		}

		allocs, err := s.retryAllocSlice(ctx, args, loc, blobIdx, sliceIdx, remainSize)
		if err != nil {
			return failLoc, err
		}
		loc.Slices, blobIdx = s.updateLocationSlices(loc.Slices, allocs, blobIdx, sliceIdx, remainSize)
		needRead = false
		buf = buf1 // prevent repeated io read
	}

	return loc, nil
}

func (s *sdkHandler) putOneSlice(ctx context.Context, args *acapi.PutBlobArgs, loc proto.Location,
	blobIdx int, needRead bool, buf []byte,
) ([]byte, int, uint64, error) {
	slice := loc.Slices[blobIdx]
	var err error

	// todo: dont concurrency, support concurrency next version
	for cnt, remainSize := 0, slice.ValidSize; uint32(cnt) < slice.Count; {
		readSize := uint64(loc.SliceSize)
		if readSize > remainSize {
			readSize = remainSize // maybe less SliceSize, last one
			buf = buf[:readSize]
		}
		if needRead {
			if _, err = io.ReadFull(args.Body, buf); err != nil {
				return []byte{}, 0, 0, err
			}
		}

		_, err = s.doPutAt(ctx, &acapi.PutAtArgs{
			ClusterID: loc.ClusterID,
			Vid:       slice.Vid,
			BlobID:    slice.MinSliceID + proto.BlobID(cnt),
			Size:      int64(len(buf)), // maybe less SliceSize
			Body:      bytes.NewReader(buf),
		})
		if err != nil {
			return buf, cnt, remainSize, err // remainSize must not 0
		}
		cnt++
		remainSize -= readSize
		needRead = true
	}
	return []byte{}, 0, 0, nil
}

// e.g. Slices[
//       {bid:1~4; vid:1}   first
//       {bid:10~14; vid:2}  second
//       {bid:100~104; vid:3} third
//     ],
//  will split slice: if first slice all ok, second slice 10,11 ok; 12,13,14 fail
//	slice{bid:1~4; vid:1}, slice{10,11; vid:2}, slice{15,16,17; vid:4}, slice{200~204; vid:5} ; fail 12,13,14-> 15,16,17
func (s *sdkHandler) retryAllocSlice(ctx context.Context, args *acapi.PutBlobArgs, loc proto.Location,
	blobIdx, sliceIdx int, remainSize uint64,
) ([]proto.Slice, error) {
	var err error
	slice := loc.Slices[blobIdx] // current
	fail := proto.Slice{
		MinSliceID: slice.MinSliceID + proto.BlobID(sliceIdx),
		Vid:        slice.Vid,
		Count:      slice.Count - uint32(sliceIdx), // e.g. slice.Count=3, sliceIdx in [0,3)
		ValidSize:  remainSize,
	}

	var alloc shardnode.AllocSliceRet
	rerr := retry.Timed(s.conf.MaxRetry, s.conf.RetryDelayMs).RuptOn(func() (bool, error) {
		alloc, err = s.handler.AllocSlice(ctx, &acapi.AllocSliceArgs{
			ClusterID: loc.ClusterID,
			BlobName:  args.BlobName,
			ShardKeys: args.ShardKeys,
			CodeMode:  loc.CodeMode,
			Size:      remainSize, // expect fail size
			FailSlice: fail,       // fail part of current slice
		})
		if err != nil {
			return true, err
		}
		for i := range alloc.Slices {
			if alloc.Slices[i].Vid == loc.Slices[blobIdx].Vid {
				return false, fmt.Errorf("alloc the same vid %d", alloc.Slices[i].Vid)
			}
		}
		return true, nil
	})
	if rerr != nil {
		return []proto.Slice{}, rerr
	}
	return alloc.Slices, nil
}

func (s *sdkHandler) delFailSlice(ctx context.Context, loc proto.Location, blobIdx, sliceIdx int, remainSize uint64) error {
	slice := loc.Slices[blobIdx]
	delLoc := proto.Location{
		ClusterID: loc.ClusterID,
		CodeMode:  loc.CodeMode,
		Size_:     remainSize,
		SliceSize: loc.SliceSize,
		Slices: []proto.Slice{{
			MinSliceID: slice.MinSliceID + proto.BlobID(sliceIdx),
			Vid:        slice.Vid,
			Count:      slice.Count - uint32(sliceIdx), // e.g. slice.Count=3, sliceIdx in [0,3)
			ValidSize:  remainSize,
		}},
	}

	err := security.LocationCrcFill(&delLoc)
	if err != nil {
		return err
	}

	if _, err = s.Delete(ctx, &acapi.DeleteArgs{Locations: []proto.Location{delLoc}}); err != nil {
		return err
	}

	return nil
}

func (s *sdkHandler) updateLocationSlices(oldAll, fail []proto.Slice, blobIdx, sliceIdx int, remainSize uint64) ([]proto.Slice, int) {
	slice := oldAll[blobIdx] // current
	undo := oldAll[blobIdx+1:]
	succAll := make([]proto.Slice, 0, blobIdx+1)
	succAll = append(succAll, oldAll[:blobIdx]...) // success slices, include previous success and part of current
	if sliceIdx > 0 {
		succAll = append(succAll, proto.Slice{
			MinSliceID: slice.MinSliceID,
			Vid:        slice.Vid,
			Count:      uint32(sliceIdx), // cant be zero
			ValidSize:  slice.ValidSize - remainSize,
		})
		blobIdx++ // insert split slice, so current idx++
	}

	newSlice := make([]proto.Slice, 0, len(succAll)+len(fail)+len(undo))
	newSlice = append(newSlice, succAll...) // all success slice, include before+current
	newSlice = append(newSlice, fail...)    // alloc fail slice
	newSlice = append(newSlice, undo...)    // undo slice
	// modifying the loc.Slices in this function is useless. unless it's a pointer
	return newSlice, blobIdx
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
	defaulter.LessOrEqual(&cfg.MaxSizePutOnce, defaultMaxSizePutOnce)
	defaulter.LessOrEqual(&cfg.MaxRetry, defaultMaxRetry)
	defaulter.LessOrEqual(&cfg.RetryDelayMs, defaultRetryDelayMs)
	defaulter.LessOrEqual(&cfg.PartConcurrence, defaultPartConcurrence)
	log.SetOutputLevel(cfg.LogLevel)
	if cfg.Logger != nil {
		log.SetOutput(cfg.Logger)
	}
}
