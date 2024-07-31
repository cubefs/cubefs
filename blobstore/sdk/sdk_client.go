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
	"crypto/sha1"
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
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/common/uptoken"
	"github.com/cubefs/cubefs/blobstore/sdk/base"
	"github.com/cubefs/cubefs/blobstore/sdk/client"
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

func initWithRegionMagic(regionMagic string) {
	if regionMagic == "" {
		log.Warn("no region magic setting, using default secret keys for checksum")
		return
	}
	b := sha1.Sum([]byte(regionMagic))
	stream.TokenInitSecret(b[:8])
	stream.LocationInitSecret(b[:8])
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

	ShardNodeConf client.Config `json:"shard_node_conf"`

	LogLevel log.Level `json:"log_level"`
	Logger   io.Writer `json:"-"`
}

type sdkHandler struct {
	conf         Config
	handler      stream.StreamHandler
	shardNodeCli client.ShardNodeAPI
	limiter      stream.Limiter
	closer       closer.Closer
}

func New(conf *Config) (EbsClient, error) {
	fixConfig(conf)
	// add region magic checksum to the secret keys
	initWithRegionMagic(conf.StreamConfig.ClusterConfig.RegionMagic)

	cl := closer.New()
	h, err := stream.NewStreamHandler(&conf.StreamConfig, cl.Done())
	if err != nil {
		log.Errorf("new stream handler failed, err: %+v", err)
		return nil, err
	}

	return &sdkHandler{
		conf:         *conf,
		handler:      h,
		limiter:      stream.NewLimiter(conf.Limit),
		closer:       cl,
		shardNodeCli: client.New(&conf.ShardNodeConf),
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

func (s *sdkHandler) CreateBlob(ctx context.Context, args *base.CreateBlobArgs) (proto.Blob, error) {
	return proto.Blob{}, nil
}

func (s *sdkHandler) ListBlob(ctx context.Context, args *base.ListBlobArgs) (base.ListBlobResponse, error) {
	return base.ListBlobResponse{}, nil
}

func (s *sdkHandler) StatBlob(ctx context.Context, args *base.StatBlobArgs) (proto.Blob, error) {
	return proto.Blob{}, nil
}

func (s *sdkHandler) SealBlob(ctx context.Context, args *base.SealBlobArgs) error {
	return nil
}

func (s *sdkHandler) GetBlob(ctx context.Context, args *base.GetBlobArgs) (io.ReadCloser, error) {
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	sid, host, err := s.handler.GetShard(ctx, &stream.GetShardArgs{
		ClusterID: args.ClusterID,
		BlobName:  args.BlobName,
		Mode:      stream.GetShardModeLeader,
	})
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			s.handler.PunishShard(ctx, args.ClusterID, sid, host) // clusterID, shardID, host
		}
	}()
	blob, err := s.shardNodeCli.GetBlob(ctx, host, &shardnode.GetBlobRequest{})
	if err != nil {
		return nil, err
	}

	arg := &acapi.GetArgs{
		Location: blob.Blob.Location,
		Offset:   args.Offset,
		ReadSize: args.ReadSize,
		Writer:   args.Writer,
	}
	return s.Get(ctx, arg)
}

func (s *sdkHandler) DeleteBlob(ctx context.Context, args *base.DelBlobArgs) error {
	return nil
}

func (s *sdkHandler) PutBlob(ctx context.Context, args *base.PutBlobArgs) (proto.ClusterID, error) {
	return 0, nil
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
	for _, secretKey := range stream.TokenSecretKeys() {
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
	if !stream.LocationCrcVerify(&args.Location) {
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
		if !stream.LocationCrcVerify(&loc) {
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

	if err = stream.LocationCrcFill(loc); err != nil {
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

	valid := false
	for _, secretKey := range stream.TokenSecretKeys() {
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
					Token:     part.token,
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
