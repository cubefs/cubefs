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
	"crypto/sha1"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/cubefs/cubefs/blobstore/access/controller"
	"github.com/cubefs/cubefs/blobstore/access/stream"
	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cmd"
	"github.com/cubefs/cubefs/blobstore/common/consul"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/profile"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/common/uptoken"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	limitNameAlloc  = "alloc"
	limitNamePut    = "put"
	limitNamePutAt  = "putat"
	limitNameGet    = "get"
	limitNameDelete = "delete"
	limitNameSign   = "sign"
)

func initTokenSecret(b []byte) {
	stream.InitTokenSecret.Do(func() {
		for idx := range stream.StreamTokenSecretKeys {
			copy(stream.StreamTokenSecretKeys[idx][7:], b)
		}
	})
}

func initWithRegionMagic(regionMagic string) {
	if regionMagic == "" {
		log.Warn("no region magic setting, using default secret keys for checksum")
		return
	}
	b := sha1.Sum([]byte(regionMagic))
	initTokenSecret(b[:8])
	stream.LocationInitSecret(b[:8])
}

type accessStatus struct {
	Limit stream.Status       `json:"limit"`
	Pool  resourcepool.Status `json:"pool"`

	Config   stream.StreamConfig                     `json:"config"`
	Clusters []*clustermgr.ClusterInfo               `json:"clusters"`
	Services map[proto.ClusterID]map[string][]string `json:"services"`
}

// Config service configs
type Config struct {
	cmd.Config

	ServiceRegister consul.Config       `json:"service_register"`
	Stream          stream.StreamConfig `json:"stream"`
	Limit           stream.LimitConfig  `json:"limit"`
}

// Service rpc service
type Service struct {
	config        Config
	streamHandler stream.StreamHandler
	limiter       stream.Limiter
	closer        closer.Closer
}

// New returns an access service
func New(cfg Config) *Service {
	// add region magic checksum to the secret keys
	initWithRegionMagic(cfg.Stream.ClusterConfig.RegionMagic)

	cl := closer.New()
	h, err := stream.NewStreamHandler(&cfg.Stream, cl.Done())
	if err != nil {
		log.Fatalf("new stream handler failed, err: %+v", err)
	}

	return &Service{
		config:        cfg,
		streamHandler: h,
		limiter:       stream.NewLimiter(cfg.Limit),
		closer:        cl,
	}
}

// Close close server
func (s *Service) Close() {
	s.closer.Close()
}

// RegisterService register service to rpc
func (s *Service) RegisterService() {
	if s.config.ServiceRegister.ConsulAddr == "" {
		return
	}
	_, err := consul.ServiceRegister(s.config.BindAddr, &s.config.ServiceRegister)
	if err != nil {
		log.Fatalf("service register failed, err: %v", err)
	}
}

// RegisterAdminHandler register admin handler to profile
func (s *Service) RegisterAdminHandler() {
	profile.HandleFunc(http.MethodGet, "/access/status", func(c *rpc.Context) {
		var admin *stream.StreamAdmin
		if sa := s.streamHandler.Admin(); sa != nil {
			if ad, ok := sa.(*stream.StreamAdmin); ok {
				admin = ad
			}
		}
		if admin == nil {
			c.RespondStatus(http.StatusServiceUnavailable)
			return
		}

		ctx := c.Request.Context()
		span := trace.SpanFromContextSafe(ctx)

		status := new(accessStatus)
		status.Limit = s.limiter.Status()
		status.Pool = admin.MemPool.Status()
		status.Config = admin.Config
		status.Clusters = admin.Controller.All()
		status.Services = make(map[proto.ClusterID]map[string][]string, len(status.Clusters))

		for _, cluster := range status.Clusters {
			service, err := admin.Controller.GetServiceController(cluster.ClusterID)
			if err != nil {
				span.Warn(err.Error())
				continue
			}

			svrs := make(map[string][]string, 1)
			svrName := proto.ServiceNameProxy
			if hosts, err := service.GetServiceHosts(ctx, svrName); err == nil {
				svrs[svrName] = hosts
			} else {
				span.Warn(err.Error())
			}
			status.Services[cluster.ClusterID] = svrs
		}
		c.RespondJSON(status)
	})

	profile.HandleFunc(http.MethodPost, "/access/stream/controller/alg/:alg", func(c *rpc.Context) {
		algInt, err := strconv.ParseUint(c.Param.ByName("alg"), 10, 32)
		if err != nil {
			c.RespondWith(http.StatusBadRequest, "", []byte(err.Error()))
			return
		}

		alg := controller.AlgChoose(algInt)
		if sa := s.streamHandler.Admin(); sa != nil {
			if admin, ok := sa.(*stream.StreamAdmin); ok {
				if err := admin.Controller.ChangeChooseAlg(alg); err != nil {
					c.RespondWith(http.StatusForbidden, "", []byte(err.Error()))
					return
				}

				span := trace.SpanFromContextSafe(c.Request.Context())
				span.Warnf("change cluster choose algorithm to (%d %s)", alg, alg.String())
				c.Respond()
				return
			}
		}

		c.RespondStatus(http.StatusServiceUnavailable)
	}, rpc.OptArgsURI())
}

// Limit rps controller
func (s *Service) Limit(c *rpc.Context) {
	name := ""
	switch c.Request.URL.Path {
	case "/alloc":
		name = limitNameAlloc
	case "/put":
		name = limitNamePut
	case "/putat":
		name = limitNamePutAt
	case "/get":
		name = limitNameGet
	case "/delete":
		name = limitNameDelete
	case "/sign":
		name = limitNameSign
	default:
	}
	if name == "" {
		return
	}

	if err := s.limiter.Acquire(name); err != nil {
		span := trace.SpanFromContextSafe(c.Request.Context())
		span.Info("access concurrent limited", name, err)
		c.AbortWithError(errcode.ErrAccessLimited)
		return
	}
	defer s.limiter.Release(name)
	c.Next()
}

// Put one object
func (s *Service) Put(c *rpc.Context) {
	args := new(access.PutArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept /put request args:%+v", args)
	if !args.IsValid() {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	hashSumMap := args.Hashes.ToHashSumMap()
	hasherMap := make(access.HasherMap, len(hashSumMap))
	// make hashser
	for alg := range hashSumMap {
		hasherMap[alg] = alg.ToHasher()
	}

	rc := s.limiter.Reader(ctx, c.Request.Body)
	loc, err := s.streamHandler.Put(ctx, rc, args.Size, hasherMap)
	if err != nil {
		span.Error("stream put failed", errors.Detail(err))
		c.RespondError(httpError(err))
		return
	}

	// hasher sum
	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}

	if err := stream.LocationCrcFill(loc); err != nil {
		span.Error("stream put fill location crc", err)
		c.RespondError(httpError(err))
		return
	}

	c.RespondJSON(access.PutResp{
		Location:   *loc,
		HashSumMap: hashSumMap,
	})
	span.Infof("done /put request location:%+v hash:%+v", loc, hashSumMap.All())
}

// PutAt put one blob
func (s *Service) PutAt(c *rpc.Context) {
	args := new(access.PutAtArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept /putat request args:%+v", args)
	if !args.IsValid() {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	valid := false
	for _, secretKey := range stream.StreamTokenSecretKeys {
		token := uptoken.DecodeToken(args.Token)
		if token.IsValid(args.ClusterID, args.Vid, args.BlobID, uint32(args.Size), secretKey[:]) {
			valid = true
			break
		}
	}
	if !valid {
		span.Debugf("invalid token:%s", args.Token)
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	hashSumMap := args.Hashes.ToHashSumMap()
	hasherMap := make(access.HasherMap, len(hashSumMap))
	// make hashser
	for alg := range hashSumMap {
		hasherMap[alg] = alg.ToHasher()
	}

	rc := s.limiter.Reader(ctx, c.Request.Body)
	err := s.streamHandler.PutAt(ctx, rc, args.ClusterID, args.Vid, args.BlobID, args.Size, hasherMap)
	if err != nil {
		span.Error("stream putat failed", errors.Detail(err))
		c.RespondError(httpError(err))
		return
	}

	// hasher sum
	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}

	c.RespondJSON(access.PutAtResp{HashSumMap: hashSumMap})
	span.Infof("done /putat request hash:%+v", hashSumMap.All())
}

// Alloc alloc one location
func (s *Service) Alloc(c *rpc.Context) {
	args := new(access.AllocArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept /alloc request args:%+v", args)
	if !args.IsValid() {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	location, err := s.streamHandler.Alloc(ctx, args.Size, args.BlobSize, args.AssignClusterID, args.CodeMode)
	if err != nil {
		span.Error("stream alloc failed", errors.Detail(err))
		c.RespondError(httpError(err))
		return
	}

	if err := stream.LocationCrcFill(location); err != nil {
		span.Error("stream alloc fill location crc", err)
		c.RespondError(httpError(err))
		return
	}

	resp := access.AllocResp{
		Location: *location,
		Tokens:   stream.StreamGenTokens(location),
	}
	c.RespondJSON(resp)
	span.Infof("done /alloc request resp:%+v", resp)
}

// Get read file
func (s *Service) Get(c *rpc.Context) {
	args := new(access.GetArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept /get request args:%+v", args)
	if !args.IsValid() || !stream.LocationCrcVerify(&args.Location) {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	w := c.Writer
	writer := s.limiter.Writer(ctx, w)
	transfer, err := s.streamHandler.Get(ctx, writer, args.Location, args.ReadSize, args.Offset)
	if err != nil {
		span.Error("stream get prepare failed", errors.Detail(err))
		c.RespondError(httpError(err))
		return
	}

	w.Header().Set(rpc.HeaderContentType, rpc.MIMEStream)
	w.Header().Set(rpc.HeaderContentLength, strconv.FormatInt(int64(args.ReadSize), 10))
	if args.ReadSize > 0 && args.ReadSize != args.Location.Size {
		w.Header().Set(rpc.HeaderContentRange, fmt.Sprintf("bytes %d-%d/%d",
			args.Offset, args.Offset+args.ReadSize-1, args.Location.Size))
		c.RespondStatus(http.StatusPartialContent)
	} else {
		c.RespondStatus(http.StatusOK)
	}

	// flush headers to client firstly
	c.Flush()

	err = transfer()
	if err != nil {
		stream.SteamReportDownload(args.Location.ClusterID, "StatusOKError", "-")
		span.Error("stream get transfer failed", errors.Detail(err))
		return
	}
	span.Info("done /get request")
}

// Delete  all blobs in this location
func (s *Service) Delete(c *rpc.Context) {
	args := new(access.DeleteArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	var err error
	var resp access.DeleteResp
	defer func() {
		if err != nil {
			c.RespondError(httpError(err))
			return
		}

		if len(resp.FailedLocations) > 0 {
			span.Errorf("failed locations N %d of %d", len(resp.FailedLocations), len(args.Locations))
			// must return 2xx even if has failed locations,
			// cos rpc read body only on 2xx.
			// TODO: return other http status code
			c.RespondStatusData(http.StatusIMUsed, resp)
			return
		}

		c.RespondJSON(resp)
	}()

	if !args.IsValid() {
		err = errcode.ErrIllegalArguments
		return
	}
	span.Debugf("accept /delete request args: locations %d", len(args.Locations))
	defer span.Info("done /delete request")

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
		if err := s.streamHandler.Delete(ctx, &loc); err != nil {
			span.Error("stream delete failed", errors.Detail(err))
			resp.FailedLocations = []access.Location{loc}
		}
		return
	}

	// merge the same cluster locations to one delete message,
	// anyone of this cluster failed, all locations mark failure,
	//
	// a min delete message about 10-20 bytes,
	// max delete locations is 1024, one location is max to 5G,
	// merged message max size about 40MB.

	merged := make(map[proto.ClusterID][]access.SliceInfo, len(clusterBlobsN))
	for id, n := range clusterBlobsN {
		merged[id] = make([]access.SliceInfo, 0, n)
	}
	for _, loc := range args.Locations {
		merged[loc.ClusterID] = append(merged[loc.ClusterID], loc.Blobs...)
	}

	var wg sync.WaitGroup
	failedCh := make(chan proto.ClusterID, 1)
	done := make(chan struct{})
	go func() {
		for id := range failedCh {
			if resp.FailedLocations == nil {
				resp.FailedLocations = make([]access.Location, 0, len(args.Locations))
			}
			for _, loc := range args.Locations {
				if loc.ClusterID == id {
					resp.FailedLocations = append(resp.FailedLocations, loc)
				}
			}
		}
		close(done)
	}()

	wg.Add(len(merged))
	for id := range merged {
		go func(id proto.ClusterID) {
			if err := s.streamHandler.Delete(ctx, &access.Location{
				ClusterID: id,
				BlobSize:  1,
				Blobs:     merged[id],
			}); err != nil {
				span.Error("stream delete failed", id, errors.Detail(err))
				failedCh <- id
			}
			wg.Done()
		}(id)
	}

	wg.Wait()
	close(failedCh)
	<-done
}

// DeleteBlob delete one blob
func (s *Service) DeleteBlob(c *rpc.Context) {
	args := new(access.DeleteBlobArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept /deleteblob request args:%+v", args)
	if !args.IsValid() {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	valid := false
	for _, secretKey := range stream.StreamTokenSecretKeys {
		token := uptoken.DecodeToken(args.Token)
		if token.IsValid(args.ClusterID, args.Vid, args.BlobID, uint32(args.Size), secretKey[:]) {
			valid = true
			break
		}
	}
	if !valid {
		span.Debugf("invalid token:%s", args.Token)
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	if err := s.streamHandler.Delete(ctx, &access.Location{
		ClusterID: args.ClusterID,
		BlobSize:  1,
		Blobs: []access.SliceInfo{{
			MinBid: args.BlobID,
			Vid:    args.Vid,
			Count:  1,
		}},
	}); err != nil {
		span.Error("stream delete blob failed", errors.Detail(err))
		c.RespondError(httpError(err))
		return
	}

	c.Respond()
	span.Info("done /deleteblob request")
}

// Sign generate crc with locations
func (s *Service) Sign(c *rpc.Context) {
	args := new(access.SignArgs)
	if err := c.ParseArgs(args); err != nil {
		c.RespondError(err)
		return
	}

	ctx := c.Request.Context()
	span := trace.SpanFromContextSafe(ctx)

	if !args.IsValid() {
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}
	span.Debugf("accept /sign request args: %+v", args)

	loc := args.Location
	crcOld := loc.Crc
	if err := stream.LocationCrcSign(&loc, args.Locations); err != nil {
		span.Error("stream sign failed", errors.Detail(err))
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	c.RespondJSON(access.SignResp{Location: loc})
	span.Infof("done /sign request crc %d -> %d, resp:%+v", crcOld, loc.Crc, loc)
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
