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
	"time"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/cubefs/cubefs/blobstore/access/controller"
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

const (
	_tokenExpiration = time.Hour * 12
)

var (
	// tokenSecretKeys alloc token with the first secret key always,
	// so that you can change the secret key.
	//
	// parse-1: insert a new key at the first index,
	// parse-2: delete the old key at the last index after _tokenExpiration duration.
	tokenSecretKeys = [...][20]byte{
		{0x5f, 0x00, 0x88, 0x96, 0x00, 0xa1, 0xfe, 0x1b},
		{0xff, 0x1f, 0x2f, 0x4f, 0x7f, 0xaf, 0xef, 0xff},
	}
	_initTokenSecret sync.Once
)

func initTokenSecret(b []byte) {
	_initTokenSecret.Do(func() {
		for idx := range tokenSecretKeys {
			copy(tokenSecretKeys[idx][7:], b)
		}
	})
}

func initWithRegionMagic(regionMagic string) {
	if regionMagic == "" {
		log.Warn("no region magic setting, using default secret keys for checksum")
		return
	}

	log.Info("using magic secret keys for checksum with:", regionMagic)
	b := sha1.Sum([]byte(regionMagic))
	initTokenSecret(b[:8])
	initLocationSecret(b[:8])
}

type accessStatus struct {
	Limit Status              `json:"limit"`
	Pool  resourcepool.Status `json:"pool"`

	Config   StreamConfig                            `json:"config"`
	Clusters []*clustermgr.ClusterInfo               `json:"clusters"`
	Services map[proto.ClusterID]map[string][]string `json:"services"`
}

// Config service configs
type Config struct {
	cmd.Config
	ConsulAgentAddr string        `json:"consul_agent_addr"`
	ServiceRegister consul.Config `json:"service_register"`
	Stream          StreamConfig  `json:"stream"`
	Limit           LimitConfig   `json:"limit"`
}

// Service rpc service
type Service struct {
	config        Config
	streamHandler StreamHandler
	limiter       Limiter
	closer        closer.Closer
}

// New returns an access service
func New(cfg Config) *Service {
	consulConf := consulapi.DefaultConfig()
	consulConf.Address = cfg.ConsulAgentAddr

	client, err := consulapi.NewClient(consulConf)
	if err != nil {
		log.Fatalf("new consul client failed, err: %v", err)
	}

	// add region magic checksum to the secret keys
	initWithRegionMagic(cfg.Stream.ClusterConfig.RegionMagic)

	cl := closer.New()
	return &Service{
		config:        cfg,
		streamHandler: NewStreamHandler(&cfg.Stream, client, cl.Done()),
		limiter:       NewLimiter(cfg.Limit),
		closer:        cl,
	}
}

// Close close server
func (s *Service) Close() {
	s.closer.Close()
}

// RegisterService register service to rpc
func (s *Service) RegisterService() {
	_, err := consul.ServiceRegister(s.config.BindAddr, &s.config.ServiceRegister)
	if err != nil {
		log.Fatalf("service register failed, err: %v", err)
	}
}

// RegisterAdminHandler register admin handler to profile
func (s *Service) RegisterAdminHandler() {
	profile.HandleFunc(http.MethodGet, "/access/status", func(c *rpc.Context) {
		var admin *streamAdmin
		if sa := s.streamHandler.Admin(); sa != nil {
			if ad, ok := sa.(*streamAdmin); ok {
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
		status.Pool = admin.memPool.Status()
		status.Config = admin.config
		status.Clusters = admin.controller.All()
		status.Services = make(map[proto.ClusterID]map[string][]string, len(status.Clusters))

		for _, cluster := range status.Clusters {
			service, err := admin.controller.GetServiceController(cluster.ClusterID)
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
		algInt, err := strconv.Atoi(c.Param.ByName("alg"))
		if err != nil {
			c.RespondWith(http.StatusBadRequest, "", []byte(err.Error()))
			return
		}
		if algInt < 0 {
			c.RespondWith(http.StatusBadRequest, "", []byte("invalid algorithm"))
			return
		}

		alg := controller.AlgChoose(algInt)
		if sa := s.streamHandler.Admin(); sa != nil {
			if admin, ok := sa.(*streamAdmin); ok {
				if err := admin.controller.ChangeChooseAlg(alg); err != nil {
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
		span.Debugf("invalid args:%+v", args)
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

	if err := fillCrc(loc); err != nil {
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
		span.Debugf("invalid args:%+v", args)
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	valid := false
	for _, secretKey := range tokenSecretKeys {
		token := uptoken.DecodeToken(args.Token)
		if token.IsValid(args.ClusterID, args.Vid, args.Blobid, uint32(args.Size), secretKey[:]) {
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
	err := s.streamHandler.PutAt(ctx, rc, args.ClusterID, args.Vid, args.Blobid, args.Size, hasherMap)
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
		span.Debugf("invalid args:%+v", args)
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	location, err := s.streamHandler.Alloc(ctx, args.Size, args.BlobSize, args.AssignClusterID, args.CodeMode)
	if err != nil {
		span.Error("stream alloc failed", errors.Detail(err))
		c.RespondError(httpError(err))
		return
	}

	if err := fillCrc(location); err != nil {
		span.Error("stream alloc fill location crc", err)
		c.RespondError(httpError(err))
		return
	}

	resp := access.AllocResp{
		Location: *location,
		Tokens:   genTokens(location),
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
	if !args.IsValid() || !verifyCrc(&args.Location) {
		span.Debugf("invalid args:%+v", args)
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
		if !verifyCrc(&loc) {
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
		span.Debugf("invalid args:%+v", args)
		c.RespondError(errcode.ErrIllegalArguments)
		return
	}

	valid := false
	for _, secretKey := range tokenSecretKeys {
		token := uptoken.DecodeToken(args.Token)
		if token.IsValid(args.ClusterID, args.Vid, args.Blobid, uint32(args.Size), secretKey[:]) {
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
			MinBid: args.Blobid,
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
	if err := signCrc(&loc, args.Locations); err != nil {
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

// genTokens generate tokens
// 1. Returns 0 token if has no blobs.
// 2. Returns 1 token if file size less than blobsize.
// 3. Returns len(blobs) tokens if size divided by blobsize.
// 4. Otherwise returns len(blobs)+1 tokens, the last token
//    will be used by the last blob, even if the last slice blobs' size
//    less than blobsize.
// 5. Each segment blob has its specified token include the last blob.
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
