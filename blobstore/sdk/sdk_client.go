package sdk

import (
	"context"
	"io"
	"net/http"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cubefs/cubefs/blobstore/access"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/common/uptoken"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	defaultClientTimeoutMs int64 = 5000 // 5s
)

type noopBody struct{}

var _ io.ReadCloser = (*noopBody)(nil)

func (rc noopBody) Read(p []byte) (n int, err error) { return 0, io.EOF }
func (rc noopBody) Close() error                     { return nil }

type Config struct {
	access.StreamConfig
	Limit           access.LimitConfig `json:"limit"`
	ClientTimeoutMs int64              `json:"client_timeout_ms"`
	LogLevel        log.Level          `json:"log_level"`
	Logger          *lumberjack.Logger `json:"logger"`
}

type sdkHandler struct {
	conf    Config
	handler access.StreamHandler
	limiter access.Limiter
	closer  closer.Closer
}

func New(conf *Config) (acapi.API, error) {
	fixConfig(conf)

	cl := closer.New()
	h, err := access.NewStreamHandler(&conf.StreamConfig, cl.Done())
	if err != nil {
		return nil, err
	}

	return &sdkHandler{
		conf:    *conf,
		handler: h,
		limiter: access.NewLimiter(conf.Limit),
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

func (s *sdkHandler) Put(ctx context.Context, args *acapi.PutArgs) (location acapi.Location, hashSumMap acapi.HashSumMap, err error) {
	if args == nil {
		return acapi.Location{}, nil, errcode.ErrIllegalArguments
	}

	if args.Size == 0 {
		hashSumMap = args.Hashes.ToHashSumMap()
		for alg := range hashSumMap {
			hashSumMap[alg] = alg.ToHasher().Sum(nil)
		}
		return acapi.Location{Blobs: make([]acapi.SliceInfo, 0)}, hashSumMap, nil
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	return s.doPutObject(ctx, args)
}

func (s *sdkHandler) PutAt(ctx context.Context, args *acapi.PutAtArgs) (acapi.HashSumMap, error) {
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	return s.doPutAt(ctx, args)
}

func (s *sdkHandler) Alloc(ctx context.Context, args *acapi.AllocArgs) (acapi.AllocResp, error) {
	if !args.IsValid() {
		return acapi.AllocResp{}, errcode.ErrIllegalArguments
	}

	ctx = acapi.ClientWithReqidContext(ctx)
	return s.doAlloc(ctx, args)
}

func (s *sdkHandler) doGet(ctx context.Context, args *acapi.GetArgs) (resp io.ReadCloser, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept sdk request args:%+v", args)
	if !access.LocationCrcVerify(&args.Location) {
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
		if !access.LocationCrcVerify(&loc) {
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
			for _, loc := range args.Locations {
				if loc.ClusterID == cid {
					resp.FailedLocations = append(resp.FailedLocations, loc)
				}
			}
		}
	}

	return
}

func (s *sdkHandler) doPutObject(ctx context.Context, args *acapi.PutArgs) (location acapi.Location, hashSumMap acapi.HashSumMap, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept sdk put request args:%+v", args)
	if !args.IsValid() {
		err = errcode.ErrIllegalArguments
		span.Error("stream get args is invalid ", errors.Detail(err))
		return
	}

	hashSumMap = args.Hashes.ToHashSumMap()
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
		return
	}

	// hasher sum
	for alg, hasher := range hasherMap {
		hashSumMap[alg] = hasher.Sum(nil)
	}

	if err = access.LocationCrcFill(loc); err != nil {
		span.Error("stream put fill location crc", err)
		err = httpError(err)
		return
	}

	location = *loc
	span.Infof("done /put request location:%+v hash:%+v", loc, hashSumMap.All())
	return location, hashSumMap, nil
}

func (s *sdkHandler) doPutAt(ctx context.Context, args *acapi.PutAtArgs) (hashSumMap acapi.HashSumMap, err error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("accept sdk putat request args:%+v", args)

	valid := false
	for _, secretKey := range access.StreamTokenSecretKeys {
		token := uptoken.DecodeToken(args.Token)
		if token.IsValid(args.ClusterID, args.Vid, args.BlobID, uint32(args.Size), secretKey[:]) {
			valid = true
			break
		}
	}
	if !valid {
		span.Debugf("invalid token:%s", args.Token)
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

	if err = access.LocationCrcFill(location); err != nil {
		span.Error("stream alloc fill location crc", err)
		return resp, err
	}

	resp = acapi.AllocResp{
		Location: *location,
		Tokens:   access.StreamGenTokens(location),
	}
	span.Infof("done /alloc request resp:%+v", resp)

	return resp, nil
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
	log.SetOutputLevel(cfg.LogLevel)
	if cfg.Logger != nil {
		log.SetOutput(cfg.Logger)
	}
}
