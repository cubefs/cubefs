package sdk

import (
	"bytes"
	"context"
	"io"
	"net/http"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cubefs/cubefs/blobstore/access"
	acapi "github.com/cubefs/cubefs/blobstore/api/access"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/closer"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

const (
	defaultMaxSizePutOnce  int64 = 1 << 28 // 256MB
	defaultClientTimeoutMs int64 = 5000
)

type ConfigSdk struct {
	access.StreamConfig
	Limit           access.LimitConfig `json:"limit"`
	MaxSizePutOnce  int64              `json:"max_size_put_once"`
	ClientTimeoutMs int64              `json:"client_timeout_ms"`
	LogLevel        log.Level          `json:"log_level"`
	Logger          *lumberjack.Logger `json:"logger"`
}

type sdkHandler struct {
	conf    ConfigSdk
	handler access.StreamHandler
	limiter access.Limiter
	closer  closer.Closer
}

func NewSdkBlobstore(conf *ConfigSdk) (acapi.API, error) {
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
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}

	ctx = acapi.WithReqidContext(ctx)
	if args.Location.Size == 0 || args.ReadSize == 0 {
		return acapi.NoopBody{}, nil
	}

	resp, err := s.doGet(ctx, args)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *sdkHandler) Delete(ctx context.Context, args *acapi.DeleteArgs) (failedLocations []acapi.Location, err error) {
	if !args.IsValid() {
		return nil, errcode.ErrIllegalArguments
	}

	cid := args.Locations[0].ClusterID
	for _, loc := range args.Locations { // only 1 cluster
		if cid != loc.ClusterID {
			return nil, errcode.ErrIllegalArguments
		}
	}

	ctx = acapi.WithReqidContext(ctx)
	locations := make([]acapi.Location, 0, len(args.Locations)) // check location size
	for _, loc := range args.Locations {
		if loc.Size > 0 {
			locations = append(locations, loc.Copy())
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

	ctx = acapi.WithReqidContext(ctx)
	if args.Size <= s.conf.MaxSizePutOnce {
		return s.doPutObject(ctx, args)
	}

	span := trace.SpanFromContextSafe(ctx)
	err = errcode.ErrRequestNotAllow
	span.Errorf("sdk put too large size at once: %+v", err)
	return acapi.Location{}, nil, err
}

func (s *sdkHandler) doGet(ctx context.Context, args *acapi.GetArgs) (resp io.ReadCloser, err error) {
	span := trace.SpanFromContextSafe(ctx)

	span.Debugf("accept sdk request args:%+v", args)
	if !access.VerifyCrc(&args.Location) {
		err = errcode.ErrIllegalArguments
		span.Error("stream get args is invalid ", errors.Detail(err))
		return
	}

	if args.ReadSize > 0 && args.ReadSize != args.Location.Size {
		span.Errorf("stream get size is invalid, bytes %d-%d/%d", args.Offset, args.Offset+args.ReadSize-1, args.Location.Size)
		err = errcode.ErrIllegalArguments
		return
	}

	mp := s.getMemPool()
	if mp == nil {
		return nil, errcode.ErrUnexpected
	}
	buf, err := mp.Alloc(int(args.ReadSize))
	if err != nil {
		return nil, err
	}

	w := bytes.NewBuffer(buf)
	writer := s.limiter.Writer(ctx, w)
	transfer, err := s.handler.Get(ctx, writer, args.Location, args.ReadSize, args.Offset)
	if err != nil {
		span.Error("stream get prepare failed", errors.Detail(err))
		return
	}

	err = transfer()
	if err != nil {
		access.ReportDownload(args.Location.ClusterID, "StatusOKError", "-")
		span.Error("stream get transfer failed", errors.Detail(err))
		return
	}
	span.Info("done sdk get request")

	return io.NopCloser(bytes.NewReader(buf)), nil
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

	blobsN := 0
	for _, loc := range args.Locations {
		if !access.VerifyCrc(&loc) {
			span.Infof("invalid crc %+v", loc)
			err = errcode.ErrIllegalArguments
			return
		}
		blobsN += len(loc.Blobs)
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
	merged := make([]acapi.SliceInfo, 0, blobsN)
	for _, loc := range args.Locations {
		merged = append(merged, loc.Blobs...)
	}

	failedCh := make(chan proto.ClusterID, 1)
	done := make(chan struct{})
	go func() {
		for id := range failedCh {
			if resp.FailedLocations == nil {
				resp.FailedLocations = make([]acapi.Location, 0, len(args.Locations))
			}
			for _, loc := range args.Locations {
				if loc.ClusterID == id {
					resp.FailedLocations = append(resp.FailedLocations, loc)
				}
			}
		}
		close(done)
	}()

	cid := args.Locations[0].ClusterID
	delCh := make(chan struct{}, 1)
	go func() {
		if err := s.handler.Delete(ctx, &acapi.Location{
			ClusterID: cid,
			BlobSize:  1,
			Blobs:     merged,
		}); err != nil {
			span.Error("stream delete failed", cid, errors.Detail(err))
			failedCh <- cid
		}
		delCh <- struct{}{}
	}()

	<-delCh
	close(failedCh)
	<-done
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

	if err = access.FillCrc(loc); err != nil {
		span.Error("stream put fill location crc", err)
		err = httpError(err)
		return
	}

	location = *loc
	span.Infof("done /put request location:%+v hash:%+v", loc, hashSumMap.All())
	return location, hashSumMap, nil
}

func (s *sdkHandler) getMemPool() *resourcepool.MemPool {
	if sa := s.handler.Admin(); sa != nil {
		if ad, ok := sa.(*access.StreamAdmin); ok {
			return ad.MemPool
		}
	}

	return nil
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

func fixConfig(cfg *ConfigSdk) {
	defaulter.LessOrEqual(&cfg.MaxSizePutOnce, defaultMaxSizePutOnce)
	defaulter.Less(&cfg.ClientTimeoutMs, defaultClientTimeoutMs)
	log.SetOutputLevel(cfg.LogLevel)
	if cfg.Logger != nil {
		log.SetOutput(cfg.Logger)
	}
}
