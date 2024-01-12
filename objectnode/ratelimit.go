// Copyright 2023 The CubeFS Authors.
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

package objectnode

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/concurrent"
	"github.com/cubefs/cubefs/util/flowctrl"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ratelimit"
)

const (
	PUT                  = "put"
	List                 = "list"
	DefaultFlowLimitSize = 4 * 1024 // 4kB
)

var putApi = map[string]string{
	strings.ToLower(PUT_OBJECT):       PUT,
	strings.ToLower(COPY_OBJECT):      PUT,
	strings.ToLower(POST_OBJECT):      PUT,
	strings.ToLower(UPLOAD_PART):      PUT,
	strings.ToLower(UPLOAD_PART_COPY): PUT,
}

var listApi = map[string]string{
	List_BUCKETS:           List,
	LIST_OBJECTS:           List,
	LIST_OBJECTS_V2:        List,
	LIST_PARTS:             List,
	LIST_MULTIPART_UPLOADS: List,
}

type RateLimiter interface {
	AcquireLimitResource(uid string, api string) error
	ReleaseLimitResource(uid string, api string)
	GetResponseWriter(uid string, api string, w io.Writer) io.Writer
	GetReader(uid string, api string, r io.Reader) io.Reader
}

type RateLimit struct {
	S3ApiRateLimitMgr map[string]UserRateManager      // api -> UserRateMgr
	ApiLimitConf      map[string]*proto.UserLimitConf // api -> UserLimitConf
	putApi            map[string]string
	limitMutex        sync.RWMutex
}

func NewRateLimit(apiLimitConf map[string]*proto.UserLimitConf) RateLimiter {
	if len(apiLimitConf) == 0 {
		return &NullRateLimit{}
	}

	s3ApiRateLimitMgr := make(map[string]UserRateManager, len(apiLimitConf))
	for api, userLimitConf := range apiLimitConf {
		userRateManager := NewUserRateMgr(userLimitConf)
		s3ApiRateLimitMgr[api] = userRateManager
	}

	rateLimit := &RateLimit{
		S3ApiRateLimitMgr: s3ApiRateLimitMgr,
		ApiLimitConf:      apiLimitConf,
		putApi:            putApi,
	}
	return rateLimit
}

func (r *RateLimit) AcquireLimitResource(uid string, api string) error {
	api = strings.ToLower(api)
	if putTotal, isPutApi := r.putApi[api]; isPutApi {
		api = putTotal
	}
	userRateMgr, ok := r.S3ApiRateLimitMgr[api]
	if !ok {
		return nil
	}
	// QPS
	if allowed, _ := userRateMgr.QPSLimitAllowed(uid); !allowed {
		return TooManyRequests
	}
	// Concurrency
	if err := userRateMgr.ConcurrentLimitAcquire(uid); err != nil {
		return TooManyRequests
	}
	return nil
}

func (r *RateLimit) ReleaseLimitResource(uid string, api string) {
	api = strings.ToLower(api)
	if putTotal, isPutApi := r.putApi[api]; isPutApi {
		api = putTotal
	}
	userRateMgr, ok := r.S3ApiRateLimitMgr[api]
	if !ok {
		return
	}
	userRateMgr.ConcurrentLimitRelease(uid)
}

func (r *RateLimit) GetResponseWriter(uid string, api string, w io.Writer) io.Writer {
	api = strings.ToLower(api)
	userRateMgr, ok := r.S3ApiRateLimitMgr[api]
	if !ok {
		return w
	}
	return userRateMgr.GetResponseWriter(uid, w)
}

func (r *RateLimit) GetReader(uid string, api string, reader io.Reader) io.Reader {
	api = strings.ToLower(api)
	if putTotal, isPutApi := r.putApi[api]; isPutApi {
		api = putTotal
	}
	userRateMgr, ok := r.S3ApiRateLimitMgr[api]
	if !ok {
		return reader
	}
	return userRateMgr.GetReader(uid, reader)
}

// No RateLimit
type NullRateLimit struct{}

func (n *NullRateLimit) AcquireLimitResource(uid string, api string) error {
	return nil
}

func (n *NullRateLimit) ReleaseLimitResource(uid string, api string) {
	return
}

func (n *NullRateLimit) GetResponseWriter(uid string, api string, w io.Writer) io.Writer {
	return w
}

func (n *NullRateLimit) GetReader(uid string, api string, r io.Reader) io.Reader {
	return r
}

type UserRateManager interface {
	QPSLimitAllowed(uid string) (bool, time.Duration)
	ConcurrentLimitAcquire(uid string) error
	ConcurrentLimitRelease(uid string)
	GetResponseWriter(uid string, w io.Writer) io.Writer
	GetReader(uid string, r io.Reader) io.Reader
}

// UserRateMgr specific api user rate Manager
type UserRateMgr struct {
	BandWidthLimit  *flowctrl.KeyFlowCtrl
	QPSLimit        *ratelimit.KeyRateLimit
	ConcurrentLimit *concurrent.KeyConcurrentLimit
	UserLimitConf   *proto.UserLimitConf
}

func NewUserRateMgr(conf *proto.UserLimitConf) UserRateManager {
	bandWidthLimit := flowctrl.NewKeyFlowCtrl()
	qpsLimit := ratelimit.NewKeyRateLimit()
	concurrentLimit := concurrent.NewLimit()

	userRateMgr := &UserRateMgr{
		BandWidthLimit:  bandWidthLimit,
		QPSLimit:        qpsLimit,
		ConcurrentLimit: concurrentLimit,
		UserLimitConf:   conf,
	}

	return userRateMgr
}

func (r *UserRateMgr) QPSLimitAllowed(uid string) (bool, time.Duration) {
	defaultQPSLimit := r.UserLimitConf.QPSQuota[proto.DefaultUid]
	usrQPSLimit := r.UserLimitConf.QPSQuota[uid]
	qpsQuota := getUserLimitQuota(defaultQPSLimit, usrQPSLimit)
	qps, err := safeConvertUint64ToInt(qpsQuota)
	if err != nil {
		log.LogWarnf("QPSLimitAllowed: safeConvertUint64ToInt err[%v]", err)
		return true, 0
	}
	if qps == 0 {
		return true, 0
	}
	log.LogDebugf("QPSLimit: defaultQPSLimit[%d] usrQPSLimit[%d] uid[%s]", defaultQPSLimit, usrQPSLimit, uid)
	qpsLimit := r.QPSLimit.Acquire(uid, qps)

	return !qpsLimit.Limit(), 0
}

func (r *UserRateMgr) ConcurrentLimitAcquire(uid string) error {
	defaultConcurrentLimit := r.UserLimitConf.ConcurrentQuota[proto.DefaultUid]
	usrConcurrentLimit := r.UserLimitConf.ConcurrentQuota[uid]

	concurrentQuota := getUserLimitQuota(defaultConcurrentLimit, usrConcurrentLimit)
	if concurrentQuota == 0 {
		return nil
	}
	log.LogDebugf("ConcurrentLimit: defaultConcurrentLimit[%d] usrConcurrentLimit[%d] uid[%s]", defaultConcurrentLimit, usrConcurrentLimit, uid)
	return r.ConcurrentLimit.Acquire(uid, int64(concurrentQuota))
}

func (r *UserRateMgr) ConcurrentLimitRelease(uid string) {
	r.ConcurrentLimit.Release(uid)
}

func (r *UserRateMgr) GetResponseWriter(uid string, w io.Writer) io.Writer {
	defaultBandWidthLimit := r.UserLimitConf.BandWidthQuota[proto.DefaultUid]
	usrBandWidthLimit := r.UserLimitConf.BandWidthQuota[uid]

	bandWidthQuota := getUserLimitQuota(defaultBandWidthLimit, usrBandWidthLimit)
	if bandWidthQuota == 0 {
		return w
	}
	log.LogDebugf("WriterFlowCtrl: defaultBandWidthLimit[%d] usrBandWidthLimit[%d] uid[%s]", defaultBandWidthLimit, usrBandWidthLimit, uid)
	rate, _ := convertUint64ToInt(bandWidthQuota)
	flowCtrl := r.BandWidthLimit.Acquire(uid, rate)
	w = flowctrl.NewRateWriterWithCtrl(w, flowCtrl)

	return w
}

func (r *UserRateMgr) GetReader(uid string, reader io.Reader) io.Reader {
	defaultBandWidthLimit := r.UserLimitConf.BandWidthQuota[proto.DefaultUid]
	usrBandWidthLimit := r.UserLimitConf.BandWidthQuota[uid]

	bandWidthQuota := getUserLimitQuota(defaultBandWidthLimit, usrBandWidthLimit)
	if bandWidthQuota == 0 {
		return reader
	}
	log.LogDebugf("ReaderFlowCtrl: defaultBandWidthLimit[%d] usrBandWidthLimit[%d] uid[%s]", defaultBandWidthLimit, usrBandWidthLimit, uid)
	rate, _ := convertUint64ToInt(bandWidthQuota)
	flowCtrl := r.BandWidthLimit.Acquire(uid, rate)
	reader = flowctrl.NewRateReaderWithCtrl(reader, flowCtrl)

	return reader
}

// priority: usrLimit > defaultLimit
func getUserLimitQuota(defaultLimit, usrLimit uint64) uint64 {
	if usrLimit != 0 {
		return usrLimit
	}
	return defaultLimit
}

func (o *ObjectNode) Reload(data []byte) error {
	s3QosResponse := proto.S3QoSResponse{}
	if err := json.Unmarshal(data, &s3QosResponse); err != nil {
		return err
	}
	apiLimitConf := s3QosResponse.ApiLimitConf
	s3NodeNum := s3QosResponse.Nodes
	if s3NodeNum == 0 {
		o.limitMutex.Lock()
		o.rateLimit = &NullRateLimit{}
		o.limitMutex.Unlock()
		return nil
	}
	for _, userLimitConf := range apiLimitConf {
		for uid, bandWidthQuota := range userLimitConf.BandWidthQuota {
			quota := bandWidthQuota / s3NodeNum
			if quota == 0 && bandWidthQuota != 0 {
				quota += 1
			}
			userLimitConf.BandWidthQuota[uid] = quota
		}
		for uid, qpsQuota := range userLimitConf.QPSQuota {
			quota := qpsQuota / s3NodeNum
			if quota == 0 && qpsQuota != 0 {
				quota += 1
			}
			userLimitConf.QPSQuota[uid] = quota
		}
		for uid, concurrentQuota := range userLimitConf.ConcurrentQuota {
			quota := concurrentQuota / s3NodeNum
			if quota == 0 && concurrentQuota != 0 {
				quota += 1
			}
			userLimitConf.ConcurrentQuota[uid] = quota
		}
	}
	o.limitMutex.Lock()
	o.rateLimit = NewRateLimit(apiLimitConf)
	o.limitMutex.Unlock()
	return nil
}

func (o *ObjectNode) requestRemote() (data []byte, err error) {
	data, err = o.mc.AdminAPI().GetS3QoSInfo()
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (o *ObjectNode) AcquireRateLimiter() RateLimiter {
	o.limitMutex.RLock()
	rateLimit := o.rateLimit
	o.limitMutex.RUnlock()
	return rateLimit
}

func convertUint64ToInt(num uint64) (int, error) {
	str := strconv.FormatUint(num, 10)
	parsed, err := strconv.ParseInt(str, 10, 0)
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}
