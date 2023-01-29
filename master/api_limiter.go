package master

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
	"strings"
	"sync"
	"time"
)

const (
	defaultApiLimitBurst = 1
)

type ApiLimitInfo struct {
	ApiName        string        `json:"api_name"`
	QueryPath      string        `json:"query_path"`
	Limit          uint32        `json:"limit"` // 0: unlimited, other: qps
	LimiterTimeout uint32        `json:"limiter_timeout"`
	Limiter        *rate.Limiter `json:"-"`
}

type ApiLimiter struct {
	m            sync.RWMutex
	limiterInfos map[string]*ApiLimitInfo
}

func newApiLimiter() *ApiLimiter {
	return &ApiLimiter{
		limiterInfos: make(map[string]*ApiLimitInfo),
	}
}

func (l *ApiLimiter) SetLimiter(apiName string, Limit uint32, LimiterTimeout uint32) (err error) {
	var normalizedName string
	var qPath string
	if err, normalizedName, qPath = l.IsApiNameValid(apiName); err != nil {
		return err
	}

	lInfo := &ApiLimitInfo{
		ApiName:        normalizedName,
		QueryPath:      qPath,
		Limit:          Limit,
		LimiterTimeout: LimiterTimeout,
		Limiter:        rate.NewLimiter(rate.Limit(Limit), defaultApiLimitBurst),
	}

	l.m.Lock()
	l.limiterInfos[qPath] = lInfo
	l.m.Unlock()
	return nil
}

func (l *ApiLimiter) RmLimiter(apiName string) (err error) {
	var qPath string
	if err, _, qPath = l.IsApiNameValid(apiName); err != nil {
		return err
	}

	l.m.Lock()
	delete(l.limiterInfos, qPath)
	l.m.Unlock()
	return nil
}

func (l *ApiLimiter) Wait(qPath string) (err error) {

	var lInfo *ApiLimitInfo
	var ok bool
	l.m.RLock()
	if lInfo, ok = l.limiterInfos[qPath]; !ok {
		l.m.RUnlock()
		log.LogDebugf("no api limiter for api[%v]", qPath)
		return nil
	}
	l.m.RUnlock()
	ctx, _ := context.WithTimeout(context.Background(), time.Second*time.Duration(lInfo.LimiterTimeout))
	err = lInfo.Limiter.Wait(ctx)
	if err != nil {
		log.LogErrorf("wait api limiter for api[%v] failed: %v", qPath, err)
		return err
	}
	log.LogDebugf("wait api limiter for api[%v]", qPath)
	return nil
}

func (l *ApiLimiter) IsApiNameValid(name string) (err error, normalizedName, qPath string) {
	normalizedName = strings.ToLower(name)
	var ok bool
	if qPath, ok = proto.GApiInfo[normalizedName]; ok {
		return nil, normalizedName, qPath
	}
	return fmt.Errorf("api name [%v] is not valid", name), normalizedName, qPath
}
