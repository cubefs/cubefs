package metanode

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
)

const (
	defaultOpLimitBurst = 512
	// Default rate limit for operations (IOPS)
	defaultAsyncOpLimit = 10000 // 10K IOPS for async operations
)

type OpLimitInfo struct {
	OpName         string        `json:"op_name"`         // Operation name
	OpCode         uint8         `json:"op_code"`         // Operation code
	Limit          uint32        `json:"limit"`           // IOPS limit
	LimiterTimeout uint32        `json:"limiter_timeout"` // Limiter timeout in seconds
	Limiter        *rate.Limiter `json:"-"`               // Rate limiter instance
}

func (li *OpLimitInfo) InitLimiter() {
	li.Limiter = rate.NewLimiter(rate.Limit(li.Limit), defaultOpLimitBurst)
}

type OpLimiter struct {
	m            sync.RWMutex
	limiterInfos map[uint8]*OpLimitInfo // Rate limiter info indexed by OpCode
}

func newOpLimiter() *OpLimiter {
	ol := &OpLimiter{
		limiterInfos: make(map[uint8]*OpLimitInfo),
	}
	// Initialize default limiters for async operations
	ol.initDefaultAsyncLimiters()
	return ol
}

func (ol *OpLimiter) clear() {
	for k := range ol.limiterInfos {
		delete(ol.limiterInfos, k)
	}
}

func (ol *OpLimiter) Clear() {
	ol.m.Lock()
	defer ol.m.Unlock()
	ol.clear()
}

func (ol *OpLimiter) Replace(limiterInfos map[uint8]*OpLimitInfo) {
	ol.m.Lock()
	defer ol.m.Unlock()
	ol.clear()
	for k, v := range limiterInfos {
		ol.limiterInfos[k] = v
	}
}

func (ol *OpLimiter) SetLimiter(opName string, limit uint32, limiterTimeout uint32) (err error) {
	// limiterTimeout can be 0
	if limit == 0 {
		return fmt.Errorf("limit cannot be zero")
	}

	var normalizedName string
	var opCode uint8
	if err, normalizedName, opCode = ol.IsOpNameValid(opName); err != nil {
		return err
	}

	lInfo := &OpLimitInfo{
		OpName:         normalizedName,
		OpCode:         opCode,
		Limit:          limit,
		LimiterTimeout: limiterTimeout,
	}
	lInfo.InitLimiter()

	ol.m.Lock()
	ol.limiterInfos[opCode] = lInfo
	ol.m.Unlock()
	return nil
}

func (ol *OpLimiter) RmLimiter(opName string) (err error) {
	var opCode uint8
	if err, _, opCode = ol.IsOpNameValid(opName); err != nil {
		return err
	}

	ol.m.Lock()
	delete(ol.limiterInfos, opCode)
	ol.m.Unlock()
	return nil
}

func (ol *OpLimiter) Wait(opCode uint8) (err error) {
	var lInfo *OpLimitInfo
	var ok bool
	ol.m.RLock()
	if lInfo, ok = ol.limiterInfos[opCode]; !ok {
		ol.m.RUnlock()
		log.LogDebugf("no op limiter for opCode[%v]", opCode)
		return nil
	}
	ol.m.RUnlock()

	// Unified rate limiting strategy based on timeout configuration
	if lInfo.LimiterTimeout == 0 {
		if !lInfo.Limiter.Allow() {
			log.LogWarnf("wait op limiter for opCode[%v] rate limited", opCode)
			return fmt.Errorf("rate limited")
		}
		return nil
	}
	// timeout>0: wait until timeout, then fail
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(lInfo.LimiterTimeout))
	defer cancel()
	err = lInfo.Limiter.Wait(ctx)
	if err != nil {
		log.LogWarnf("wait op limiter for opCode[%v] timeout after %ds: %v", opCode, lInfo.LimiterTimeout, err)
		return fmt.Errorf("rate limited")
	}
	log.LogDebugf("wait op limiter for opCode[%v] succeeded", opCode)
	return nil
}

func (ol *OpLimiter) IsOpNameValid(name string) (err error, normalizedName string, opCode uint8) {
	normalizedName = strings.ToLower(name)
	var ok bool
	if opCode, ok = proto.GOpInfo[normalizedName]; ok {
		return nil, normalizedName, opCode
	}
	return fmt.Errorf("op name [%v] is not valid", name), normalizedName, opCode
}

// isAsyncOperation checks if an operation is an async operation based on its opcode
func (ol *OpLimiter) isAsyncOperation(opCode uint8) bool {
	asyncOpcodes := map[uint8]bool{
		proto.OpMetaAsyncReadDir:           true,
		proto.OpMetaAsyncLookup:            true,
		proto.OpMetaAsyncInodeGet:          true,
		proto.OpMetaAsyncCreateInode:       true,
		proto.OpMetaAsyncCreateDentry:      true,
		proto.OpMetaAsyncDeleteDentry:      true,
		proto.OpMetaAsyncXAttrSet:          true,
		proto.OpMetaAsyncXAttrGet:          true,
		proto.OpMetaAsyncLockDir:           true,
		proto.OpMetaAsyncTxCreateInode:     true,
		proto.OpMetaAsyncTxCreateDentry:    true,
		proto.OpMetaAsyncTxCreate:          true,
		proto.OpMetaAsyncGetInodeQuota:     true,
		proto.OpMetaAsyncUnlinkInode:       true,
		proto.OpMetaAsyncEvictInode:        true,
		proto.OpMetaAsyncLinkInode:         true,
		proto.OpMetaAsyncUpdateDentry:      true,
		proto.OpMetaAsyncBatchDeleteDentry: true,
		proto.OpMetaAsyncExtentsList:       true,
		proto.OpQuotaAsyncCreateDentry:     true,
	}
	return asyncOpcodes[opCode]
}

// initDefaultAsyncLimiters initializes default rate limiters for async operations
func (ol *OpLimiter) initDefaultAsyncLimiters() {
	asyncOps := make(map[string]uint8)
	for opName, opCode := range proto.GOpInfo {
		if ol.isAsyncOperation(opCode) {
			asyncOps[opName] = opCode
		}
	}

	ol.m.Lock()
	defer ol.m.Unlock()

	// Initialize default limiters for async operations if not already configured
	for opName, opCode := range asyncOps {
		if _, exists := ol.limiterInfos[opCode]; !exists {
			lInfo := &OpLimitInfo{
				OpName:         opName,
				OpCode:         opCode,
				Limit:          defaultAsyncOpLimit,
				LimiterTimeout: 0,
			}
			lInfo.InitLimiter()
			ol.limiterInfos[opCode] = lInfo
			log.LogInfof("initialized default async op limiter: %s (opcode=%d) limit=%d",
				opName, opCode, defaultAsyncOpLimit)
		}
	}
}
