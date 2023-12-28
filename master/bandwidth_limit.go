package master

import (
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/time/rate"
	"net/http"
	"strconv"
	"sync/atomic"
)

func (m *Server) setBandwidthLimiter(w http.ResponseWriter, r *http.Request) {
	var (
		bw  uint64
		err error
	)
	if bw, err = parseSetBandwidthLimiterPara(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setBandwidthRateLimit(bw); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	currentBw := atomic.LoadUint64(&m.cluster.cfg.BandwidthRateLimit)
	if err = m.resetBandwidthLimiter(int(currentBw)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set bw to %v success", currentBw)))
	return
}

func parseSetBandwidthLimiterPara(r *http.Request) (bw uint64, err error) {
	r.ParseForm()
	var value string
	if value = r.FormValue(paraBandWidth); value == "" {
		err = keyNotFound(paraBandWidth)
		return
	}
	if bw, err = strconv.ParseUint(value, 10, 64); err != nil {
		return
	}
	return
}

func (m *Server) resetBandwidthLimiter(bw int) error {
	if bw < minBw {
		return fmt.Errorf("bandwidth can't less than: %v,receive value:%v", minBw, bw)
	}
	m.bandwidthRateLimiter.SetLimit(rate.Limit(bw))
	m.bandwidthRateLimiter.SetBurst(bw)
	return nil
}

func (c *Cluster) setBandwidthRateLimit(bw uint64) (err error) {
	if bw < minBw {
		return fmt.Errorf("bandwidth can't less than: %v,receive value:%v", minBw, bw)
	}
	oldBandwidth := atomic.LoadUint64(&c.cfg.BandwidthRateLimit)
	atomic.StoreUint64(&c.cfg.BandwidthRateLimit, bw)

	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setBandwidthRateLimit] err[%v]", err)
		atomic.StoreUint64(&c.cfg.BandwidthRateLimit, oldBandwidth)
		err = errors.New("persistence by raft occurred error")
		return
	}
	return
}

func (c *Cluster) setAPIReqBandwidthRateLimit(opCode uint8, value int64) (err error) {
	oldVal, ok := c.cfg.APIReqBwRateLimitMap[opCode]
	if !ok {
		return fmt.Errorf("action[setAPIReqBandwidthRateLimit] bandwitdh limit is not allowed for opCode:%v", opCode)
	}
	oldMap := c.cfg.APIReqBwRateLimitMap
	tmpMap := make(map[uint8]int64)
	for k, v := range c.cfg.APIReqBwRateLimitMap {
		tmpMap[k] = v
	}
	tmpMap[opCode] = value
	log.LogDebugf("action[setAPIReqBandwidthRateLimit] opCode:%v,val:%v", opCode, value)
	c.cfg.APIReqBwRateLimitMap = tmpMap
	if err = c.syncPutCluster(); err != nil {
		c.cfg.APIReqBwRateLimitMap = oldMap
		log.LogErrorf("action[setAPIReqBandwidthRateLimit] err[%v],opCode:%v,value:%v", err, opCode, oldVal)
	}
	return
}

func (m *Server) getAPIReqLimitInfo(w http.ResponseWriter, r *http.Request) {
	cInfo := &proto.LimitInfo{
		Cluster:              m.cluster.Name,
		ApiReqBwRateLimitMap: m.cluster.cfg.APIReqBwRateLimitMap,
	}
	sendOkReply(w, r, newSuccessHTTPReply(cInfo))
	return
}

func (m *Server) setNodesLiveRatioThreshold(w http.ResponseWriter, r *http.Request) {
	var (
		threshold float64
		err       error
	)
	if threshold, err = parseAndExtractThreshold(r); err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}

	if float32(threshold) < defaultNodesLiveRatio {
		err = fmt.Errorf("nodeLiveRatio can't less than %v", defaultNodesLiveRatio)
		sendErrReply(w, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	if err = m.cluster.setNodesLiveRatio(float32(threshold)); err != nil {
		sendErrReply(w, r, newErrHTTPReply(err))
		return
	}
	sendOkReply(w, r, newSuccessHTTPReply(fmt.Sprintf("set threshold to %v successfully", threshold)))
	return
}

func (c *Cluster) setNodesLiveRatio(val float32) (err error) {
	oldVal := c.cfg.NodesLiveRatio
	c.cfg.NodesLiveRatio = val
	if err = c.syncPutCluster(); err != nil {
		log.LogErrorf("action[setNodesLiveRatio] from %v to %v failed,err[%v]", oldVal, val, err)
		c.cfg.NodesLiveRatio = oldVal
		err = errors.New("persistence by raft occurred error")
		return
	}
	return
}
