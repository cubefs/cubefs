package rpc

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// hostItem Host information
type hostItem struct {
	rawHost        string
	lastFailedTime int64
	retryTimes     int32
	isBackup       bool

	sync.RWMutex
}

func (h *hostItem) isAvailable() bool {
	h.RLock()
	defer h.RUnlock()
	return h.retryTimes > 0
}

type Selector interface {
	Get() string
	SetFail(string)
	Close()
}

// allocate hostItem to request
type selector struct {
	hosts              []*hostItem
	crackHosts         map[*hostItem]interface{}
	backupHost         []*hostItem
	hostMap            map[string]*hostItem
	hostTryTimes       int32
	failRetryIntervalS int64

	sync.RWMutex
	cancelDetectionGoroutine context.CancelFunc
}

func NewSelector(cfg *LbConfig) Selector {
	ctx, cancelFunc := context.WithCancel(context.Background())
	rand.Seed(time.Now().UnixNano())
	s := &selector{
		hosts:                    initHost(cfg.Hosts, cfg, false),
		backupHost:               initHost(cfg.BackupHosts, cfg, true),
		hostTryTimes:             cfg.HostTryTimes,
		failRetryIntervalS:       cfg.FailRetryIntervalS,
		crackHosts:               map[*hostItem]interface{}{},
		hostMap:                  map[string]*hostItem{},
		cancelDetectionGoroutine: cancelFunc,
	}
	s.initHostMap()
	go func() {
		s.detectAvailableHostInBack()
		ticker := time.NewTicker(time.Duration(s.failRetryIntervalS) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.detectAvailableHostInBack()
			case <-ctx.Done():
				return
			}
		}
	}()
	return s
}

// Get return a host from hosts or backupHost
func (s *selector) Get() (host string) {
	var tmpHosts []*hostItem
	s.RLock()
	if len(s.hosts) == 0 {
		tmpHosts = append(tmpHosts, s.backupHost...)
	} else {
		tmpHosts = append(tmpHosts, s.hosts...)
	}
	s.RUnlock()
	s.randomShuffle(&tmpHosts)
	if len(tmpHosts) > 0 {
		host = tmpHosts[0].rawHost
		if !tmpHosts[0].isAvailable() {
			return s.Get()
		}
		return
	}
	return
}

// SetFail update the requestFailedRetryTimes of hostItem or disable the host
func (s *selector) SetFail(host string) {
	item := s.hostMap[host]
	item.Lock()
	now := time.Now().Unix()
	// init last failed time
	if item.lastFailedTime == 0 {
		item.lastFailedTime = now
		item.retryTimes -= 1
	}
	// update last failed time
	if now-item.lastFailedTime >= s.failRetryIntervalS {
		item.retryTimes = s.hostTryTimes
		item.lastFailedTime = now
	}
	item.retryTimes -= 1
	if item.retryTimes > 0 {
		item.Unlock()
		return
	}
	item.Unlock()
	s.disableHost(item)
}

// detectAvailableHostInBack enable the host from crackHosts
func (s *selector) detectAvailableHostInBack() {
	s.Lock()
	defer s.Unlock()
	for hItem := range s.crackHosts {
		hItem.Lock()
		now := time.Now().Unix()
		if now-hItem.lastFailedTime >= s.failRetryIntervalS {
			hItem.retryTimes = s.hostTryTimes
			hItem.lastFailedTime = 0
			hItem.Unlock()
			s.enableHost(hItem)
			continue
		}
		hItem.Unlock()
	}
}

func initHost(hosts []string, cfg *LbConfig, isBackup bool) (hs []*hostItem) {
	for _, host := range hosts {
		hs = append(hs, &hostItem{
			retryTimes: cfg.HostTryTimes,
			rawHost:    host,
			isBackup:   isBackup,
		})

	}
	return
}

func (s *selector) initHostMap() {
	for _, item := range s.hosts {
		s.hostMap[item.rawHost] = item
	}
	for _, item := range s.backupHost {
		s.hostMap[item.rawHost] = item
	}
}

//  mess up the order of hosts to load balancing
func (s *selector) randomShuffle(hosts *[]*hostItem) {
	for i := len(*hosts); i > 0; i-- {
		lastIdx := i - 1
		idx := rand.Intn(i)
		(*hosts)[lastIdx], (*hosts)[idx] = (*hosts)[idx], (*hosts)[lastIdx]
	}
}

// add unavailable host from hosts or backupHost into crackHosts
func (s *selector) disableHost(item *hostItem) {
	s.Lock()
	defer s.Unlock()
	s.crackHosts[item] = struct{}{}
	index := 0
	var temp *[]*hostItem
	if item.isBackup {
		temp = &s.backupHost
	} else {
		temp = &s.hosts
	}
	for ; index < len(*temp); index++ {
		if item == (*temp)[index] {
			if index == len(*temp)-1 {
				*temp = (*temp)[:index]
				return
			}
			*temp = append((*temp)[:index], (*temp)[index+1:]...)
			return
		}
	}
}

//enableHost add available host from crackHosts into backupHost or hosts
func (s *selector) enableHost(hItem *hostItem) {
	delete(s.crackHosts, hItem)
	if hItem.isBackup {
		s.backupHost = append(s.backupHost, hItem)
		return
	}
	s.hosts = append(s.hosts, hItem)
}

func (s *selector) Close() {
	s.cancelDetectionGoroutine()
}
