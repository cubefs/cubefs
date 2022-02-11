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

package rpc

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

// hostItem Host information
type hostItem struct {
	rawHost string
	// The last time the host failed
	lastFailedTime int64
	// The number of times the host can retry
	retryTimes int32
	// Whether the host is a backup host
	isBackup bool

	sync.RWMutex
}

type Selector interface {
	// GetAvailableHosts get the available hosts
	GetAvailableHosts() []string
	// SetFail Mark host unavailable
	SetFail(string)
	// Close If use a background coroutine to enable the broken host, you can use it to close the coroutine
	Close()
}

// allocate hostItem to request
type selector struct {
	// normal hosts
	hosts []*hostItem
	// broken hosts
	crackHosts map[*hostItem]interface{}
	// backup hosts
	backupHost []*hostItem
	hostMap    map[string]*hostItem
	// the frequency for a host to retry, if retryTimes > hostTryTimes the host will be marked as failed
	hostTryTimes int32
	// retry interval after host failure, after this time, the host can be remarked as available
	failRetryIntervalS int64
	// the interval for a host can fail one time, the lastFailedTime will be set to current time if exceeded
	maxFailsPeriodS int64

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
		maxFailsPeriodS:          cfg.MaxFailsPeriodS,
	}
	s.initHostMap()
	if cfg.FailRetryIntervalS < 0 {
		return s
	}
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

// GetAvailableHosts return a host from hosts or backupHost
func (s *selector) GetAvailableHosts() (hosts []string) {
	s.RLock()
	hostLen := len(s.hosts)
	length := len(s.hosts) + len(s.backupHost)
	hosts = make([]string, length)
	for index, host := range s.hosts {
		hosts[index] = host.rawHost
	}
	for index, host := range s.backupHost {
		hosts[index+hostLen] = host.rawHost
	}
	s.RUnlock()
	randomShuffle(hosts, hostLen)
	return
}

// SetFail update the requestFailedRetryTimes of hostItem or disable the host
func (s *selector) SetFail(host string) {
	if s.failRetryIntervalS < 0 {
		return
	}
	item := s.hostMap[host]
	item.Lock()
	now := time.Now().Unix()
	// init last failed time
	if item.lastFailedTime == 0 {
		item.lastFailedTime = now
	}
	// update last failed time
	if now-item.lastFailedTime >= s.maxFailsPeriodS {
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
func randomShuffle(hosts []string, length int) {
	for i := length; i > 0; i-- {
		lastIdx := i - 1
		idx := rand.Intn(i)
		hosts[lastIdx], hosts[idx] = hosts[idx], hosts[lastIdx]
	}
	for i := len(hosts); i > length; i-- {
		lastIdx := i - 1
		idx := rand.Intn(i-length) + length
		hosts[lastIdx], hosts[idx] = hosts[idx], hosts[lastIdx]
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

// enableHost add available host from crackHosts into backupHost or hosts
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
