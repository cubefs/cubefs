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
	id      int
	rawHost string
	// The last time the host failed
	lastFailedTime int64
	// The number of times the host can retry
	retryTimes int
	// Whether the host is a backup host
	isBackup bool

	sync.RWMutex
}

func (hi *hostItem) ID() int      { return hi.id }
func (hi *hostItem) Host() string { return hi.rawHost }

// UniqueHost unique host maked by config's hosts and backups
type UniqueHost interface {
	ID() int
	Host() string
}

type Selector interface {
	// GetAllHosts() returns all hosts of the selector
	GetAllHosts() []UniqueHost
	// GetAvailableHosts get the available hosts, those hosts must be in all hosts
	GetAvailableHosts() []UniqueHost
	// SetFailHost try to mark the host unavailable
	SetFailHost(UniqueHost)
	// Close the background coroutine which enable the broken host
	Close()
}

// allocate hostItem to request
type selector struct {
	// the frequency for a host to retry, if retryTimes > hostTryTimes the host will be marked as failed
	hostTryTimes int
	// retry interval after host failure, after this time, the host can be remarked as available
	failRetryIntervalS int
	// the interval for a host can fail one time, the lastFailedTime will be set to current time if exceeded
	maxFailsPeriodS int

	cancelDetectionGoroutine context.CancelFunc

	hostItems map[*hostItem]struct{}

	sync.RWMutex
	availHosts       []*hostItem            // normal available hosts
	availBackupHosts []*hostItem            // backup available hosts
	unavailHosts     map[*hostItem]struct{} // unavailable hosts
}

// NewSelector returns default selector
func NewSelector(cfg *LbConfig) Selector {
	ctx, cancelFunc := context.WithCancel(context.Background())
	rand.Seed(time.Now().UnixNano())
	s := &selector{
		hostTryTimes:             cfg.HostTryTimes,
		failRetryIntervalS:       cfg.FailRetryIntervalS,
		unavailHosts:             make(map[*hostItem]struct{}),
		hostItems:                make(map[*hostItem]struct{}),
		cancelDetectionGoroutine: cancelFunc,
		maxFailsPeriodS:          cfg.MaxFailsPeriodS,
	}
	s.initHosts(cfg)
	if cfg.FailRetryIntervalS < 0 {
		return s
	}
	go func() {
		ticker := time.NewTicker(time.Duration(s.failRetryIntervalS) * time.Second)
		defer ticker.Stop()
		for {
			s.detectUnavailableHosts()
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()
	return s
}

// GetAllHosts returns hosts and backups
func (s *selector) GetAllHosts() (hosts []UniqueHost) {
	hosts = make([]UniqueHost, 0, len(s.hostItems))
	for host := range s.hostItems {
		hosts = append(hosts, host)
	}
	return hosts
}

// GetAvailableHosts return available hosts from hosts and backupHosts
func (s *selector) GetAvailableHosts() (hosts []UniqueHost) {
	s.RLock()
	hostLen := len(s.availHosts)
	length := len(s.availHosts) + len(s.availBackupHosts)
	hosts = make([]UniqueHost, 0, length)
	for _, host := range s.availHosts {
		hosts = append(hosts, host)
	}
	for _, host := range s.availBackupHosts {
		hosts = append(hosts, host)
	}
	s.RUnlock()
	randomShuffle(hosts, hostLen)
	return hosts
}

// SetFailHost update the requestFailedRetryTimes of UniqueHost or disable the host
func (s *selector) SetFailHost(host UniqueHost) {
	if s.failRetryIntervalS < 0 {
		return
	}
	for item := range s.hostItems {
		if item.ID() == host.ID() {
			s.setFailHost(item)
		}
	}
}

func (s *selector) setFailHost(item *hostItem) {
	item.Lock()
	now := time.Now().Unix()
	// init last failed time
	if item.lastFailedTime == 0 {
		item.lastFailedTime = now
	}
	// update last failed time
	if now-item.lastFailedTime >= int64(s.maxFailsPeriodS) {
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

// detectUnavailableHosts enable the host from unavailHosts
func (s *selector) detectUnavailableHosts() {
	var cache []*hostItem
	s.RLock()
	for key := range s.unavailHosts {
		cache = append(cache, key)
	}
	s.RUnlock()

	for _, hItem := range cache {
		hItem.Lock()
		now := time.Now().Unix()
		if now-hItem.lastFailedTime >= int64(s.failRetryIntervalS) {
			hItem.retryTimes = s.hostTryTimes
			hItem.lastFailedTime = 0
			s.enableHost(hItem)
		}
		hItem.Unlock()
	}
}

func (s *selector) initHosts(cfg *LbConfig) {
	id := 1
	for _, host := range cfg.Hosts {
		item := &hostItem{
			id:         id,
			rawHost:    host,
			retryTimes: cfg.HostTryTimes,
			isBackup:   false,
		}
		s.availHosts = append(s.availHosts, item)
		s.hostItems[item] = struct{}{}
		id++
	}
	for _, host := range cfg.BackupHosts {
		item := &hostItem{
			id:         id,
			rawHost:    host,
			retryTimes: cfg.HostTryTimes,
			isBackup:   true,
		}
		s.availBackupHosts = append(s.availBackupHosts, item)
		s.hostItems[item] = struct{}{}
		id++
	}
}

// randomShuffle mess up the order of hosts to load balancing
func randomShuffle(hosts []UniqueHost, length int) {
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

// add unavailable host from hosts or backupHosts into unavailHosts
func (s *selector) disableHost(item *hostItem) {
	s.Lock()
	defer s.Unlock()
	s.unavailHosts[item] = struct{}{}
	index := 0
	var temp *[]*hostItem
	if item.isBackup {
		temp = &s.availBackupHosts
	} else {
		temp = &s.availHosts
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

// enableHost add available host from unavailHosts into backupHosts or hosts
func (s *selector) enableHost(item *hostItem) {
	s.Lock()
	defer s.Unlock()
	delete(s.unavailHosts, item)
	if item.isBackup {
		s.availBackupHosts = append(s.availBackupHosts, item)
		return
	}
	s.availHosts = append(s.availHosts, item)
}

func (s *selector) Close() {
	s.cancelDetectionGoroutine()
}
