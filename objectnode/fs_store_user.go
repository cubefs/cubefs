// Copyright 2019 The ChubaoFS Authors.
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
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/util/exporter"

	"github.com/chubaofs/chubaofs/sdk/master"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	updateUserStoreInterval      = time.Minute * 1
	userBlacklistCleanupInterval = time.Minute * 1
	userBlacklistTTL             = time.Second * 10
	userInfoLoaderNum            = 4
)

type UserInfoStore interface {
	LoadUser(accessKey string) (*proto.UserInfo, error)
}

type StrictUserInfoStore struct {
	mc *master.MasterClient
}

func (s *StrictUserInfoStore) LoadUser(accessKey string) (*proto.UserInfo, error) {
	// if error occurred when loading user, and error is not NotExist, output an ump log
	userInfo, err := s.mc.UserAPI().GetAKInfo(accessKey)
	if err != nil && err != proto.ErrUserNotExists && err != proto.ErrAccessKeyNotExists {
		log.LogErrorf("LoadUser: fetch user info fail: err(%v)", err)
		exporter.Warning(fmt.Sprintf("StrictUserInfoStore load user fail: accessKey(%v) err(%v)", accessKey, err))
	}
	return userInfo, err
}

type CacheUserInfoStore struct {
	mc      *master.MasterClient
	loaders [userInfoLoaderNum]*CacheUserInfoLoader
}

func (s *CacheUserInfoStore) selectLoader(accessKey string) *CacheUserInfoLoader {
	i := crc32.ChecksumIEEE([]byte(accessKey)) % userInfoLoaderNum
	return s.loaders[i]
}

func (s *CacheUserInfoStore) Close() {
	for _, loader := range s.loaders {
		loader.Close()
	}
}

func (s *CacheUserInfoStore) LoadUser(accessKey string) (*proto.UserInfo, error) {
	return s.selectLoader(accessKey).LoadUser(accessKey)
}

func NewUserInfoStore(masters []string, strict bool, users []*proto.AuthUser) UserInfoStore {
	mc := master.NewMasterClient(masters, false)
	mc.SetUsers(users)
	if strict {
		return &StrictUserInfoStore{
			mc: mc,
		}
	}
	store := &CacheUserInfoStore{
		mc: mc,
	}
	for i := 0; i < userInfoLoaderNum; i++ {
		store.loaders[i] = NewUserInfoLoader(mc)
	}
	return store
}

func ReleaseUserInfoStore(store UserInfoStore) {
	if cacheStore, is := store.(*CacheUserInfoStore); is {
		cacheStore.Close()
	}
	return
}

type CacheUserInfoLoader struct {
	mc          *master.MasterClient
	akInfoStore map[string]*proto.UserInfo // mapping: access key -> user info (*proto.UserInfo)
	akInfoMutex sync.RWMutex
	akInitMap   sync.Map // mapping: access key -> *sync.Mutex
	blacklist   sync.Map // mapping: access key -> timestamp (time.Time)
	closeCh     chan struct{}
	closeOnce   sync.Once
}

func NewUserInfoLoader(mc *master.MasterClient) *CacheUserInfoLoader {
	us := &CacheUserInfoLoader{
		mc:          mc,
		akInfoStore: make(map[string]*proto.UserInfo),
		closeCh:     make(chan struct{}, 1),
	}
	go us.scheduleUpdate()
	go us.blacklistCleanup()
	return us
}

func (us *CacheUserInfoLoader) blacklistCleanup() {
	t := time.NewTimer(userBlacklistCleanupInterval)
	for {
		select {
		case <-t.C:
		case <-us.closeCh:
			t.Stop()
			return
		}
		us.blacklist.Range(func(key, value interface{}) bool {
			ts, is := value.(time.Time)
			if !is || time.Since(ts) > userBlacklistTTL {
				us.blacklist.Delete(key)
			}
			return true
		})
		t.Reset(userBlacklistCleanupInterval)
	}
}

func (us *CacheUserInfoLoader) scheduleUpdate() {
	t := time.NewTimer(updateUserStoreInterval)
	aks := make([]string, 0)
	for {
		select {
		case <-t.C:
		case <-us.closeCh:
			t.Stop()
			return
		}

		aks = aks[:0]
		us.akInfoMutex.RLock()
		for ak := range us.akInfoStore {
			aks = append(aks, ak)
		}
		us.akInfoMutex.RUnlock()
		for _, ak := range aks {
			akPolicy, err := us.mc.UserAPI().GetAKInfo(ak)
			if err == proto.ErrUserNotExists || err == proto.ErrAccessKeyNotExists {
				us.akInfoMutex.Lock()
				delete(us.akInfoStore, ak)
				us.akInfoMutex.Unlock()
				us.blacklist.Store(ak, time.Now())
				log.LogDebugf("scheduleUpdate: release user info: accessKey(%v)", ak)
				continue
			}
			// if error info is not empty, it means error occurred communication with master, output an ump log
			if err != nil {
				log.LogErrorf("scheduleUpdate: fetch user info fail: accessKey(%v), err(%v)", ak, err)
				exporter.Warning(fmt.Sprintf("CacheUserInfoLoader get user info fail when scheduling update: err(%v)", err))
				break
			}
			us.akInfoMutex.Lock()
			us.akInfoStore[ak] = akPolicy
			us.akInfoMutex.Unlock()
		}
		t.Reset(updateUserStoreInterval)
	}
}

func (us *CacheUserInfoLoader) syncUserInit(accessKey string) (releaseFunc func()) {
	value, _ := us.akInitMap.LoadOrStore(accessKey, new(sync.Mutex))
	var initMu = value.(*sync.Mutex)
	initMu.Lock()
	log.LogDebugf("syncUserInit: get user init lock: accessKey(%v)", accessKey)
	return func() {
		initMu.Unlock()
		us.akInitMap.Delete(accessKey)
		log.LogDebugf("syncUserInit: release user init lock: accessKey(%v)", accessKey)
	}
}

func (us *CacheUserInfoLoader) LoadUser(accessKey string) (*proto.UserInfo, error) {
	var err error
	// Check if the access key is on the blacklist.
	if val, exist := us.blacklist.Load(accessKey); exist {
		if ts, is := val.(time.Time); is {
			if time.Since(ts) <= userBlacklistTTL {
				return nil, proto.ErrUserNotExists
			}
		}
	}
	var userInfo *proto.UserInfo
	var exist bool
	us.akInfoMutex.RLock()
	userInfo, exist = us.akInfoStore[accessKey]
	us.akInfoMutex.RUnlock()
	if !exist {
		var release = us.syncUserInit(accessKey)
		us.akInfoMutex.RLock()
		userInfo, exist = us.akInfoStore[accessKey]
		if exist {
			us.akInfoMutex.RUnlock()
			release()
			return userInfo, nil
		}
		us.akInfoMutex.RUnlock()

		userInfo, err = us.mc.UserAPI().GetAKInfo(accessKey)
		if err != nil {
			if err != proto.ErrUserNotExists && err != proto.ErrAccessKeyNotExists {
				log.LogErrorf("LoadUser: fetch user info fail: err(%v)", err)
				// if error occurred when loading user, and error is not NotExist, output an ump log
				exporter.Warning(fmt.Sprintf("CacheUserInfoLoader load user info fail: accessKey(%v) err(%v)", accessKey, err))
			}
			release()
			us.blacklist.Store(accessKey, time.Now())
			return nil, err
		}

		us.akInfoMutex.Lock()
		us.akInfoStore[accessKey] = userInfo
		us.akInfoMutex.Unlock()
		release()
	}
	return userInfo, nil
}

func (us *CacheUserInfoLoader) Close() {
	us.akInfoMutex.Lock()
	defer us.akInfoMutex.Unlock()
	us.closeOnce.Do(func() {
		close(us.closeCh)
	})
}
