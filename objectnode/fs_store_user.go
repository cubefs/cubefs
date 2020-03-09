// Copyright 2018 The ChubaoFS Authors.
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
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	RefreshUserStoreInterval = time.Minute * 1
)

type UserStore struct {
	akInfoStore map[string]*proto.AKPolicy
	akMu        sync.RWMutex
	closeCh     chan struct{}
	closeOnce   sync.Once
}

func (o *ObjectNode) newUserStore() *UserStore {
	us := &UserStore{
		akInfoStore: make(map[string]*proto.AKPolicy),
		closeCh:     make(chan struct{}, 1),
	}
	go o.refresh()
	return us
}

func (o *ObjectNode) refresh() {
	t := time.NewTicker(RefreshUserStoreInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			o.userStore.akMu.Lock()
			for ak := range o.userStore.akInfoStore {
				akPolicy, err := o.mc.UserAPI().GetAKInfo(ak)
				if err != nil {
					delete(o.userStore.akInfoStore, ak)
					log.LogInfof("update user policy failed and delete: accessKey(%v), err(%v)", ak, err)
					continue
				}
				o.userStore.akInfoStore[ak] = akPolicy
			}
			o.userStore.akMu.Unlock()
		case <-o.userStore.closeCh:
			return
		}
	}
}

//TODO where call close?
func (us *UserStore) Close() {
	us.akMu.Lock()
	defer us.akMu.Unlock()
	us.closeOnce.Do(func() {
		close(us.closeCh)
	})
}

func (us *UserStore) Get(accessKey string) (akPolicy *proto.AKPolicy, exit bool) {
	us.akMu.RLock()
	defer us.akMu.RUnlock()
	akPolicy, exit = us.akInfoStore[accessKey]
	return
}

func (us *UserStore) Put(accessKey string, akPolicy *proto.AKPolicy) {
	us.akMu.Lock()
	defer us.akMu.Unlock()
	us.akInfoStore[accessKey] = akPolicy
	return
}

func (us *UserStore) Delete(accessKey string) (err error) {
	us.akMu.Lock()
	defer us.akMu.Unlock()
	delete(us.akInfoStore, accessKey)
	return
}

func (us *UserStore) List(vol, path string) (data [][]byte, err error) {
	//TODO: implement authonode store list method

	return
}
