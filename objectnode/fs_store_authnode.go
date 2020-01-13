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
	authSDK "github.com/chubaofs/chubaofs/sdk/auth"
	"github.com/chubaofs/chubaofs/util/keystore"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	RefreshAuthStoreInterval = time.Minute * 1
)

type authnodeStore struct {
	authKey     string
	authClient  *authSDK.AuthClient
	akCapsStore map[string]*keystore.AccessKeyCaps
	capsMu      sync.RWMutex
	closeCh     chan struct{}
	closeOnce   sync.Once
}

func newAuthStore(authNodes []string, authKey, certFile string, enableHTTPS bool) *authnodeStore {
	authClient := authSDK.NewAuthClient(authNodes, enableHTTPS, certFile)
	as := &authnodeStore{
		authKey:     authKey,
		authClient:  authClient,
		akCapsStore: make(map[string]*keystore.AccessKeyCaps),
		closeCh:     make(chan struct{}, 1),
	}

	go as.refresh()
	return as
}

//TODO where call close?
func (as *authnodeStore) Close() {
	as.capsMu.Lock()
	defer as.capsMu.Unlock()
	as.closeOnce.Do(func() {
		close(as.closeCh)
	})
}

func (as *authnodeStore) Get(accessKey string) (akCaps *keystore.AccessKeyCaps, exit bool) {
	as.capsMu.RLock()
	defer as.capsMu.RUnlock()
	akCaps, exit = as.akCapsStore[accessKey]
	return
}

func (as *authnodeStore) Put(accessKey string, cap *keystore.AccessKeyCaps) {
	as.capsMu.Lock()
	defer as.capsMu.Unlock()
	as.akCapsStore[accessKey] = cap
	return
}

func (as *authnodeStore) refresh() {
	t := time.NewTicker(RefreshAuthStoreInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			as.capsMu.Lock()
			for ak := range as.akCapsStore {
				akCaps, err := as.authClient.API().OSSGetCaps(proto.ObjectServiceID, as.authKey, ak)
				if err != nil {
					delete(as.akCapsStore, ak)
					log.LogInfof("update user policy failed and delete: accessKey(%v), err(%v)", ak, err)
					continue
				}
				as.akCapsStore[ak] = akCaps
			}
			as.capsMu.Unlock()
		case <-as.closeCh:
			return
		}
	}
}

func (as *authnodeStore) GetAkCaps(accessKey string) (*keystore.AccessKeyCaps, error) {
	var err error
	akCaps, exit := as.Get(accessKey)
	if !exit {
		if akCaps, err = as.authClient.API().OSSGetCaps(proto.ObjectServiceID, as.authKey, accessKey); err != nil {
			log.LogInfof("load user policy err: %v", err)
			return akCaps, err
		}
		as.Put(accessKey, akCaps)
	}
	return akCaps, err
}

func (as *authnodeStore) Delete(accessKey string) (err error) {
	as.capsMu.Lock()
	defer as.capsMu.Unlock()
	delete(as.akCapsStore, accessKey)
	return
}

func (as *authnodeStore) List(vol, path string) (data [][]byte, err error) {
	//TODO: implement authonode store list method

	return
}
