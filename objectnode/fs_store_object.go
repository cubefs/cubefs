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
	"github.com/cubefs/cubefs/util/log"
)

const (
	META_OSS_VOLUME = ".oss_meta"
)

type objectStore struct {
	vm *VolumeManager
}

func (s *objectStore) Init(vm *VolumeManager) {
	s.vm = vm
	//TODO: init meta dir
}

func (s *objectStore) Put(vol, obj, key string, data []byte) (err error) {
	log.LogInfo("put object store")

	return
}

func (s *objectStore) Get(vol, obj, key string) (data []byte, err error) {
	return
}

func (s *objectStore) Delete(vol, obj, key string) (err error) {
	return
}

func (s *objectStore) List(vol, obj string) (data [][]byte, err error) {
	return
}
