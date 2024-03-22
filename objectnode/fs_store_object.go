// Copyright 2019 The CubeFS Authors.
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
	"context"
)

const (
	META_OSS_VOLUME = ".oss_meta"
)

var (
	// TODO unused
	_ = (*objectStore)(nil).Init
	_ = (*objectStore)(nil).Put
	_ = (*objectStore)(nil).Get
	_ = (*objectStore)(nil).Delete
	_ = (*objectStore)(nil).List
)

type objectStore struct {
	vm *VolumeManager
}

func (s *objectStore) Init(vm *VolumeManager) {
	s.vm = vm
	// TODO: init meta dir
}

func (s *objectStore) Put(ctx context.Context, vol, obj, key string, data []byte) (err error) {
	// do nothing
	return
}

func (s *objectStore) Get(ctx context.Context, vol, obj, key string) (data []byte, err error) {
	// do nothing
	return
}

func (s *objectStore) Delete(ctx context.Context, vol, obj, key string) (err error) {
	// do nothing
	return
}

func (s *objectStore) List(ctx context.Context, vol, obj string) (data [][]byte, err error) {
	// do nothing
	return
}
