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

	"github.com/cubefs/cubefs/proto"
)

const (
	volumeRootInode = uint64(1)
)

type xattrStore struct {
	vm *VolumeManager // vol *Volume
}

func (s *xattrStore) Init(vm *VolumeManager) {
	s.vm = vm
}

func (s *xattrStore) Put(ctx context.Context, vol, path, key string, data []byte) error {
	v, err := s.vm.Volume(ctx, vol)
	if err != nil {
		return err
	}

	return v.SetXAttr(ctx, path, key, data, false)
}

func (s *xattrStore) Get(ctx context.Context, vol, path, key string) (val []byte, err error) {
	var v *Volume
	if v, err = s.vm.Volume(ctx, vol); err != nil {
		return
	}

	var xattrInfo *proto.XAttrInfo
	if xattrInfo, err = v.GetXAttr(ctx, path, key); err != nil {
		return
	}
	if xattrInfo == nil {
		return
	}

	strVal := xattrInfo.XAttrs[key]
	if len(strVal) > 0 {
		val = []byte(strVal)
	}

	return
}

func (s *xattrStore) Delete(ctx context.Context, vol, path, key string) error {
	v, err := s.vm.Volume(ctx, vol)
	if err != nil {
		return err
	}

	return v.DeleteXAttr(ctx, path, key)
}

func (s *xattrStore) List(ctx context.Context, vol, obj string) (data [][]byte, err error) {
	// do nothing
	return
}
