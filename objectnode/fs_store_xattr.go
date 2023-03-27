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
	"context"
	"strings"

	"github.com/cubefs/cubefs/proto"

	"github.com/cubefs/cubefs/util/log"
)

const (
	volumeRootInode = uint64(1)
)

type xattrStore struct {
	vm *VolumeManager //vol *Volume
}

func (s *xattrStore) Init(vm *VolumeManager) {
	s.vm = vm
}

func (s *xattrStore) getInode(vol, path string) (*Volume, uint64, error) {
	v, err := s.vm.Volume(vol)
	if err != nil {
		return nil, 0, err
	}
	inode := volumeRootInode
	if path != "" && path != "/" {
		items := strings.Split(path, "/")
		for _, item := range items {
			if item == "" {
				continue
			}
			inode, _, err = v.mw.Lookup_ll(context.Background(), inode, item)
			if err != nil {
				return v, inode, err
			}
		}
	}
	return v, inode, nil
}

func (s *xattrStore) Put(vol, path, key string, data []byte) (err error) {
	v, err1 := s.vm.Volume(vol)
	if err1 != nil {
		err = err1
		return
	}
	err = v.SetXAttr(path, key, data, false)
	if err != nil {
		log.LogErrorf("put xattr failed: vol[%v], key[%v], data[%v]", vol, key, data)
	}
	return
}

func (s *xattrStore) Get(vol, path, key string) (val []byte, err error) {
	var v *Volume
	v, err = s.vm.Volume(vol)
	if err != nil {
		return
	}

	var xattrInfo *proto.XAttrInfo
	if xattrInfo, err = v.GetXAttr(path, key); err != nil {
		return
	}
	if xattrInfo == nil {
		return
	}

	var strVal string
	strVal = xattrInfo.XAttrs[key]
	if len(strVal) > 0 {
		val = []byte(strVal)
		return
	}
	return
}

func (s *xattrStore) Delete(vol, path, key string) (err error) {
	var v *Volume
	if v, err = s.vm.Volume(vol); err != nil {
		return
	}
	if err = v.DeleteXAttr(path, key); err != nil {
		log.LogErrorf("delete xattr failed: vol[%v], key[%v]", vol, key)
		return
	}
	return nil
}

func (s *xattrStore) List(vol, obj string) (data [][]byte, err error) {

	return
}
