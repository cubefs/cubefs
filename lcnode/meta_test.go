// Copyright 2023 The CubeFS Authors.
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

package lcnode

import "github.com/cubefs/cubefs/proto"

type MockMetaWrapper struct{}

func NewMockMetaWrapper() *MockMetaWrapper {
	return &MockMetaWrapper{}
}

func (*MockMetaWrapper) ReadDirLimitByVer(parentID uint64, from string, limit uint64, verSeq uint64, is2nd bool) ([]proto.Dentry, error) {
	return nil, nil
}

func (*MockMetaWrapper) Delete_Ver_ll(parentID uint64, name string, isDir bool, verSeq uint64) (*proto.InodeInfo, error) {
	return nil, nil
}

func (*MockMetaWrapper) Lookup_ll(parentID uint64, name string) (inode uint64, mode uint32, err error) {
	return
}

func (*MockMetaWrapper) BatchInodeGet(inodes []uint64) []*proto.InodeInfo {
	return nil
}

func (*MockMetaWrapper) BatchDelete_ll(dentries []*proto.ScanDentry) {

}

func (*MockMetaWrapper) ReadDirLimit_ll(parentID uint64, from string, limit uint64) ([]proto.Dentry, error) {
	return nil, nil
}
