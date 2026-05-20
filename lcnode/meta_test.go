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

import (
	"os"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
)

type MockMetaWrapper struct {
	mu                     sync.Mutex
	updateLeaseExpireCalls []uint64
}

func NewMockMetaWrapper() *MockMetaWrapper {
	return &MockMetaWrapper{}
}

func (*MockMetaWrapper) ReadDirLimitForSnapShotClean(parentID uint64, from string, limit uint64, verSeq uint64, isDir bool) ([]proto.Dentry, error) {
	return nil, nil
}

func (*MockMetaWrapper) Delete_Ver_ll(parentID uint64, name string, isDir bool, verSeq uint64, fullPath string) (*proto.InodeInfo, error) {
	return nil, nil
}

func (*MockMetaWrapper) Lookup_ll(parentID uint64, name string, isAsync bool) (inode uint64, mode uint32, err error) {
	return
}

func (*MockMetaWrapper) InodeGet_ll(inode uint64, isAsync bool) (*proto.InodeInfo, error) {
	switch inode {
	case 1:
		return &proto.InodeInfo{
			Inode:           1,
			AccessTime:      time.Now().AddDate(0, 0, -2),
			Size:            100,
			PoolId:          proto.DefaultSSDPoolId,
			Generation:      101,
			LeaseExpireTime: 101,
		}, nil
	case 2:
		return &proto.InodeInfo{
			Inode:      2,
			AccessTime: time.Now().AddDate(0, 0, -3),
			Size:       200,
			PoolId:     proto.DefaultSSDPoolId,
			Generation: 102,
		}, nil
	case 3:
		return &proto.InodeInfo{
			Inode:      3,
			AccessTime: time.Now().AddDate(0, 0, -4),
			PoolId:     proto.DefaultSSDPoolId,
			Generation: 103,
		}, nil
	case 6:
		return &proto.InodeInfo{
			Inode:       6,
			AccessTime:  time.Now().AddDate(0, 0, -4),
			ForbiddenLc: true,
			PoolId:      proto.DefaultSSDPoolId,
			Generation:  106,
		}, nil
	}
	return nil, nil
}

func (*MockMetaWrapper) DeleteWithCond_ll(parentID, cond uint64, name string, isDir bool, fullPath string, isAsync bool) (*proto.InodeInfo, error) {
	return nil, nil
}

func (*MockMetaWrapper) Evict(inode uint64, fullPath string, isAsync bool) error {
	return nil
}

func (m *MockMetaWrapper) UpdateExtentKeyAfterMigration(inode uint64, storageType uint32, extentKeys []proto.ObjExtentKey, poolId uint8, leaseExpireTime uint64, delayDelMinute uint64, fullPath string) error {
	m.mu.Lock()
	m.updateLeaseExpireCalls = append(m.updateLeaseExpireCalls, leaseExpireTime)
	m.mu.Unlock()
	return nil
}

func (m *MockMetaWrapper) LastUpdateLeaseExpire() (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.updateLeaseExpireCalls) == 0 {
		return 0, false
	}
	return m.updateLeaseExpireCalls[len(m.updateLeaseExpireCalls)-1], true
}

func (*MockMetaWrapper) DeleteMigrationExtentKey(inode uint64, fullPath string) error {
	return nil
}

func (*MockMetaWrapper) BatchInodeGet(inodes []uint64) []*proto.InodeInfo {
	return nil
}

func (*MockMetaWrapper) ReadDirLimit_ll(parentID uint64, from string, limit uint64, isAsync bool) ([]proto.Dentry, error) {
	// for handleDirLimitDepthFirst
	if parentID == 4 {
		return []proto.Dentry{
			{
				Inode: 5,
				Type:  uint32(os.ModeDir),
			},
			{
				Inode: 6,
				Type:  uint32(420),
			},
		}, nil
	}
	// for handleDirLimitBreadthFirst
	if parentID == 5 {
		return nil, nil
	}
	return []proto.Dentry{
		{
			Inode: 1,
			Type:  uint32(420),
		},
		{
			Inode: 2,
			Type:  uint32(420),
		},
		{
			Inode: 3,
			Type:  uint32(420),
		},
		{
			Inode: 4,
			Type:  uint32(os.ModeDir),
		},
		{
			Inode: 5,
			Type:  uint32(os.ModeDir),
		},
	}, nil
}

func (*MockMetaWrapper) ScanInodeByPool(req *proto.ScanInodeByPoolRequest) (*proto.ScanInodeByPoolResponse, error) {
	return &proto.ScanInodeByPoolResponse{
		Inodes:       []uint64{},
		NextInode:    0,
		HasMore:      false,
		TotalScanned: 0,
	}, nil
}

func (*MockMetaWrapper) Close() error {
	return nil
}
