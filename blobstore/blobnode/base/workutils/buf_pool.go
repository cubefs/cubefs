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

package workutils

import (
	"github.com/cubefs/cubefs/blobstore/common/resourcepool"
	"github.com/cubefs/cubefs/blobstore/util/defaulter"
)

var TaskBufPool *BufPool

var (
	defaultMigrateBufSize     = 1 << 24 // 16M
	defaultRepairBufSize      = 1 << 22 // 4M
	defaultMigrateBufCapacity = 100
	defaultRepairCapacity     = 500
)

type BufConfig struct {
	MigrateBufSize     int `json:"migrate_buf_size"`
	MigrateBufCapacity int `json:"migrate_buf_capacity"`
	RepairBufSize      int `json:"repair_buf_size"`
	RepairBufCapacity  int `json:"repair_buf_capacity"`
}

type BufPool struct {
	bufPool        *resourcepool.MemPool
	migrateBufSize int
	repairBufSize  int
}

func (conf *BufConfig) checkConfig() {
	defaulter.LessOrEqual(&conf.MigrateBufSize, defaultMigrateBufSize)
	defaulter.LessOrEqual(&conf.MigrateBufCapacity, defaultMigrateBufCapacity)
	defaulter.LessOrEqual(&conf.RepairBufSize, defaultRepairBufSize)
	defaulter.LessOrEqual(&conf.RepairBufCapacity, defaultRepairCapacity)
}

func NewBufPool(cfg *BufConfig) *BufPool {
	cfg.checkConfig()
	sizeClasses := map[int]int{
		cfg.RepairBufSize:  cfg.RepairBufCapacity,
		cfg.MigrateBufSize: cfg.MigrateBufCapacity,
	}
	bufPool := resourcepool.NewMemPool(sizeClasses)
	return &BufPool{
		bufPool:        bufPool,
		migrateBufSize: cfg.MigrateBufSize,
		repairBufSize:  cfg.RepairBufSize,
	}
}

func (b *BufPool) GetMigrateBuf() ([]byte, error) {
	return b.bufPool.Alloc(b.migrateBufSize)
}

func (b *BufPool) GetMigrateBufSize() int {
	return b.migrateBufSize
}

func (b *BufPool) GetRepairBuf() ([]byte, error) {
	return b.bufPool.Alloc(b.repairBufSize)
}

func (b *BufPool) Put(buf []byte) error {
	return b.bufPool.Put(buf)
}
