// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wal

import "github.com/tiglabs/raft/util"

const (
	DefaultFileCacheCapacity = 4
	DefaultFileSize          = 32 * util.MB
	DefaultSync              = false
	DefaultSyncRotate        = false
)

// Config wal config
type Config struct {
	// FileCacheCapacity  缓存多少个打开的日志文件（包括index等）
	FileCacheCapacity int

	// FileSize 日志文件的大小
	FileSize int

	Sync bool

	// TruncateFirstDummy  初始化时添加一条日志然后截断
	TruncateFirstDummy bool

	SyncRotate bool
}

func (c *Config) GetFileCacheCapacity() int {
	if c == nil || c.FileCacheCapacity <= 0 {
		return DefaultFileCacheCapacity
	}
	return c.FileCacheCapacity
}

func (c *Config) GetFileSize() int {
	if c == nil || c.FileSize <= 0 {
		return DefaultFileSize
	}

	return c.FileSize
}

func (c *Config) GetSync() bool {
	if c == nil {
		return DefaultSync
	}
	return c.Sync
}

func (c *Config) GetTruncateFirstDummy() bool {
	if c == nil {
		return false
	}
	return c.TruncateFirstDummy
}

func (c *Config) GetSyncRotate() bool {
	if c == nil {
		return DefaultSyncRotate
	}
	return c.SyncRotate
}

func (c *Config) dup() *Config {
	if c != nil {
		dc := *c
		return &dc
	} else {
		return nil
	}
}
