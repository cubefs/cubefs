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

package db

import (
	"github.com/cubefs/cubefs/blobstore/blobnode/base/priority"
	"github.com/cubefs/cubefs/blobstore/blobnode/base/qos"
	rdb "github.com/cubefs/cubefs/blobstore/common/kvstore"
)

const (
	DefaultBatchProcessCount = 1024
	DefaultWritePriRatio     = 0.7       // 70 %
	DefaultWriteBufferSize   = 1 << 20   // 1 M
	DefaultLRUCache          = 256 << 20 // 256 M
)

type MetaConfig struct {
	MetaRootPrefix     string            `json:"meta_root_prefix"`
	SupportInline      bool              `json:"support_inline"`
	TinyFileThresholdB int               `json:"tinyfile_threshold_B"`
	Sync               bool              `json:"sync"`
	BatchProcessCount  int64             `json:"batch_process_count"`
	WritePriRatio      float64           `json:"write_pri_ratio"`
	MetaQos            qos.LevelConfig   `json:"meta_qos"`
	RocksdbOption      rdb.RocksDBOption `json:"rocksdb_option"`
}

func initConfig(conf *MetaConfig) error {
	// check params
	for name := range conf.MetaQos {
		if !priority.IsValidPriName(name) {
			return ErrWrongConfig
		}
	}

	if conf.BatchProcessCount <= 0 {
		conf.BatchProcessCount = DefaultBatchProcessCount
	}

	// [ 55%, 95% ]
	if conf.WritePriRatio <= 0 {
		conf.WritePriRatio = DefaultWritePriRatio
	}
	if conf.WritePriRatio <= 0.55 {
		conf.WritePriRatio = 0.55
	}
	if conf.WritePriRatio >= 0.95 {
		conf.WritePriRatio = 0.95
	}

	if conf.RocksdbOption.WriteBufferSize <= 0 {
		conf.RocksdbOption.WriteBufferSize = DefaultWriteBufferSize
	}

	if conf.RocksdbOption.LRUCache <= 0 {
		conf.RocksdbOption.LRUCache = DefaultLRUCache
	}

	return nil
}
