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

const (
	DefaultLRUCache = 256 << 20 // 256 M
)

type MetaConfig struct {
	MetaRootPrefix     string `json:"meta_root_prefix"`
	SupportInline      bool   `json:"support_inline"`
	TinyFileThresholdB int    `json:"tinyfile_threshold_B"`
	Sync               bool   `json:"sync"`
	LRUCacheSize       uint64 `json:"cache_size"`
}

func initConfig(conf *MetaConfig) error {
	// check params
	if conf.LRUCacheSize == 0 {
		conf.LRUCacheSize = DefaultLRUCache
	}

	return nil
}
