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

package configmgr

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/cubefs/cubefs/blobstore/clustermgr/kvmgr"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	OperTypeSetConfig = iota + 1
	OperTypeDeleteConfig
)

type ConfigMgr struct {
	module               string
	kvMgr                kvmgr.KvMgrAPI
	defaultClusterConfig map[string]string
	mu                   sync.RWMutex
	raftServer           raftserver.RaftServer
}

type ConfigMgrAPI interface {
	Get(ctx context.Context, key string) (val string, err error)
	Set(ctx context.Context, key string, value string) (err error)
	Delete(ctx context.Context, key string) (err error)
}

func New(kvMgr kvmgr.KvMgrAPI, clusterConfig map[string]interface{}) (*ConfigMgr, error) {
	defaultClusterConfig := make(map[string]string)
	for k, v := range clusterConfig {
		if _, ok := v.(string); ok {
			defaultClusterConfig[k] = v.(string)
			continue
		}
		val, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Info(err, "marshal cluster config error").Detail(err)
		}
		defaultClusterConfig[k] = string(val)
	}

	configManager := &ConfigMgr{
		kvMgr:                kvMgr,
		defaultClusterConfig: defaultClusterConfig,
		mu:                   sync.RWMutex{},
	}

	return configManager, nil
}

func (c *ConfigMgr) Get(ctx context.Context, key string) (val string, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if key != "" {
		dbVal, err := c.kvMgr.Get(key)
		if err != nil {
			val, ok := c.defaultClusterConfig[key]
			if !ok {
				return "", err
			}

			return val, nil
		}
		return string(dbVal), nil
	}
	return "", errors.New("config key is empty")
}

func (c *ConfigMgr) Delete(ctx context.Context, key string) (err error) {
	err = c.kvMgr.Delete(key)
	return
}

func (c *ConfigMgr) Set(ctx context.Context, key, value string) (err error) {
	return c.kvMgr.Set(key, []byte(value))
}

func (c *ConfigMgr) SetRaftServer(raftServer raftserver.RaftServer) {
	c.raftServer = raftServer
}
