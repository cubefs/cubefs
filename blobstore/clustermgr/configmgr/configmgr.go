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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	OperTypeSetConfig = iota + 1
	OperTypeDeleteConfig
)

type ConfigMgr struct {
	module               string
	configTbl            *normaldb.ConfigTable
	defaultClusterConfig map[string]string
	mu                   sync.RWMutex
	raftServer           raftserver.RaftServer
}

type ConfigMgrAPI interface {
	Get(ctx context.Context, key string) (val string, err error)
	Set(ctx context.Context, key string, value string) (err error)
	List(ctx context.Context) (allConfig map[string]string, err error)
	Delete(ctx context.Context, key string) (err error)
}

func New(db *normaldb.NormalDB, clusterConfig map[string]interface{}) (*ConfigMgr, error) {
	configTable, err := normaldb.OpenConfigTable(db)
	if err != nil {
		return nil, errors.Info(err, "OpenConfigTable error").Detail(err)
	}

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
		configTbl:            configTable,
		defaultClusterConfig: defaultClusterConfig,
		mu:                   sync.RWMutex{},
	}

	return configManager, nil
}

func (v *ConfigMgr) Get(ctx context.Context, key string) (val string, err error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if key != "" {
		dbVal, err := v.configTbl.Get(key)
		if err != nil {
			val, ok := v.defaultClusterConfig[key]
			if !ok {
				return "", err
			}

			return val, nil
		}
		return dbVal, nil
	}
	return "", errors.New("config key is empty")
}

func (v *ConfigMgr) Delete(ctx context.Context, key string) (err error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	err = v.configTbl.Delete(key)
	return
}

func (v *ConfigMgr) Set(ctx context.Context, key, value string) (err error) {
	configSetArgs := &clustermgr.ConfigSetArgs{
		Key:   key,
		Value: value,
	}
	data, err := json.Marshal(configSetArgs)
	if err != nil {
		return err
	}
	proposeInfo := base.EncodeProposeInfo(v.GetModuleName(), OperTypeSetConfig, data, base.ProposeContext{ReqID: trace.SpanFromContextSafe(ctx).TraceID()})
	err = v.raftServer.Propose(ctx, proposeInfo)
	return
}

func (v *ConfigMgr) List(ctx context.Context) (allConfig map[string]string, err error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	allConfig, err = v.configTbl.List()
	for key, val := range v.defaultClusterConfig {
		if _, ok := allConfig[key]; ok {
			continue
		}
		allConfig[key] = val
	}
	return allConfig, err
}

func (v *ConfigMgr) SetRaftServer(raftServer raftserver.RaftServer) {
	v.raftServer = raftServer
}
