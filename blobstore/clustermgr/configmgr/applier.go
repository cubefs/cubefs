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

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *ConfigMgr) LoadData(ctx context.Context) error {
	return nil
}

func (v *ConfigMgr) GetModuleName() string {
	return v.module
}

func (v *ConfigMgr) SetModuleName(module string) {
	v.module = module
}

func (v *ConfigMgr) Apply(ctx context.Context, operTypes []int32, datas [][]byte, contexts []base.ProposeContext) (err error) {
	for i, t := range operTypes {
		span, ctx := trace.StartSpanFromContextWithTraceID(ctx, "", contexts[i].ReqID)
		switch t {
		case OperTypeSetConfig:
			configSetArgs := &clustermgr.ConfigSetArgs{}
			err = json.Unmarshal(datas[i], configSetArgs)
			if err != nil {
				span.Errorf("ConfigMgr.Apply OperTypeSetConfig json unmarshal failed, err: %v, data: %v", err, datas[i])
				return
			}
			err = v.configTbl.Update([]byte(configSetArgs.Key), []byte(configSetArgs.Value))
			if err != nil {
				span.Errorf("ConfigMgr.Apply OperTypeSetConfig update failed, err: %v, args: %v", err, configSetArgs)
				return
			}
		case OperTypeDeleteConfig:
			configDelArgs := &clustermgr.ConfigArgs{}
			err = json.Unmarshal(datas[i], configDelArgs)
			if err != nil {
				span.Errorf("ConfigMgr.Apply OperTypeDeleteConfig json unmarshal failed, err: %v, data: %v", err, datas[i])
				return
			}
			err = v.Delete(ctx, configDelArgs.Key)
			if err != nil {
				span.Errorf("ConfigMgr.Apply OperTypeDeleteConfig delete failed, err: %v, args: %v", err, configDelArgs)
				return
			}
		default:
			err = errors.New("unsupported operation")
			return
		}
	}

	return
}

// Flush will flush memory data into persistent storage
func (v *ConfigMgr) Flush(ctx context.Context) error {
	return nil
}

// Switch manager work when leader change
func (v *ConfigMgr) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
}
