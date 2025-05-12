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

package clustermgr

import (
	"context"
	"encoding/json"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type ConfigArgs struct {
	Key string `json:"key"`
}

type ConfigSetArgs struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type AllConfig struct {
	Configs map[string]string `json:"configs"`
}

func (c *Client) GetConfig(ctx context.Context, key string) (ret string, err error) {
	err = c.GetWith(ctx, "/config/get?key="+key, &ret)
	return
}

func (c *Client) SetConfig(ctx context.Context, key, value string) (err error) {
	err = c.PostWith(ctx, "/config/set", nil, &ConfigSetArgs{Key: key, Value: value})
	return
}

func (c *Client) DeleteConfig(ctx context.Context, key string) (err error) {
	err = c.PostWith(ctx, "/config/delete?key="+key, nil, rpc.NoneBody)
	return
}

func LoadExtendCodemode(ctx context.Context, configer interface {
	GetConfig(context.Context, string) (string, error)
},
) error {
	extend, err := configer.GetConfig(ctx, proto.CodeModeExtendKey)
	if err != nil {
		return err
	}
	extends := make([]codemode.ExtendCodeMode, 0)
	if err = json.Unmarshal([]byte(extend), &extends); err != nil {
		return err
	}
	codemode.Extend(extends...)
	return nil
}
