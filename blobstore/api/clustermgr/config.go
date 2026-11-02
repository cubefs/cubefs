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
	"fmt"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
)

type configGetter interface {
	GetConfig(context.Context, string) (string, error)
}

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

// LoadExtendCodemode loads code_mode_extend from clustermgr and registers the
// extend codemodes into the global codemode registry.
func LoadExtendCodemode(ctx context.Context, configer configGetter) error {
	extend, err := configer.GetConfig(ctx, proto.CodeModeExtendKey)
	if err != nil {
		return fmt.Errorf("load codemode extend failed: %w", err)
	}
	extends := make([]codemode.ExtendCodeMode, 0)
	if err := json.Unmarshal([]byte(extend), &extends); err != nil {
		return fmt.Errorf("apply codemode extend failed: %w", err)
	}
	codemode.Extend(extends...)
	return nil
}

// LoadPolicies loads code_mode_extend and code_mode from clustermgr, codemode
// extends are applied before policy parsing so that every policy mode name is
// registered before the validity check.
func LoadPolicies(ctx context.Context, configer configGetter) ([]codemode.Policy, error) {
	if err := LoadExtendCodemode(ctx, configer); err != nil {
		return nil, err
	}
	policyRaw, err := configer.GetConfig(ctx, proto.CodeModeConfigKey)
	if err != nil {
		return nil, fmt.Errorf("load codemode policy failed: %w", err)
	}
	var policies []codemode.Policy
	if err := json.Unmarshal([]byte(policyRaw), &policies); err != nil {
		return nil, fmt.Errorf("parse codemode policy failed: %w", err)
	}
	for _, p := range policies {
		if !p.ModeName.IsValid() {
			return nil, fmt.Errorf("policy has unknown mode_name %q", p.ModeName)
		}
	}
	return policies, nil
}

// EnsureExtend reloads extend when mode is not registered locally.
func EnsureExtend(ctx context.Context, configer configGetter, mode codemode.CodeMode) error {
	if mode.IsValid() {
		return nil
	}
	if err := LoadExtendCodemode(ctx, configer); err != nil {
		return err
	}
	if !mode.IsValid() {
		return fmt.Errorf("codemode %d still invalid after extend reload", mode)
	}
	return nil
}
