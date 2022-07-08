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

package scopemgr

import (
	"context"
	"encoding/json"

	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	OperTypeAllocScope = iota + 1
)

const (
	module = "scopeMgr"
)

type allocCtx struct {
	Name    string `json:"name"`
	Count   int    `json:"count"`
	Current uint64 `json:"current"`
}

func (s *ScopeMgr) LoadData(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	scopeItems, err := s.tbl.Load()
	if err != nil {
		return err
	}
	s.scopeItems = scopeItems
	span.Info("scope item:", scopeItems)
	return nil
}

func (s *ScopeMgr) GetModuleName() string {
	return module
}

func (s *ScopeMgr) SetModuleName(module string) {
}

func (s *ScopeMgr) Apply(ctx context.Context, operTypes []int32, datas [][]byte, contexts []base.ProposeContext) error {
	span := trace.SpanFromContextSafe(ctx)
	for i := range operTypes {
		_, taskCtx := trace.StartSpanFromContextWithTraceID(ctx, "", contexts[i].ReqID)
		switch operTypes[i] {
		case OperTypeAllocScope:
			args := &allocCtx{}
			err := json.Unmarshal(datas[i], args)
			if err != nil {
				span.Errorf("json unmarshal failed, err: %v, operation type: %d, data: %v", err, operTypes[i], datas[i])
				return errors.Info(err, "json unmarshal failed").Detail(err)
			}
			err = s.applyCommit(taskCtx, args)
			if err != nil {
				return errors.Info(err, "apply commit failed, args: ", args).Detail(err)
			}
		}
	}

	return nil
}

// nothing to do
func (s *ScopeMgr) Flush(ctx context.Context) error {
	return nil
}

// nothing to do
func (s *ScopeMgr) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
}
