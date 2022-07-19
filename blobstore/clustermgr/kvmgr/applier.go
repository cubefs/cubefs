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

package kvmgr

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

const (
	OperTypeSetKv = iota + 1
	OperTypeDeleteKv
)

func (t *KvMgr) LoadData(ctx context.Context) error {
	return nil
}

func (t *KvMgr) GetModuleName() string {
	return t.module
}

func (t *KvMgr) SetModuleName(module string) {
	t.module = module
}

func (t *KvMgr) Apply(ctx context.Context, operTypes []int32, datas [][]byte, contexts []base.ProposeContext) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(operTypes))
	errs := make([]error, len(operTypes))

	for i, tp := range operTypes {
		idx := i
		switch tp {
		case OperTypeSetKv:
			kvSetArgs := &clustermgr.SetKvArgs{}
			start := time.Now()
			err = json.Unmarshal(datas[idx], kvSetArgs)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			unmarshalCost := time.Since(start)
			t.taskPool.Run(rand.Intn(defaultApplyConcurrency), func() {
				errs[idx] = t.Set(kvSetArgs.Key, kvSetArgs.Value)
				wg.Done()
				span.Debugf("unmarshal cost time : %v", unmarshalCost)
			})

		case OperTypeDeleteKv:
			kvDeleteArgs := &clustermgr.DeleteKvArgs{}
			err = json.Unmarshal(datas[i], kvDeleteArgs)
			if err != nil {
				errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
				wg.Done()
				continue
			}
			t.taskPool.Run(rand.Intn(defaultApplyConcurrency), func() {
				errs[idx] = t.Delete(kvDeleteArgs.Key)
				wg.Done()
			})
		default:
			err = errors.New("unsupported operation")
			return
		}
	}
	wg.Wait()
	failedCount := 0
	for i := range errs {
		if errs[i] != nil {
			failedCount += 1
			span.Error(fmt.Sprintf("operation type: %d, apply failed => ", operTypes[i]), errors.Detail(errs[i]))
		}
	}
	if failedCount > 0 {
		return errors.New(fmt.Sprintf("batch apply failed, failed count: %d", failedCount))
	}

	return
}

// Flush will flush memory data into persistent storage
func (t *KvMgr) Flush(ctx context.Context) error {
	return nil
}

// Switch manager work when leader change
func (t *KvMgr) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
}
