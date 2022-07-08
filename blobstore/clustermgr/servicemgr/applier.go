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

package servicemgr

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
)

func (s *ServiceMgr) LoadData(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	return s.tbl.Range(func(key, val []byte) bool {
		var node serviceNode
		if err := json.Unmarshal(val, &node); err != nil {
			span.Panicf("Invalid serviceNodeInfo: %v", err)
		}
		v, _ := s.cache.LoadOrStore(node.Name, &service{
			nodes: make(map[string]serviceNode),
		})
		sv := v.(*service)
		sv.nodes[node.Host] = node
		return true
	})
}

func (s *ServiceMgr) GetModuleName() string {
	return s.moduleName
}

func (s *ServiceMgr) SetModuleName(name string) {
	// nothing to do
}

func (s *ServiceMgr) Apply(ctx context.Context, opTypes []int32, datas [][]byte, contexts []base.ProposeContext) error {
	span := trace.SpanFromContextSafe(ctx)
	wg := sync.WaitGroup{}
	wg.Add(len(opTypes))
	errs := make([]error, len(opTypes))

	for i, t := range opTypes {
		idx := i
		_, taskCtx := trace.StartSpanFromContextWithTraceID(context.Background(), "", contexts[idx].ReqID)
		switch t {
		case OpRegister:
			s.taskPool.Run(rand.Intn(defaultApplyConcurrency), func() {
				defer wg.Done()
				var arg clustermgr.RegisterArgs
				if err := json.Unmarshal(datas[idx], &arg); err != nil {
					errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
					return
				}
				errs[idx] = s.handleRegister(taskCtx, arg)
			})

		case OpUnregister:
			s.taskPool.Run(rand.Intn(defaultApplyConcurrency), func() {
				defer wg.Done()
				var arg clustermgr.UnregisterArgs
				if err := json.Unmarshal(datas[idx], &arg); err != nil {
					errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
					return
				}
				errs[idx] = s.handleUnregister(taskCtx, arg.Name, arg.Host)
			})

		case OpHeartbeat:
			s.taskPool.Run(rand.Intn(defaultApplyConcurrency), func() {
				defer wg.Done()
				var arg clustermgr.HeartbeatArgs
				if err := json.Unmarshal(datas[idx], &arg); err != nil {
					errs[idx] = errors.Info(err, "json unmarshal failed, data: ", datas[idx]).Detail(err)
					return
				}
				errs[idx] = s.handleHeartbeat(taskCtx, arg.Name, arg.Host)
			})
		}
	}

	wg.Wait()
	failedCount := 0
	for i := range errs {
		if errs[i] != nil {
			failedCount += 1
			span.Error(fmt.Sprintf("operation type: %d, apply failed => ", opTypes[i]), errors.Detail(errs[i]))
		}
	}
	if failedCount > 0 {
		return errors.New(fmt.Sprintf("batch apply failed, failed count: %d", failedCount))
	}

	return nil
}

func (s *ServiceMgr) Flush(ctx context.Context) error {
	span := trace.SpanFromContextSafe(ctx)
	dirty := s.dirty.Load().(*sync.Map)
	s.dirty.Store(&sync.Map{})

	dirty.Range(func(key, val interface{}) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		name := key.(nodeName).name
		host := key.(nodeName).host
		op := val.(dbOpType)
		switch op {
		case DbDelete:
			if err := s.tbl.Delete(name, host); err != nil {
				span.Panicf("delete [moduleName: %s, host: %s] error: %v", name, host, err)
			}
		case DbPut:
			v, hit := s.cache.Load(name)
			if !hit {
				return true
			}
			sv := v.(*service)
			sv.RLock()
			node, hit := sv.nodes[host]
			sv.RUnlock()
			if !hit {
				return true
			}
			data, err := json.Marshal(node)
			if err != nil {
				span.Panicf("marshal serviceNodeInfo error: %v", err)
			}
			if err = s.tbl.Put(name, host, data); err != nil {
				span.Panicf("put [moduleName: %s, host: %s] error: %v", name, host, err)
			}
		}
		return true
	})
	return nil
}

func (s *ServiceMgr) NotifyLeaderChange(ctx context.Context, leader uint64, host string) {
}
