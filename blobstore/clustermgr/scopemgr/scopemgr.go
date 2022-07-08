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
	"errors"
	"sync"

	base_ "github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/kvstore"
	"github.com/cubefs/cubefs/blobstore/common/raftserver"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

var (
	MaxCount = 1000000

	ErrInvalidCount = errors.New("request count is invalid")
)

type ScopeMgrAPI interface {
	Alloc(ctx context.Context, name string, count int) (base, new uint64, err error)
	GetCurrent(name string) uint64
}

type ScopeMgr struct {
	scopeItems map[string]uint64
	raftServer raftserver.RaftServer

	tbl  *normaldb.ScopeTable
	lock sync.RWMutex
}

func NewScopeMgr(db *normaldb.NormalDB) (*ScopeMgr, error) {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewScopeMgr")
	tbl, err := normaldb.OpenScopeTable(db)
	if err != nil {
		return nil, err
	}

	s := &ScopeMgr{tbl: tbl}
	if err = s.LoadData(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ScopeMgr) SetRaftServer(raftServer raftserver.RaftServer) {
	s.raftServer = raftServer
}

func (s *ScopeMgr) Alloc(ctx context.Context, name string, count int) (base, new uint64, err error) {
	if count <= 0 {
		return 0, 0, ErrInvalidCount
	}
	if count > MaxCount {
		count = MaxCount
	}
	span := trace.SpanFromContextSafe(ctx)
	s.lock.Lock()
	s.scopeItems[name] += uint64(count)
	new = s.scopeItems[name]
	s.lock.Unlock()

	data, err := json.Marshal(&allocCtx{Name: name, Current: new})
	if err != nil {
		return
	}

	err = s.raftServer.Propose(ctx, base_.EncodeProposeInfo(s.GetModuleName(), OperTypeAllocScope, data, base_.ProposeContext{ReqID: span.TraceID()}))
	if err != nil {
		return
	}

	base = new - uint64(count) + 1
	return
}

func (s *ScopeMgr) GetCurrent(name string) uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return s.scopeItems[name]
}

func (s *ScopeMgr) applyCommit(ctx context.Context, args *allocCtx) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.scopeItems[args.Name] < args.Current {
		s.scopeItems[args.Name] = args.Current
	}

	current, err := s.tbl.Get(args.Name)
	if err != nil && err != kvstore.ErrNotFound {
		return err
	}
	if current > args.Current {
		return nil
	}

	err = s.tbl.Put(args.Name, args.Current)
	if err != nil {
		return err
	}

	return nil
}
