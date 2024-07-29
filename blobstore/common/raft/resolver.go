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

package raft

import (
	"context"
	"sync"
)

// todo: as we store raft group members into storage,
// shall we implements AddressResolver by raft group?
type cacheAddressResolver struct {
	m        sync.Map
	resolver AddressResolver
}

func (r *cacheAddressResolver) Resolve(ctx context.Context, nodeId uint64) (Addr, error) {
	if v, ok := r.m.Load(nodeId); ok {
		return v.(Addr), nil
	}

	addr, err := r.resolver.Resolve(ctx, nodeId)
	if err != nil {
		return nil, err
	}
	r.m.Store(nodeId, addr)
	return addr, nil
}
