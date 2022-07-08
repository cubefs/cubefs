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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/clustermgr/base"
	"github.com/cubefs/cubefs/blobstore/clustermgr/persistence/normaldb"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
)

const (
	defaultApplyConcurrency = 20
	moduleName              = "service manager"
)

const (
	OpRegister int32 = iota + 1
	OpUnregister
	OpHeartbeat
)

type dbOpType uint8

const (
	DbPut dbOpType = iota
	DbDelete
)

type serviceNode struct {
	ClusterID uint64    `json:"cluster_id"`
	Name      string    `json:"moduleName"`
	Host      string    `json:"host"`
	Idc       string    `json:"idc"`
	Timeout   int       `json:"timeout"`
	Expires   time.Time `json:"expires"`
}

type service struct {
	sync.RWMutex
	// key: host
	nodes map[string]serviceNode
}

type nodeName struct {
	name string
	host string
}

type ServiceMgr struct {
	moduleName string
	tbl        *normaldb.ServiceTable
	cache      sync.Map
	dirty      atomic.Value
	taskPool   *base.TaskDistribution
}

func NewServiceMgr(t *normaldb.ServiceTable) *ServiceMgr {
	_, ctx := trace.StartSpanFromContext(context.Background(), "NewServiceMgr")
	mgr := &ServiceMgr{
		moduleName: moduleName,
		tbl:        t,
		taskPool:   base.NewTaskDistribution(defaultApplyConcurrency, 1),
	}
	mgr.dirty.Store(&sync.Map{})
	if err := mgr.LoadData(ctx); err != nil {
		log.Panic("reload data error", err)
	}
	return mgr
}

func (s *ServiceMgr) GetServiceInfo(sname string) (info clustermgr.ServiceInfo) {
	val, hit := s.cache.Load(sname)
	if !hit {
		return
	}
	sv := val.(*service)
	sv.RLock()
	defer sv.RUnlock()
	for _, val := range sv.nodes {
		if time.Until(val.Expires) <= 0 {
			continue
		}
		node := clustermgr.ServiceNode{
			ClusterID: val.ClusterID,
			Name:      val.Name,
			Host:      val.Host,
			Idc:       val.Idc,
		}
		info.Nodes = append(info.Nodes, node)
	}
	return
}

func (s *ServiceMgr) ListServiceInfo() (info clustermgr.ServiceInfo, err error) {
	s.cache.Range(func(key, value interface{}) bool {
		sv := value.(*service)
		sv.RLock()
		for _, val := range sv.nodes {
			node := clustermgr.ServiceNode{
				ClusterID: val.ClusterID,
				Name:      val.Name,
				Host:      val.Host,
				Idc:       val.Idc,
			}
			info.Nodes = append(info.Nodes, node)
		}
		sv.RUnlock()

		return true
	})

	return
}

func (s *ServiceMgr) handleRegister(ctx context.Context, arg clustermgr.RegisterArgs) (err error) {
	info := serviceNode{
		ClusterID: arg.ClusterID,
		Name:      arg.Name,
		Host:      arg.Host,
		Idc:       arg.Idc,
		Timeout:   arg.Timeout,
		Expires:   time.Now().Add(time.Duration(arg.Timeout) * time.Second),
	}
	key := nodeName{arg.Name, arg.Host}
	// lookup service in cache
	v, _ := s.cache.LoadOrStore(info.Name, &service{
		nodes: make(map[string]serviceNode),
	})
	sv := v.(*service)
	sv.Lock()
	sv.nodes[info.Host] = info
	s.dirty.Load().(*sync.Map).Store(key, DbPut)
	sv.Unlock()
	return nil
}

func (s *ServiceMgr) handleUnregister(ctx context.Context, sname, host string) error {
	key := nodeName{sname, host}
	val, hit := s.cache.Load(sname)
	if !hit {
		return nil
	}
	sv := val.(*service)
	sv.Lock()
	defer sv.Unlock()
	delete(sv.nodes, host)
	s.dirty.Load().(*sync.Map).Store(key, DbDelete)
	return nil
}

func (s *ServiceMgr) handleHeartbeat(ctx context.Context, sname, host string) (err error) {
	val, hit := s.cache.Load(sname)
	if !hit {
		return
	}
	sv := val.(*service)
	key := nodeName{sname, host}

	sv.Lock()
	defer sv.Unlock()
	node, hit := sv.nodes[host]
	if !hit {
		return
	}
	node.Expires = time.Now().Add(time.Duration(node.Timeout) * time.Second)
	sv.nodes[host] = node
	s.dirty.Load().(*sync.Map).Store(key, DbPut)
	return nil
}

func (s *ServiceMgr) IsRegistered(name string, host string) bool {
	val, hit := s.cache.Load(name)
	if !hit {
		return false
	}
	sv := val.(*service)
	sv.RLock()
	defer sv.RUnlock()
	for _, val := range sv.nodes {
		if val.Host == host {
			return true
		}
	}
	return false
}
