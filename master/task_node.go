// Copyright 2018 The Chubao Authors.
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

package master

import (
	"sync"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	BatchGoroutineSize = 1024 //batch sync send goroutine
)

// TaskNode
type TaskNode interface {
	Address() string
	AddAsyncAdminTask(task *proto.AdminTask)
	RunSyncAdminTask(task *proto.AdminTask) (packet *proto.Packet, err error)
}

type TaskResponse interface {
	String() string
}

type TaskNodeResponse struct {
	Addr     string
	Response TaskResponse
}

func getTaskNodesByHosts(nodes *sync.Map, hosts []string) []TaskNode {
	ns := make([]TaskNode, 0)
	if hosts != nil && len(hosts) > 0 {
		for _, h := range hosts {
			if m, ok := nodes.Load(h); ok {
				ns = append(ns, m.(TaskNode))
			}
		}
	} else {
		nodes.Range(func(key interface{}, v interface{}) bool {
			ns = append(ns, v.(TaskNode))
			return true
		})
	}
	return ns
}

func extractTaskPacketData(p *proto.Packet) TaskResponse {
	switch p.Opcode {
	case proto.OpGetMetaNodeParams:
		data := &proto.GetMetaNodeParamsResponse{}
		p.UnmarshalData(data)
		return data
	case proto.OpGetDataNodeParams:
		data := &proto.GetDataNodeParamsResponse{}
		p.UnmarshalData(data)
		return data
	default:
	}
	return nil
}

func buildAdminReqTasks(nodes []TaskNode, opCode uint8, req interface{}) []*proto.AdminTask {
	tasks := make([]*proto.AdminTask, 0)
	for _, n := range nodes {
		addr := n.Address()
		task := proto.NewAdminTask(proto.OpSetMetaNodeParams, addr, req)
		tasks = append(tasks, task)
	}

	return tasks
}

func syncTaskToNodes(nodes []TaskNode, opCode uint8, req interface{}, resp map[string]TaskNodeResponse) {
	var (
		wg sync.WaitGroup
		mu sync.Mutex
	)
	for _, node := range nodes {
		addr := node.Address()
		task := proto.NewAdminTask(opCode, addr, req)
		wg.Add(1)
		go func(n TaskNode, t *proto.AdminTask) {
			defer func() {
				wg.Done()
			}()
			if rep, _ := doTaskOnNode(n, t); rep != nil {
				adr := n.Address()
				mu.Lock()
				resp[adr] = TaskNodeResponse{
					Addr:     adr,
					Response: rep,
				}
				mu.Unlock()
			}

		}(node, task)
	}
	wg.Wait()
}

func doTaskOnNode(node TaskNode, task *proto.AdminTask) (resp TaskResponse, err error) {
	var p *proto.Packet
	if p, err = node.RunSyncAdminTask(task); err != nil {
		log.LogErrorf("task_node: RunSyncAdminTask error: %v", err)
		return
	}
	resp = extractTaskPacketData(p)
	return
}

func (m *Server) RunSyncAdminTasks(nodes []TaskNode, opCode uint8, req interface{}) (resp []TaskNodeResponse, err error) {
	totalSize := len(nodes)
	nodesResp := make(map[string]TaskNodeResponse)

	if totalSize > BatchGoroutineSize {
		for i := 0; i < totalSize; {
			leftSize := totalSize - i
			size := BatchGoroutineSize
			if leftSize < BatchGoroutineSize {
				size = leftSize
			}
			syncTaskToNodes(nodes[i:i+size], opCode, req, nodesResp)
			i += size
		}
	} else {
		syncTaskToNodes(nodes, opCode, req, nodesResp)
	}

	for _, v := range nodesResp {
		resp = append(resp, v)
	}

	//log.LogInfof("task_node: resp: %v", resp)

	return
}

func (m *Server) RunAsyncAdminTasks(nodes []TaskNode, opCode uint8, req interface{}) {
	for _, n := range nodes {
		addr := n.Address()
		task := proto.NewAdminTask(opCode, addr, req)
		n.AddAsyncAdminTask(task)
	}
}
