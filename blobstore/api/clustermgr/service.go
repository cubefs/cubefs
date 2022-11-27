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
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	registerUrl    = "/service/register"
	unregisterUrl  = "/service/unregister"
	getserviceUrl  = "/service/get"
	heartbeatUrl   = "/service/heartbeat"
	ListServiceUrl = "/service/list"
)

type ServiceNode struct {
	ClusterID uint64 `json:"cluster_id"`
	Name      string `json:"name"`
	Host      string `json:"host"`
	Idc       string `json:"idc"`
}

type RegisterArgs struct {
	ServiceNode
	Timeout int `json:"timeout"`
}

type GetServiceArgs struct {
	Name string `json:"name"`
}

type UnregisterArgs struct {
	Name string `json:"name"`
	Host string `json:"host"`
}

type HeartbeatArgs UnregisterArgs

type ServiceInfo struct {
	Nodes []ServiceNode `json:"nodes"`
}

// Register service node to cm
// tickInterval: unit of second
// HeartbeatInterval = heartbeatTicks * tickInterval
// expires = expiresTicks * tickInterval
func (c *Client) RegisterService(ctx context.Context, node ServiceNode, tickInterval, heartbeatTicks, expiresTicks uint32) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	params := &RegisterArgs{
		ServiceNode: node,
		Timeout:     int(expiresTicks * tickInterval),
	}
	if err = c.PostWith(ctx, registerUrl, nil, params); err != nil {
		return
	}
	go func() {
		ticker := time.NewTicker(time.Duration(heartbeatTicks*tickInterval) * time.Second)
		var lastErrStatus error
		for range ticker.C {
			err := c.heartbeat(context.Background(), node.Name, node.Host)
			// only when err status change,print log info
			if lastErrStatus != err {
				lastErrStatus = err
				if err != nil {
					span.Errorf("heartbeat error:%v,name:[%s] host:[%s]", err, node.Name, node.Host)
				} else {
					span.Infof("heartbeat recover,name:[%s] host:[%s]", node.Name, node.Host)
				}
			}

		}
	}()
	return
}

func (c *Client) UnregisterService(ctx context.Context, args UnregisterArgs) (err error) {
	err = c.PostWith(ctx, unregisterUrl, nil, args)
	return
}

func (c *Client) GetService(ctx context.Context, args GetServiceArgs) (info ServiceInfo, err error) {
	err = c.GetWith(ctx, getserviceUrl+"?name="+args.Name, &info)
	return
}

func (c *Client) heartbeat(ctx context.Context, name, host string) (err error) {
	args := HeartbeatArgs{name, host}
	err = c.PostWith(ctx, heartbeatUrl, nil, args)
	return
}

func (c *Client) ListService(ctx context.Context) (info ServiceInfo, err error) {
	err = c.GetWith(ctx, ListServiceUrl, &info)
	return
}
