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

package config

import (
	"errors"

	"github.com/hashicorp/consul/api"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/rpc/auth"
)

// NewConsulClient returns client of consul with address.
func NewConsulClient(addr string) (*api.Client, error) {
	if addr == "" {
		return nil, errors.New("if use consul, consul address need config")
	}
	conf := api.DefaultConfig()
	conf.Address = addr
	return api.NewClient(conf)
}

func getClustersFromConsul(addr, region string) (clusters map[string][]string) {
	cli, err := NewConsulClient(addr)
	if err != nil {
		return
	}
	if region == "" {
		region = "test-region"
	}

	path := cmapi.GetConsulClusterPath(region)
	pairs, _, err := cli.KV().List(path, nil)
	if err != nil {
		return
	}

	clusters = make(map[string][]string)
	for _, pair := range pairs {
		clusterInfo := &cmapi.ClusterInfo{}
		err := common.Unmarshal(pair.Value, clusterInfo)
		if err != nil {
			fmt.Println("\terror:", err)
			continue
		}
		clusters[clusterInfo.ClusterID.ToString()] = clusterInfo.Nodes
	}
	return
}

// Cluster returns config default cluster.
func Cluster() *cmapi.Client {
	clusterID := proto.ClusterID(DefaultClusterID()).ToString()
	return NewCluster(clusterID, nil, "")
}

// NewCluster returns cluster client.
// TODO: returns cached cluster with the same params.
func NewCluster(clusterID string, hosts []string, secret string) *cmapi.Client {
	if len(hosts) == 0 {
		hosts = Clusters()[clusterID]
	}
	if secret == "" {
		secret = ClusterMgrSecret()
	}
	return cmapi.New(&cmapi.Config{
		LbConfig: rpc.LbConfig{
			Hosts: hosts,
			Config: rpc.Config{
				Tc: rpc.TransportConfig{
					Auth: auth.Config{
						EnableAuth: secret != "",
						Secret:     secret,
					},
				},
			},
		},
	})
}
