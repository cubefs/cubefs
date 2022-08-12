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

package common

import (
	"errors"
	"fmt"

	"github.com/hashicorp/consul/api"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
)

func NewConsulClient(addr string) (*api.Client, error) {
	if addr == "" {
		return nil, errors.New("if use consul, consul address need config")
	}
	conf := api.DefaultConfig()
	conf.Address = addr
	return api.NewClient(conf)
}

func GetClustersFromConsul(addr, region string) (clusters map[string][]string) {
	clusters = make(map[string][]string)

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

	for _, pair := range pairs {
		clusterInfo := &cmapi.ClusterInfo{}
		err := Unmarshal(pair.Value, clusterInfo)
		if err != nil {
			fmt.Println("\terror:", err)
			continue
		}
		clusters[clusterInfo.ClusterID.ToString()] = clusterInfo.Nodes
	}
	return
}
