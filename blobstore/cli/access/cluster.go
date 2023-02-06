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

package access

import (
	"context"
	"strconv"
	"strings"

	"github.com/desertbit/grumble"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
	"github.com/cubefs/cubefs/blobstore/cli/common/fmt"
	"github.com/cubefs/cubefs/blobstore/cli/config"
	"github.com/cubefs/cubefs/blobstore/common/proto"
)

func showClusters(c *grumble.Context) error {
	cli, err := config.NewConsulClient(config.AccessConsulAddr())
	if err != nil {
		return showClusterWithConfig()
	}

	var regions []string
	region := c.Args.String("region")
	if region == "" {
		region = config.Region()
	}
	if region != "" {
		regions = []string{region}
	} else {
		pairs, _, err := cli.KV().List("/ebs/", nil)
		if err != nil {
			return err
		}
		for _, pair := range pairs {
			paths := strings.Split(strings.Trim(pair.Key, "/"), "/")
			region := paths[1]
			has := false
			for _, r := range regions {
				if r == region {
					has = true
					break
				}
			}
			if !has {
				regions = append(regions, region)
			}
		}
		fmt.Printf("found %s regions\n\n", color.GreenString("%d", len(regions)))
	}

	fmt.Println("\tClusters Info From Consul\t")
	for _, region := range regions {
		fmt.Println("to list region:", color.RedString("%s", region))
		path := cmapi.GetConsulClusterPath(region)
		pairs, _, err := cli.KV().List(path, nil)
		if err != nil {
			fmt.Println("\terror:", err)
			continue
		}

		capacity := int64(1)
		available := int64(1)
		for _, pair := range pairs {
			clusterInfo := &cmapi.ClusterInfo{}
			err := common.Unmarshal(pair.Value, clusterInfo)
			if err != nil {
				fmt.Println("\terror:", err)
				continue
			}

			capacity += clusterInfo.Capacity
			available += clusterInfo.Available

			clusterInfo.Capacity++
			clusterInfo.Available++
			fmt.Println(cfmt.ClusterInfoJoin(clusterInfo, "\t"))
			fmt.Println()
		}
		fmt.Printf("\tspace in region: %s (%s / %s)\n", region,
			common.ColorizeInt64(-available, capacity).Sprint(humanize.IBytes(uint64(available))),
			humanize.IBytes(uint64(capacity)))
	}

	return nil
}

func showClusterWithConfig() error {
	fmt.Println("\tClusters Info From Config\t")

	cs := config.Clusters()
	for clusterID, hosts := range cs {
		fmt.Println("====================================")
		client := config.NewCluster(clusterID, nil, "")
		stat, err := client.Stat(context.Background())
		if err != nil {
			fmt.Println("\terror:", err)
			continue
		}

		clusterInfo := &cmapi.ClusterInfo{}
		ClusterID, _ := strconv.Atoi(clusterID)
		clusterInfo.ClusterID = proto.ClusterID(ClusterID)
		clusterInfo.Capacity = stat.SpaceStat.TotalSpace
		clusterInfo.Available = stat.SpaceStat.WritableSpace
		clusterInfo.Nodes = hosts
		clusterInfo.Readonly = stat.ReadOnly

		fmt.Println(cfmt.ClusterInfoJoin(clusterInfo, "\t"))
		fmt.Println()

		fmt.Printf("\tspace in cluster: %s (%s / %s)\n", clusterID,
			common.ColorizeInt64(-stat.SpaceStat.WritableSpace, stat.SpaceStat.TotalSpace).Sprint(humanize.IBytes(uint64(stat.SpaceStat.WritableSpace))),
			humanize.IBytes(uint64(stat.SpaceStat.TotalSpace)))
	}
	return nil
}
