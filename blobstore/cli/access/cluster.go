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
	"fmt"
	"strings"

	"github.com/desertbit/grumble"
	"github.com/dustin/go-humanize"
	"github.com/fatih/color"

	cmapi "github.com/cubefs/cubefs/blobstore/api/clustermgr"
	"github.com/cubefs/cubefs/blobstore/cli/common"
	"github.com/cubefs/cubefs/blobstore/cli/common/cfmt"
)

func showClusters(c *grumble.Context) error {
	cli, err := newConsulClient()
	if err != nil {
		return err
	}

	var regions []string
	if region := c.Args.String("region"); region != "" {
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
		fmt.Printf("    space in region: %s (%s / %s)\n", region,
			common.ColorizeInt64(-available, capacity).Sprint(humanize.IBytes(uint64(available))),
			humanize.IBytes(uint64(capacity)))
	}

	return nil
}
